package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 后台轮询分发器：领取到期事件并调用 {@link EventHandler}，驱动全部状态流转。
 *
 * <p>失败分类判定依据：</p>
 * <ul>
 *   <li>{@link NonRetryableEventException} -&gt; DEAD_LETTER/NON_RETRYABLE（不重试）；</li>
 *   <li>{@link EventValidationException} -&gt; DEAD_LETTER/VALIDATION_FAILED（不重试）；</li>
 *   <li>没有注册处理器 -&gt; DEAD_LETTER/NON_RETRYABLE；</li>
 *   <li>其它任意异常 -&gt; 瞬时故障，按指数退避 scheduleRetry，次数耗尽转死信 RETRIES_EXHAUSTED。</li>
 * </ul>
 *
 * <p>并发安全：领取与每一次状态推进都在存储层以“租约令牌 + 条件更新 + 单批落盘”完成；
 * 若提交租约不匹配（说明事件已被别的消费者/租约回收重新领取），本次结果直接丢弃。</p>
 */
public class EventDispatcher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventStore store;
    private final HandlerRegistry registry;
    private final RetryPolicy retryPolicy;
    private final long pollIntervalMillis;
    private final int workerCount;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> pollers = new CopyOnWriteArrayList<>();

    public EventDispatcher(EventStore store, HandlerRegistry registry, RetryPolicy retryPolicy,
                           long pollIntervalMillis, int workerCount) {
        this.store = store;
        this.registry = registry;
        this.retryPolicy = retryPolicy;
        this.pollIntervalMillis = pollIntervalMillis;
        this.workerCount = Math.max(1, workerCount);
        this.workers = Executors.newFixedThreadPool(this.workerCount, r -> {
            Thread t = new Thread(r, "reliable-event-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < workerCount; i++) {
            Thread poller = new Thread(this::pollLoop, "reliable-event-poller-" + i);
            poller.setDaemon(true);
            poller.start();
            pollers.add(poller);
        }
        log.info("事件分发器已启动: poller={} worker={} pollInterval={}ms", workerCount, workerCount, pollIntervalMillis);
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                store.claimNext(System.currentTimeMillis())
                        .ifPresent(claimed -> workers.submit(() -> process(claimed)));
                sleep(pollIntervalMillis);
            } catch (Throwable t) {
                log.error("分发循环异常（将继续轮询）: {}", t.toString(), t);
                sleep(pollIntervalMillis);
            }
        }
    }

    /** 处理一条已领取事件。包级可见以便测试直接驱动。 */
    public void process(EventStore.ClaimedEvent claimed) {
        EventRecord ev = claimed.event();
        long lease = claimed.leaseToken();
        long now = System.currentTimeMillis();
        EventHandler handler = registry.find(ev.type()).orElse(null);
        if (handler == null) {
            safeDeadLetter(ev, lease, DeadLetterReason.NON_RETRYABLE,
                    "未注册的事件类型: " + ev.type() + "（判定依据：永久性配置缺失，重试无意义）", now);
            return;
        }
        Map<String, String> sideEffects;
        try {
            log.info("开始消费: eventId={} type={} attempt={}/{} payload={}",
                    ev.eventId(), ev.type(), ev.attemptCount(), ev.maxAttempts(), ev.payload());
            sideEffects = handler.handle(ev);
        } catch (EventValidationException e) {
            log.warn("消费期输入校验失败: eventId={} 判定依据={}", ev.eventId(), e.getMessage());
            safeDeadLetter(ev, lease, DeadLetterReason.VALIDATION_FAILED,
                    "消费期输入校验失败: " + e.getMessage(), System.currentTimeMillis());
            return;
        } catch (NonRetryableEventException e) {
            log.warn("处理器声明永久故障: eventId={} 判定依据={}", ev.eventId(), e.getMessage());
            safeDeadLetter(ev, lease, DeadLetterReason.NON_RETRYABLE,
                    "不可重试故障: " + e.getMessage(), System.currentTimeMillis());
            return;
        } catch (Throwable t) {
            long failAt = System.currentTimeMillis();
            int completed = ev.attemptCount();
            long nextAt = failAt + retryPolicy.backoffMillis(completed, failAt);
            log.warn("瞬时故障: eventId={} attempt={}/{} nextAttemptAt=+{}ms 判定依据={}",
                    ev.eventId(), completed, retryPolicy.maxAttempts(),
                    nextAt - failAt, t.toString());
            safeRetry(ev, lease, t.toString(), nextAt, failAt);
            return;
        }
        try {
            store.markSucceeded(ev.eventId(), lease, sideEffects, System.currentTimeMillis());
        } catch (OptimisticLockingFailureException e) {
            log.warn("成功提交因租约不匹配被丢弃（事件已被其它领取者推进，不产生重复副作用）: {}", e.getMessage());
        }
    }

    private void safeRetry(EventRecord ev, long lease, String error, long nextAt, long now) {
        try {
            store.scheduleRetry(ev.eventId(), lease, error, nextAt, now);
        } catch (OptimisticLockingFailureException e) {
            log.warn("重试排定因租约不匹配被丢弃: {}", e.getMessage());
        }
    }

    private void safeDeadLetter(EventRecord ev, long lease, DeadLetterReason reason, String msg, long now) {
        try {
            store.markDeadLetter(ev.eventId(), lease, reason, msg, now);
        } catch (OptimisticLockingFailureException e) {
            log.warn("死信标记因租约不匹配被丢弃: {}", e.getMessage());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("事件分发器已停止");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}

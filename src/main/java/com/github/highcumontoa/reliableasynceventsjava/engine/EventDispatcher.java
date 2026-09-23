package com.github.highcumontoa.reliableasynceventsjava.engine;

import com.github.highcumontoa.reliableasynceventsjava.config.EventProperties;
import com.github.highcumontoa.reliableasynceventsjava.core.*;
import com.github.highcumontoa.reliableasynceventsjava.storage.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 进程内可靠事件调度器：轮询 outbox、领取、执行处理器、按失败分类重试/死信。
 *
 * <p>并发与重启安全：</p>
 * <ul>
 *   <li>乐观领取（status=PENDING + 到期 条件更新）：并发消费者只有一个能领取成功；</li>
 *   <li>每次领取生成唯一 claimToken，后续推进均校验 token，崩溃的消费者无法误改新领取；</li>
 *   <li>崩溃遗留的 PROCESSING 租约到期后自动回收为 PENDING；</li>
 *   <li>消费端 processed_event 去重 + 处理器在同一事务内执行，副作用最多生效一次。</li>
 * </ul>
 */
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventRepository repository;
    private final EventHandlerRegistry registry;
    private final RetryPolicy retryPolicy;
    private final TransactionTemplate tx;
    private final EventProperties properties;

    private ScheduledExecutorService poller;
    private ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean pollInProgress = new AtomicBoolean(false);

    public EventDispatcher(EventRepository repository,
                           EventHandlerRegistry registry,
                           RetryPolicy retryPolicy,
                           PlatformTransactionManager txManager,
                           EventProperties properties) {
        this.repository = repository;
        this.registry = registry;
        this.retryPolicy = retryPolicy;
        this.properties = properties;
        this.tx = new TransactionTemplate(txManager);
        this.tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /** 启动轮询线程与工作线程池。 */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        int workerCount = Math.max(1, properties.getWorkerCount());
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-poller");
            t.setDaemon(true);
            return t;
        });
        workers = new ThreadPoolExecutor(workerCount, workerCount,
                30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Math.max(16, properties.getBatchSize() * 2)),
                r -> {
                    Thread t = new Thread(r, "event-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        long intervalMs = properties.getPollInterval().toMillis();
        poller.scheduleAtFixedRate(this::pollSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("[dispatcher] 启动 workers={} pollInterval={}ms maxAttempts={} leaseTtl={}ms",
                workerCount, intervalMs, retryPolicy.getMaxAttempts(), properties.getLeaseTtl().toMillis());
    }

    /** 优雅停止：先停轮询，再等待在途事件处理结束。 */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("[dispatcher] 停止中…");
        if (poller != null) {
            poller.shutdownNow();
        }
        if (workers != null) {
            workers.shutdown();
            try {
                if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                    workers.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workers.shutdownNow();
            }
        }
        log.info("[dispatcher] 已停止");
    }

    public boolean isRunning() {
        return running.get();
    }

    private void pollSafely() {
        if (!pollInProgress.compareAndSet(false, true)) {
            return; // 上一轮还没跑完，跳过，避免重叠
        }
        try {
            reclaimStaleLeases();
            Instant now = Instant.now();
            List<EventRecord> due = repository.findDuePending(now, properties.getBatchSize());
            if (due.isEmpty()) {
                return;
            }
            log.info("[dispatcher] 本轮发现 {} 条到期事件", due.size());
            for (EventRecord e : due) {
                if (!running.get()) {
                    return;
                }
                workers.execute(() -> claimAndProcess(e.id()));
            }
        } catch (Throwable t) {
            log.error("[dispatcher] 轮询异常（下一轮继续）", t);
        } finally {
            pollInProgress.set(false);
        }
    }

    private void reclaimStaleLeases() {
        Instant now = Instant.now();
        Instant deadline = now.minusMillis(properties.getLeaseTtl().toMillis());
        Integer n = tx.execute(s -> repository.reclaimStaleLeases(now, deadline));
        if (n != null && n > 0) {
            log.warn("[dispatcher] 回收 {} 条崩溃遗留的过期租约", n);
        }
    }

    /** 同步执行一轮扫描（测试用）：在当前线程串行处理一批到期事件，返回处理条数。 */
    public int runOnce() {
        reclaimStaleLeases();
        List<EventRecord> due = repository.findDuePending(Instant.now(), properties.getBatchSize());
        int processed = 0;
        for (EventRecord e : due) {
            String result = claimAndProcess(e.id());
            if (!"SKIPPED".equals(result)) {
                processed++;
            }
        }
        return processed;
    }

    /** 领取并处理一条事件；返回最终状态（COMPLETED/PENDING/DEAD_LETTER/SKIPPED）。 */
    public String claimAndProcess(long eventId) {
        String token = "claim-" + eventId + "-" + UUID.randomUUID();
        Boolean claimed = tx.execute(s -> {
            if (repository.claim(eventId, token, Instant.now())) {
                EventRecord rec = repository.findById(eventId).orElseThrow();
                repository.recordTransition(eventId, EventStatus.PENDING, EventStatus.PROCESSING,
                        TransitionReason.CLAIMED, "token=" + token + " attempts=" + rec.attempts(),
                        rec.attempts());
                return true;
            }
            return false;
        });
        if (!Boolean.TRUE.equals(claimed)) {
            log.info("[dispatcher] event={} 已被其它消费者领取，跳过", eventId);
            return "SKIPPED";
        }
        return processClaimed(eventId, token);
    }

    /** 处理一条已领取的事件。返回最终状态名，便于测试断言与日志解释。 */
    public String processClaimed(long eventId, String token) {
        EventRecord rec = repository.findById(eventId).orElse(null);
        if (rec == null || rec.status() != EventStatus.PROCESSING
                || !token.equals(rec.claimToken())) {
            log.warn("[dispatcher] event={} 非本领取者所有，放弃处理", eventId);
            return "SKIPPED";
        }

        EventHandler handler = registry.lookup(rec.type()).orElse(null);
        if (handler == null) {
            String detail = "未找到事件类型 " + rec.type() + " 的处理器，判定为永久故障";
            tx.execute(s -> {
                repository.markDead(eventId, EventStatus.PROCESSING, DeadReason.NON_RETRYABLE,
                        detail, rec.attempts(), token);
                repository.recordTransition(eventId, EventStatus.PROCESSING, EventStatus.DEAD_LETTER,
                        TransitionReason.NON_RETRYABLE, detail, rec.attempts());
                return null;
            });
            log.warn("[dispatch-result] event={} -> DEAD_LETTER reason=NON_RETRYABLE detail={}", eventId, detail);
            return "DEAD_LETTER";
        }

        Throwable failure;
        Boolean firstTime;
        try {
            firstTime = tx.execute(s -> {
                boolean first = repository.markProcessingIfFirst(eventId);
                log.info("[dispatch] event={} type={} attempts(失败计数)={} 首次处理={} payload={}",
                        eventId, rec.type(), rec.attempts(), first, rec.payload());
                if (first) {
                    try {
                        handler.handle(rec.payload());
                    } catch (RuntimeException re) {
                        throw re;
                    } catch (Exception ex) {
                        // 受检异常保守视为瞬时故障
                        throw new RetryableEventException(
                                "处理器抛出受检异常: " + ex.getMessage(), ex);
                    }
                } else {
                    log.info("[dispatch] event={} 命中消费端去重，跳过副作用直接完成", eventId);
                }
                boolean done = repository.markCompleted(eventId, token);
                repository.recordTransition(eventId, EventStatus.PROCESSING, EventStatus.COMPLETED,
                        TransitionReason.COMPLETED,
                        first ? "处理器执行成功" : "去重命中，副作用此前已生效", rec.attempts());
                if (!done) {
                    throw new EventException("event=" + eventId + " 完成推进失败（claim 已失效）");
                }
                return first;
            });
            log.info("[dispatch-result] event={} -> COMPLETED 首次处理={}", eventId, firstTime);
            return "COMPLETED";
        } catch (Exception e) {
            // 业务事务已整体回滚：处理器副作用与去重标记都不会留下
            failure = unwrap(e);
            log.warn("[dispatch] event={} 处理失败，准备分类: {}: {}",
                    eventId, failure.getClass().getSimpleName(), failure.getMessage());
        }

        return classifyFailure(rec, token, failure);
    }

    private String classifyFailure(EventRecord rec, String token, Throwable failure) {
        int newAttempts = rec.attempts() + 1;

        if (failure instanceof InvalidEventException) {
            return dead(rec, token, DeadReason.VALIDATION_FAILED, TransitionReason.HANDLER_VALIDATION,
                    newAttempts, "处理器判定输入非法: " + failure.getMessage());
        }
        if (failure instanceof NonRetryableEventException) {
            return dead(rec, token, DeadReason.NON_RETRYABLE, TransitionReason.NON_RETRYABLE,
                    newAttempts, "永久故障: " + failure.getMessage());
        }

        // 其余一律按瞬时故障处理（保守重试，绝不静默丢弃）
        if (newAttempts >= rec.maxAttempts()) {
            return dead(rec, token, DeadReason.RETRY_EXHAUSTED, TransitionReason.RETRY_EXHAUSTED,
                    newAttempts, "瞬时故障重试已达上限 maxAttempts=" + rec.maxAttempts()
                            + "，末次异常: " + failure.getClass().getSimpleName()
                            + ": " + failure.getMessage());
        }
        Instant nextAt = retryPolicy.nextAttemptAfter(newAttempts, Instant.now());
        String detail = "瞬时故障(" + failure.getClass().getSimpleName() + "): " + failure.getMessage()
                + "；退避至 " + nextAt + " 重试(" + newAttempts + "/" + rec.maxAttempts() + ")";
        tx.execute(s -> {
            repository.markPendingForRetry(rec.id(), newAttempts, nextAt, token,
                    failure.getClass().getSimpleName() + ": " + failure.getMessage());
            repository.recordTransition(rec.id(), EventStatus.PROCESSING, EventStatus.PENDING,
                    TransitionReason.RETRY_BACKOFF, detail, newAttempts);
            return null;
        });
        log.info("[dispatch-result] event={} -> PENDING reason=RETRY_BACKOFF {}", rec.id(), detail);
        return "PENDING";
    }

    private String dead(EventRecord rec, String token, DeadReason reason, TransitionReason transition,
                        int attempts, String detail) {
        tx.execute(s -> {
            repository.markDead(rec.id(), EventStatus.PROCESSING, reason, detail, attempts, token);
            repository.recordTransition(rec.id(), EventStatus.PROCESSING, EventStatus.DEAD_LETTER,
                    transition, detail, attempts);
            return null;
        });
        log.warn("[dispatch-result] event={} -> DEAD_LETTER reason={} detail={}", rec.id(), reason, detail);
        return "DEAD_LETTER";
    }

    private static Throwable unwrap(Throwable t) {
        Throwable cur = t;
        while ((cur instanceof RuntimeException) && cur.getCause() != null
                && (cur.getClass().getName().contains("Transaction")
                || cur.getClass().getName().contains("Completion"))) {
            cur = cur.getCause();
        }
        return cur;
    }
}

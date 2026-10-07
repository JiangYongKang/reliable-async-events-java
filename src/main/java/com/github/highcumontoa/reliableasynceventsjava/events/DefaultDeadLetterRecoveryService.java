package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批量死信恢复的默认实现。
 *
 * <p>目标集合：任务启动时快照。显式 {@code eventIds} 与条件筛选
 * （{@code eventType}/{@code reason}/{@code deadLetteredBeforeMillis}）取并集去重；
 * <b>既不给事件列表也不给任何筛选条件时，语义为“恢复当前全部死信”</b>。
 * 快照后新进入的死信不属于本任务。</p>
 *
 * <p>流量控制（两个参数各管一件事）：</p>
 * <ul>
 *   <li>{@code ratePerSecond}：全局限速，约束“把死信重放回 PENDING”的注入速率；</li>
 *   <li>{@code concurrency}：在途上限，约束“已重放、但还没在处理链路上跑到终态
 *       （SUCCEEDED / 再次 DEAD_LETTER）”的恢复事件数量。每条事件重放后占用一个在途名额，
 *       直到它到达终态才释放；名额占满时主循环暂停注入新的重放，从而真正压住
 *       恢复流量对处理链路与下游的冲击。正常新事件的领取不受影响，不会被饿死。</li>
 * </ul>
 *
 * <p>单条重放仍是原子的 WAL 批次（REPLAY + HISTORY 同批 fsync），进程在任意时刻被强杀，
 * 已重放的事件保持 PENDING、未重放的保持死信，重启后再次发起任务即可续跑，无半截状态。</p>
 *
 * <p>幂等：目标事件若已不在死信态（被其它路径重放或重复提交同一批）则计为 skipped，
 * 不重复写历史、不产生重复副作用；同一事件在同一任务内只处理一次（启动时快照去重）。</p>
 *
 * <p>零目标：快照为空（没给条件且当前无死信，或筛选一条都没命中）时任务立即进入
 * {@link RecoveryProgress.Status#NO_TARGETS} 终态，total/replayed 均为 0，
 * 与“有目标且全部恢复成功”的 COMPLETED 明确区分，避免误判为恢复已生效。</p>
 */
public class DefaultDeadLetterRecoveryService implements DeadLetterRecoveryService, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DefaultDeadLetterRecoveryService.class);

    /** 在途事件终态轮询间隔（毫秒）。 */
    private static final long SETTLE_POLL_MILLIS = 20;

    private final EventStore store;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public DefaultDeadLetterRecoveryService(EventStore store) {
        this.store = store;
    }

    @Override
    public String startRecovery(RecoveryRequest request) {
        // 启动时快照目标集合：显式列表与条件筛选取并集去重，任务期间不受新死信影响
        boolean hasIds = request.eventIds() != null && !request.eventIds().isEmpty();
        boolean hasFilter = request.eventType() != null || request.reason() != null
                || request.deadLetteredBeforeMillis() != null;
        LinkedHashSet<String> targets = new LinkedHashSet<>();
        if (hasIds) {
            targets.addAll(request.eventIds());
        }
        if (hasFilter || !hasIds) {
            // 给了筛选条件 -> 按条件筛选；什么条件都没给 -> 恢复当前全部死信
            for (EventRecord ev : store.findDeadLetters()) {
                if (hasFilter && !matches(request, ev)) {
                    continue;
                }
                targets.add(ev.eventId());
            }
        }
        String jobId = UUID.randomUUID().toString();
        Job job = new Job(jobId, new ArrayList<>(targets), request);
        jobs.put(jobId, job);
        Thread t = new Thread(job::run, "dead-letter-recovery-" + jobId.substring(0, 8));
        t.setDaemon(true);
        t.start();
        log.info("批量恢复任务已启动: jobId={} 目标={} 速率={}/s 并发(在途上限)={} 选目标方式={} 筛选(type={}, reason={}, before={}) 显式ID={}",
                jobId, targets.size(), request.ratePerSecond(), request.concurrency(),
                !hasIds && !hasFilter ? "无条件-恢复全部死信" : (hasIds && hasFilter ? "显式ID+条件筛选" : hasIds ? "显式ID" : "条件筛选"),
                request.eventType(), request.reason(), request.deadLetteredBeforeMillis(),
                request.eventIds() == null ? 0 : request.eventIds().size());
        return jobId;
    }

    private boolean matches(RecoveryRequest request, EventRecord ev) {
        if (request.eventType() != null && !request.eventType().equals(ev.type())) {
            return false;
        }
        if (request.reason() != null && request.reason() != ev.deadLetterReason()) {
            return false;
        }
        return request.deadLetteredBeforeMillis() == null
                || ev.deadLetterAtMillis() <= request.deadLetteredBeforeMillis();
    }

    @Override
    public Optional<RecoveryProgress> progress(String jobId) {
        Job job = jobs.get(jobId);
        return job == null ? Optional.empty() : Optional.of(job.snapshot());
    }

    @Override
    public Optional<RecoveryProgress> stop(String jobId) {
        Job job = jobs.get(jobId);
        if (job == null) {
            return Optional.empty();
        }
        job.requestStop();
        log.info("批量恢复任务收到停止请求: jobId={} 已处理={}/{} 在途={}",
                jobId, job.processed(), job.total, job.inFlight.get());
        return Optional.of(job.snapshot());
    }

    /** 单个恢复任务的内部状态。 */
    private final class Job {
        private final String jobId;
        private final List<String> targets;
        private final RecoveryRequest request;
        private final int total;
        private final AtomicInteger replayed = new AtomicInteger();
        private final AtomicInteger skipped = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        /** 已重放、尚未在处理链路上到达终态的事件数（受 concurrency 上限约束）。 */
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicBoolean stopRequested = new AtomicBoolean(false);
        private final long startedAt = System.currentTimeMillis();
        private volatile RecoveryProgress.Status status = RecoveryProgress.Status.RUNNING;
        private volatile Long finishedAt = null;
        private final Object rateLock = new Object();
        private long nextSlot = System.currentTimeMillis();

        Job(String jobId, List<String> targets, RecoveryRequest request) {
            this.jobId = jobId;
            this.targets = targets;
            this.request = request;
            this.total = targets.size();
        }

        int processed() {
            return replayed.get() + skipped.get() + failed.get();
        }

        void requestStop() {
            stopRequested.set(true);
            if (status == RecoveryProgress.Status.RUNNING) {
                status = RecoveryProgress.Status.STOPPING;
            }
        }

        RecoveryProgress snapshot() {
            return new RecoveryProgress(jobId, status, total,
                    replayed.get(), skipped.get(), failed.get(), inFlight.get(), startedAt, finishedAt);
        }

        void run() {
            if (total == 0) {
                // 零目标：如实报告“这次一个目标都没有、实际恢复 0 条”，不伪装成 COMPLETED
                // 先写 finishedAt 再写 status，保证看到 NO_TARGETS 的快照一定带有完成时间
                finishedAt = System.currentTimeMillis();
                status = RecoveryProgress.Status.NO_TARGETS;
                log.info("批量恢复任务零目标直接结束: jobId={} 状态=NO_TARGETS total=0 replayed=0"
                        + "（未给条件且当前无死信，或筛选条件未命中任何死信）", jobId);
                return;
            }
            ExecutorService pool = Executors.newFixedThreadPool(request.concurrency(), r -> {
                Thread t = new Thread(r, "dead-letter-recovery-worker");
                t.setDaemon(true);
                return t;
            });
            Semaphore inFlightPermits = new Semaphore(request.concurrency());
            try {
                for (String eventId : targets) {
                    if (stopRequested.get()) {
                        break;
                    }
                    awaitRateSlot();
                    if (stopRequested.get()) {
                        break;
                    }
                    // 在途名额：占满则暂停注入，直到有恢复事件到达终态释放名额
                    if (!acquirePermit(inFlightPermits)) {
                        break;
                    }
                    // 拿到名额后再确认一次：停止时worker释放的名额不得被用来注入新重放
                    if (stopRequested.get()) {
                        inFlightPermits.release();
                        break;
                    }
                    pool.submit(() -> {
                        try {
                            if (replayOne(eventId)) {
                                awaitSettled(eventId);
                            }
                        } finally {
                            inFlightPermits.release();
                        }
                    });
                }
                // 等待在途事件全部到达终态（或停止请求使其放弃等待），保证进度计数完整
                drain(inFlightPermits);
                status = stopRequested.get()
                        ? RecoveryProgress.Status.STOPPED
                        : RecoveryProgress.Status.COMPLETED;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                status = RecoveryProgress.Status.STOPPED;
            } catch (Throwable t) {
                log.error("批量恢复任务异常终止: jobId={} 原因={}", jobId, t.toString(), t);
                status = RecoveryProgress.Status.FAILED;
            } finally {
                finishedAt = System.currentTimeMillis();
                pool.shutdownNow();
                log.info("批量恢复任务结束: jobId={} 状态={} 总数={} 已重放={} 跳过={} 失败={} 在途={}",
                        jobId, status, total, replayed.get(), skipped.get(), failed.get(), inFlight.get());
            }
        }

        /** @return true 表示确实发生了重放（需要等待其到达终态）；false 表示幂等跳过或失败。 */
        private boolean replayOne(String eventId) {
            try {
                store.replay(eventId, System.currentTimeMillis(), "batch-recovery:" + jobId);
                replayed.incrementAndGet();
                inFlight.incrementAndGet();
                log.info("批量恢复重放成功: jobId={} eventId={} 进度={}/{} 在途={}/{}",
                        jobId, eventId, processed(), total, inFlight.get(), request.concurrency());
                return true;
            } catch (EventException e) {
                // 已不在死信态（被其它路径重放/重复提交同一批）：幂等跳过，不产生重复副作用
                skipped.incrementAndGet();
                log.info("批量恢复幂等跳过: jobId={} eventId={} 判定依据={}", jobId, eventId, e.getMessage());
                return false;
            } catch (Throwable t) {
                failed.incrementAndGet();
                log.error("批量恢复重放失败: jobId={} eventId={} 原因={}", jobId, eventId, t.toString(), t);
                return false;
            }
        }

        /**
         * 等待已重放事件在处理链路上到达终态（SUCCEEDED / 再次 DEAD_LETTER），
         * 到达后释放在途名额；收到停止请求时放弃等待（事件保持 PENDING，由正常分发链路继续处理）。
         */
        private void awaitSettled(String eventId) {
            while (!stopRequested.get()) {
                Optional<EventRecord> current = store.findByEventId(eventId);
                if (current.isEmpty()) {
                    log.warn("批量恢复在途事件消失（按已终态处理）: jobId={} eventId={}", jobId, eventId);
                    break;
                }
                EventStatus s = current.get().status();
                if (s == EventStatus.SUCCEEDED || s == EventStatus.DEAD_LETTER) {
                    log.info("批量恢复在途事件到达终态: jobId={} eventId={} 终态={} 在途={}/{}",
                            jobId, eventId, s, inFlight.get() - 1, request.concurrency());
                    break;
                }
                try {
                    Thread.sleep(SETTLE_POLL_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            inFlight.decrementAndGet();
        }

        /** 获取一个在途名额；名额占满时阻塞等待，停止请求可打断。@return false 表示被停止打断且未获取名额。 */
        private boolean acquirePermit(Semaphore permits) throws InterruptedException {
            while (!permits.tryAcquire()) {
                if (stopRequested.get()) {
                    return false;
                }
                Thread.sleep(SETTLE_POLL_MILLIS);
            }
            return true;
        }

        /** 等待全部在途名额归还（在途事件均已收尾）。 */
        private void drain(Semaphore permits) throws InterruptedException {
            int acquired = 0;
            while (acquired < request.concurrency()) {
                if (permits.tryAcquire()) {
                    acquired++;
                } else {
                    Thread.sleep(SETTLE_POLL_MILLIS);
                }
            }
        }

        /** 全局限速：按 ratePerSecond 分配时间槽，停止请求可打断睡眠。 */
        private void awaitRateSlot() throws InterruptedException {
            long intervalMillis = Math.max(1L, Math.round(1000.0 / request.ratePerSecond()));
            long sleepMs;
            synchronized (rateLock) {
                long now = System.currentTimeMillis();
                long slot = Math.max(now, nextSlot);
                nextSlot = slot + intervalMillis;
                sleepMs = slot - now;
            }
            while (sleepMs > 0 && !stopRequested.get()) {
                long chunk = Math.min(sleepMs, 50);
                Thread.sleep(chunk);
                sleepMs -= chunk;
            }
        }
    }

    // ------------------------------------------------------------------ 生命周期

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        running.set(false);
        jobs.values().forEach(Job::requestStop);
        log.info("批量恢复服务已停止（进行中的任务已收到停止请求）");
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

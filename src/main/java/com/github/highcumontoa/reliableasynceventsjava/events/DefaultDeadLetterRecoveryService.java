package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批量死信恢复的默认实现。
 *
 * <p>目标选择：显式 {@code eventIds} 与条件筛选（type/reason/before）取并集去重；
 * <b>两者都未指定时，语义为“恢复当前全部死信”</b>（启动时快照，任务期间不受新死信影响）。
 * 若快照目标为空（如给了筛选条件但一条都没命中），任务直接以
 * {@link RecoveryProgress.Status#COMPLETED_EMPTY} 结束，与“有目标且全部处理完”的
 * {@link RecoveryProgress.Status#COMPLETED} 明确区分。</p>
 *
 * <p>流量控制（两个参数各管一段）：</p>
 * <ul>
 *   <li>{@code ratePerSecond}：全局限速，约束把死信重放回 PENDING 的注入速率；</li>
 *   <li>{@code concurrency}：并发上限，约束<b>已重放、但尚未到达终态</b>（SUCCEEDED/DEAD_LETTER）
 *       的恢复事件数。每条事件重放成功即占用一个额度，由在途观察者轮询存储、
 *       确认到达终态后才释放；额度占满时注入暂停。因此慢消费者/下游积压时，
 *       恢复流量会自动停下来，不会把处理链路打满。</li>
 * </ul>
 *
 * <p>单条重放仍是原子的 WAL 批次（REPLAY + HISTORY 同批 fsync），进程在任意时刻被强杀，
 * 已重放的事件保持 PENDING、未重放的保持死信，重启后再次发起任务即可续跑，无半截状态。</p>
 *
 * <p>幂等：目标事件若已不在死信态（被其它路径重放或重复提交同一批）则计为 skipped，
 * 不重复写历史、不产生重复副作用；同一事件在同一任务内只处理一次（启动时快照去重）。
 * 重放只把事件置回 PENDING 并按重放时刻参与 {@code nextAttemptAt} 公平排队，
 * 正常新事件按同一规则竞争领取，不会被恢复流量饿死。</p>
 */
public class DefaultDeadLetterRecoveryService implements DeadLetterRecoveryService, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DefaultDeadLetterRecoveryService.class);

    /** 在途观察者轮询存储确认终态的间隔。 */
    private static final long WATCH_INTERVAL_MILLIS = 20;

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
            // 指定了筛选条件 -> 按条件筛选；什么条件都没给 -> 全量恢复当前所有死信
            for (EventRecord ev : store.findDeadLetters()) {
                if (request.eventType() != null && !request.eventType().equals(ev.type())) {
                    continue;
                }
                if (request.reason() != null && request.reason() != ev.deadLetterReason()) {
                    continue;
                }
                if (request.deadLetteredBeforeMillis() != null
                        && ev.deadLetterAtMillis() > request.deadLetteredBeforeMillis()) {
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
        log.info("批量恢复任务已启动: jobId={} 目标={} 速率={}/s 并发={} 选目标方式={} 筛选(type={}, reason={}, before={}) 显式ID={}",
                jobId, targets.size(), request.ratePerSecond(), request.concurrency(),
                !hasIds && !hasFilter ? "未指定任何条件->全量恢复当前所有死信" : "显式列表与条件筛选取并集",
                request.eventType(), request.reason(), request.deadLetteredBeforeMillis(),
                request.eventIds() == null ? 0 : request.eventIds().size());
        if (targets.isEmpty()) {
            log.info("批量恢复任务无匹配目标: jobId={} 本次不会恢复任何死信（将以 COMPLETED_EMPTY 结束）", jobId);
        }
        return jobId;
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
        log.info("批量恢复任务收到停止请求: jobId={} 已处理={}/{}", jobId, job.processed(), job.total);
        return Optional.of(job.snapshot());
    }

    /** 单条重放的结果分类。 */
    private enum ReplayOutcome { REPLAYED, SKIPPED, FAILED }

    /** 单个恢复任务的内部状态。 */
    private final class Job {
        private final String jobId;
        private final List<String> targets;
        private final RecoveryRequest request;
        private final int total;
        private final AtomicInteger replayed = new AtomicInteger();
        private final AtomicInteger skipped = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicBoolean stopRequested = new AtomicBoolean(false);
        private final long startedAt = System.currentTimeMillis();
        private volatile RecoveryProgress.Status status = RecoveryProgress.Status.RUNNING;
        private volatile Long finishedAt = null;
        private final Object rateLock = new Object();
        private long nextSlot = System.currentTimeMillis();
        /** 并发额度：每条“已重放、未到终态”的恢复事件占一个，占满则暂停注入。 */
        private final Semaphore inFlightPermits;
        /** 已重放、尚未到达终态的事件（在途观察者确认终态后移除并释放额度）。 */
        private final Set<String> inFlightEvents = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean watcherDone = new AtomicBoolean(false);

        Job(String jobId, List<String> targets, RecoveryRequest request) {
            this.jobId = jobId;
            this.targets = targets;
            this.request = request;
            this.total = targets.size();
            this.inFlightPermits = new Semaphore(request.concurrency());
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
                    replayed.get(), skipped.get(), failed.get(),
                    inFlightEvents.size(), startedAt, finishedAt);
        }

        void run() {
            if (targets.isEmpty()) {
                // 没有任何匹配目标：如实上报“本次一个目标都没有、实际恢复 0 条”，不伪装成成功
                status = RecoveryProgress.Status.COMPLETED_EMPTY;
                finishedAt = System.currentTimeMillis();
                log.info("批量恢复任务结束: jobId={} 状态={} 总数=0 已重放=0 跳过=0 失败=0（无匹配目标，未恢复任何死信）",
                        jobId, status);
                return;
            }
            ExecutorService pool = Executors.newFixedThreadPool(request.concurrency(), r -> {
                Thread t = new Thread(r, "dead-letter-recovery-worker");
                t.setDaemon(true);
                return t;
            });
            Thread watcher = new Thread(this::watchInFlight, "dead-letter-recovery-watcher");
            watcher.setDaemon(true);
            watcher.start();
            try {
                for (String eventId : targets) {
                    if (stopRequested.get()) {
                        break;
                    }
                    awaitRateSlot();
                    if (stopRequested.get()) {
                        break;
                    }
                    // 并发闸门：已重放未到终态的事件达到上限时在此阻塞，直到有事件到达终态释放额度
                    if (!acquirePermit()) {
                        break;
                    }
                    pool.submit(() -> {
                        ReplayOutcome outcome = replayOne(eventId);
                        if (outcome == ReplayOutcome.REPLAYED) {
                            // 额度继续持有，由在途观察者确认终态后释放
                            inFlightEvents.add(eventId);
                        } else {
                            inFlightPermits.release();
                        }
                    });
                }
                // 等待已提交的重放动作收尾，保证进度计数完整
                pool.shutdown();
                pool.awaitTermination(1, TimeUnit.HOURS);
                // 等待所有已重放事件到达终态再落幕：任务结束时在途数归零，
                // COMPLETED/STOPPED 都是“账目已清”的稳定状态
                waitForInFlightDrain();
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
                watcherDone.set(true);
                try {
                    watcher.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                pool.shutdownNow();
                log.info("批量恢复任务结束: jobId={} 状态={} 总数={} 已重放={} 跳过={} 失败={} 仍在途(未到终态)={}",
                        jobId, status, total, replayed.get(), skipped.get(), failed.get(),
                        inFlightEvents.size());
            }
        }

        /** 获取一个并发额度；停止请求到达时立即放弃并返回 false。 */
        private boolean acquirePermit() throws InterruptedException {
            while (!stopRequested.get()) {
                if (inFlightPermits.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
            return false;
        }

        /** 等待全部已重放事件到达终态（在途归零）。已重放的事件由正常分发链路推进，终会落终态。 */
        private void waitForInFlightDrain() throws InterruptedException {
            while (!inFlightEvents.isEmpty()) {
                Thread.sleep(WATCH_INTERVAL_MILLIS);
            }
        }

        /**
         * 在途观察者：轮询存储，确认每条已重放事件到达终态（SUCCEEDED/DEAD_LETTER）后
         * 释放其并发额度。这正是 concurrency 能管住“处理链路上流转的恢复事件数”的关键：
         * 下游处理慢、事件迟迟不到终态时，额度不释放，注入自然暂停。
         */
        private void watchInFlight() {
            while (!watcherDone.get()) {
                for (String eventId : inFlightEvents) {
                    Optional<EventRecord> ev = store.findByEventId(eventId);
                    boolean terminal = ev.isPresent()
                            && (ev.get().status() == EventStatus.SUCCEEDED
                                || ev.get().status() == EventStatus.DEAD_LETTER);
                    if (terminal && inFlightEvents.remove(eventId)) {
                        inFlightPermits.release();
                        log.info("恢复事件到达终态，释放并发额度: jobId={} eventId={} 终态={} 仍在途={}",
                                jobId, eventId, ev.get().status(), inFlightEvents.size());
                    }
                }
                try {
                    Thread.sleep(WATCH_INTERVAL_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        private ReplayOutcome replayOne(String eventId) {
            try {
                store.replay(eventId, System.currentTimeMillis(), "batch-recovery:" + jobId);
                replayed.incrementAndGet();
                log.info("批量恢复重放成功: jobId={} eventId={} 进度={}/{} 在途={}",
                        jobId, eventId, processed(), total, inFlightEvents.size() + 1);
                return ReplayOutcome.REPLAYED;
            } catch (EventException e) {
                // 已不在死信态（被其它路径重放/重复提交同一批）：幂等跳过，不产生重复副作用
                skipped.incrementAndGet();
                log.info("批量恢复幂等跳过: jobId={} eventId={} 判定依据={}", jobId, eventId, e.getMessage());
                return ReplayOutcome.SKIPPED;
            } catch (Throwable t) {
                failed.incrementAndGet();
                log.error("批量恢复重放失败: jobId={} eventId={} 原因={}", jobId, eventId, t.toString(), t);
                return ReplayOutcome.FAILED;
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

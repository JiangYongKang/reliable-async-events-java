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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批量死信恢复的默认实现。
 *
 * <p>每个恢复任务在后台线程中执行：按 {@code ratePerSecond} 全局限速、
 * 以 {@code concurrency} 为并发上限，把目标死信逐条 {@link EventStore#replay} 回 PENDING。
 * 单条重放仍是原子的 WAL 批次（REPLAY + HISTORY 同批 fsync），进程在任意时刻被强杀，
 * 已重放的事件保持 PENDING、未重放的保持死信，重启后再次发起任务即可续跑，无半截状态。</p>
 *
 * <p>幂等：目标事件若已不在死信态（被其它路径重放或重复提交同一批）则计为 skipped，
 * 不重复写历史、不产生重复副作用；同一事件在同一任务内只处理一次（启动时快照去重）。</p>
 */
public class DefaultDeadLetterRecoveryService implements DeadLetterRecoveryService, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DefaultDeadLetterRecoveryService.class);

    private final EventStore store;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public DefaultDeadLetterRecoveryService(EventStore store) {
        this.store = store;
    }

    @Override
    public String startRecovery(RecoveryRequest request) {
        // 启动时快照目标集合：显式列表与条件筛选取并集去重，任务期间不受新死信影响
        LinkedHashSet<String> targets = new LinkedHashSet<>();
        if (request.eventIds() != null) {
            targets.addAll(request.eventIds());
        }
        if (request.eventType() != null || request.reason() != null
                || request.deadLetteredBeforeMillis() != null) {
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
        log.info("批量恢复任务已启动: jobId={} 目标={} 速率={}/s 并发={} 筛选(type={}, reason={}, before={}) 显式ID={}",
                jobId, targets.size(), request.ratePerSecond(), request.concurrency(),
                request.eventType(), request.reason(), request.deadLetteredBeforeMillis(),
                request.eventIds() == null ? 0 : request.eventIds().size());
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
                    replayed.get(), skipped.get(), failed.get(), startedAt, finishedAt);
        }

        void run() {
            ExecutorService pool = Executors.newFixedThreadPool(request.concurrency(), r -> {
                Thread t = new Thread(r, "dead-letter-recovery-worker");
                t.setDaemon(true);
                return t;
            });
            Semaphore inFlight = new Semaphore(request.concurrency());
            try {
                for (String eventId : targets) {
                    if (stopRequested.get()) {
                        break;
                    }
                    awaitRateSlot();
                    if (stopRequested.get()) {
                        break;
                    }
                    inFlight.acquire();
                    pool.submit(() -> {
                        try {
                            replayOne(eventId);
                        } finally {
                            inFlight.release();
                        }
                    });
                }
                // 等待在途重放收尾，保证进度计数完整
                inFlight.acquire(request.concurrency());
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
                log.info("批量恢复任务结束: jobId={} 状态={} 总数={} 已重放={} 跳过={} 失败={}",
                        jobId, status, total, replayed.get(), skipped.get(), failed.get());
            }
        }

        private void replayOne(String eventId) {
            try {
                store.replay(eventId, System.currentTimeMillis(), "batch-recovery:" + jobId);
                replayed.incrementAndGet();
                log.info("批量恢复重放成功: jobId={} eventId={} 进度={}/{}", jobId, eventId, processed(), total);
            } catch (EventException e) {
                // 已不在死信态（被其它路径重放/重复提交同一批）：幂等跳过，不产生重复副作用
                skipped.incrementAndGet();
                log.info("批量恢复幂等跳过: jobId={} eventId={} 判定依据={}", jobId, eventId, e.getMessage());
            } catch (Throwable t) {
                failed.incrementAndGet();
                log.error("批量恢复重放失败: jobId={} eventId={} 原因={}", jobId, eventId, t.toString(), t);
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

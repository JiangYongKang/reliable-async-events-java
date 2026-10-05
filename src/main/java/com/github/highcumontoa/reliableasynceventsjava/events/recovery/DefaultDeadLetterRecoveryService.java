package com.github.highcumontoa.reliableasynceventsjava.events.recovery;

import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeUnit;

/**
 * 死信批量恢复默认实现。
 *
 * <p>工作方式：启动时对待恢复集合做一次快照（显式 ID 与条件筛选取并集、去重），
 * 后台线程按 {@code ratePerSecond} 限速、按 {@code concurrency} 限制“已重放未终态”的
 * 在途数量，逐条把死信重放回 PENDING，之后由正常分发链路处理。</p>
 *
 * <p>一致性与幂等：</p>
 * <ul>
 *   <li>重放本身只是把 DEAD_LETTER 原子置回 PENDING（WAL 同批写入历史），不产生业务副作用；</li>
 *   <li>每条事件重放前重新检查状态，已不在死信态（重复发起、并发重放、停止后重发）直接跳过，
 *       因此同一批重复提交或停止后再次恢复都不会产生重复副作用；</li>
 *   <li>进程重启后：已重放的事件保持 PENDING 继续处理，未重放的仍是死信，
 *       重新发起同样的恢复请求即可幂等续跑（恢复任务状态本身不持久化）；</li>
 *   <li>在途数量上限保证新提交的正常事件不会被恢复流量饿死。</li>
 * </ul>
 */
public class DefaultDeadLetterRecoveryService implements DeadLetterRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(DefaultDeadLetterRecoveryService.class);

    private final EventStore store;
    private final Map<String, RecoveryTask> tasks = new ConcurrentHashMap<>();
    private final ExecutorService executor;

    public DefaultDeadLetterRecoveryService(EventStore store) {
        this.store = store;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "dead-letter-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public String startRecovery(RecoveryRequest request) {
        List<String> targets = snapshotTargets(request);
        String recoveryId = UUID.randomUUID().toString();
        RecoveryTask task = new RecoveryTask(recoveryId, request, targets);
        tasks.put(recoveryId, task);
        log.info("【输入】批量恢复启动: recoveryId={} 目标={} 条 (显式ID={} + 条件筛选: type={} reason={} before={}) "
                        + "concurrency={} rate={}/s",
                recoveryId, targets.size(),
                request.eventIds() == null ? 0 : request.eventIds().size(),
                request.type(), request.reason(), request.deadLetteredBeforeMillis(),
                request.concurrency(), request.ratePerSecond());
        executor.submit(() -> runRecovery(task));
        return recoveryId;
    }

    /** 启动时快照目标集合：显式 ID 并上条件筛选结果，去重，且仅保留当前确为死信的事件。 */
    private List<String> snapshotTargets(RecoveryRequest request) {
        Set<String> ids = new LinkedHashSet<>();
        if (request.eventIds() != null) {
            ids.addAll(request.eventIds());
        }
        // 条件筛选：有任一过滤条件、或未给显式 ID 列表（纯筛选模式，无过滤条件时匹配全部死信）
        boolean hasFilter = request.type() != null || request.reason() != null
                || request.deadLetteredBeforeMillis() != null;
        if (hasFilter || request.eventIds() == null) {
            for (EventRecord ev : store.findDeadLetters()) {
                if (request.type() != null && !request.type().equals(ev.type())) {
                    continue;
                }
                if (request.reason() != null && request.reason() != ev.deadLetterReason()) {
                    continue;
                }
                if (request.deadLetteredBeforeMillis() != null
                        && ev.deadLetterAtMillis() > request.deadLetteredBeforeMillis()) {
                    continue;
                }
                ids.add(ev.eventId());
            }
        }
        List<String> targets = new ArrayList<>();
        for (String id : ids) {
            Optional<EventRecord> ev = store.findByEventId(id);
            if (ev.isPresent() && ev.get().status() == EventStatus.DEAD_LETTER) {
                targets.add(id);
            } else {
                log.info("目标事件 {} 当前不是死信（{}），跳过（幂等去重）",
                        id, ev.map(e -> e.status().name()).orElse("不存在"));
            }
        }
        return targets;
    }

    private void runRecovery(RecoveryTask task) {
        long minIntervalMillis = Math.max(1, (long) (1000.0 / task.request.ratePerSecond()));
        long lastReplayAt = 0;
        int cursor = 0;
        try {
            while (task.status == RecoveryStatus.RUNNING) {
                refreshTerminalCounts(task);
                if (cursor >= task.targets.size()) {
                    if (task.inFlight() == 0) {
                        task.status = RecoveryStatus.COMPLETED;
                        log.info("【状态变化】批量恢复完成: recoveryId={} 共 {} 条, 成功={} 再次死信={}",
                                task.recoveryId, task.targets.size(), task.succeeded, task.deadAgain);
                        return;
                    }
                    sleepQuietly(20);
                    continue;
                }
                if (task.inFlight() >= task.request.concurrency()) {
                    sleepQuietly(20);
                    continue;
                }
                long now = System.currentTimeMillis();
                long wait = minIntervalMillis - (now - lastReplayAt);
                if (wait > 0) {
                    sleepQuietly(wait);
                    continue;
                }
                String eventId = task.targets.get(cursor++);
                replayOne(task, eventId);
                lastReplayAt = System.currentTimeMillis();
            }
            log.info("【状态变化】批量恢复被停止: recoveryId={} 已重放={}/{} 未重放的保持死信，可再次发起恢复（幂等）",
                    task.recoveryId, task.replayedCount(), task.targets.size());
        } catch (Throwable t) {
            task.status = RecoveryStatus.STOPPED;
            log.error("批量恢复异常终止: recoveryId={} 已重放={}/{} 原因={}",
                    task.recoveryId, task.replayedCount(), task.targets.size(), t.toString(), t);
        }
    }

    /** 重放单条：重放前重新检查状态，非死信直接跳过（重复发起/并发/重启续跑均幂等）。 */
    private void replayOne(RecoveryTask task, String eventId) {
        Optional<EventRecord> current = store.findByEventId(eventId);
        if (current.isEmpty()) {
            log.warn("目标事件 {} 已不存在，跳过", eventId);
            task.markReplayed(eventId);
            return;
        }
        if (current.get().status() != EventStatus.DEAD_LETTER) {
            log.info("事件 {} 当前状态={} 非死信，跳过重放（幂等：可能由重复批次/并发已恢复）",
                    eventId, current.get().status());
            task.markReplayed(eventId);
            return;
        }
        store.replay(eventId, "BATCH_RECOVERY:" + task.recoveryId, System.currentTimeMillis());
        task.markReplayed(eventId);
        log.info("【状态变化】批量恢复重放: recoveryId={} eventId={} DEAD_LETTER->PENDING ({}/{})",
                task.recoveryId, eventId, task.replayedCount(), task.targets.size());
    }

    private void refreshTerminalCounts(RecoveryTask task) {
        int succeeded = 0;
        int deadAgain = 0;
        for (String id : task.replayed) {
            Optional<EventRecord> ev = store.findByEventId(id);
            if (ev.isPresent()) {
                if (ev.get().status() == EventStatus.SUCCEEDED) {
                    succeeded++;
                } else if (ev.get().status() == EventStatus.DEAD_LETTER) {
                    deadAgain++;
                }
            }
        }
        task.succeeded = succeeded;
        task.deadAgain = deadAgain;
    }

    @Override
    public RecoveryProgress progress(String recoveryId) {
        RecoveryTask task = tasks.get(recoveryId);
        if (task == null) {
            return null;
        }
        refreshTerminalCounts(task);
        int replayed = task.replayedCount();
        int inFlight = task.inFlight();
        return new RecoveryProgress(recoveryId, task.status, task.targets.size(),
                replayed, task.succeeded, task.deadAgain, inFlight,
                task.targets.size() - replayed, task.startedAtMillis);
    }

    @Override
    public void stop(String recoveryId) {
        RecoveryTask task = tasks.get(recoveryId);
        if (task == null) {
            throw new IllegalArgumentException("恢复任务不存在: " + recoveryId);
        }
        if (task.status == RecoveryStatus.RUNNING) {
            task.status = RecoveryStatus.STOPPED;
            log.info("收到停止请求: recoveryId={}（已重放的事件继续处理完）", recoveryId);
        }
    }

    /** 关闭后台线程（Spring 容器销毁时调用）。 */
    public void shutdown() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 单个恢复任务的可变运行状态（仅本类内部使用）。 */
    private static final class RecoveryTask {
        final String recoveryId;
        final RecoveryRequest request;
        final List<String> targets;
        final Set<String> replayed = ConcurrentHashMap.newKeySet();
        final long startedAtMillis = System.currentTimeMillis();
        volatile RecoveryStatus status = RecoveryStatus.RUNNING;
        volatile int succeeded;
        volatile int deadAgain;

        RecoveryTask(String recoveryId, RecoveryRequest request, List<String> targets) {
            this.recoveryId = recoveryId;
            this.request = request;
            this.targets = targets;
        }

        void markReplayed(String eventId) {
            replayed.add(eventId);
        }

        int replayedCount() {
            return replayed.size();
        }

        int inFlight() {
            return replayed.size() - succeeded - deadAgain;
        }
    }
}

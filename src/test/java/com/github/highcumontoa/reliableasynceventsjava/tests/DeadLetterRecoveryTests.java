package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterReason;
import com.github.highcumontoa.reliableasynceventsjava.events.DurableWALEventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventDispatcher;
import com.github.highcumontoa.reliableasynceventsjava.events.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.ExponentialBackoffRetryPolicy;
import com.github.highcumontoa.reliableasynceventsjava.events.HandlerRegistry;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventTransition;
import com.github.highcumontoa.reliableasynceventsjava.events.history.TransitionTrigger;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.DefaultDeadLetterRecoveryService;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.RecoveryProgress;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.RecoveryRequest;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.RecoveryStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死信批量恢复：按条件/显式列表恢复、并发与速率受控、进度可查、可停止、
 * 重复发起与停止后续跑均幂等、恢复期间新事件不被饿死。
 */
class DeadLetterRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRecoveryTests.class);

    private DurableWALEventStore newStore(Path dir) {
        return new DurableWALEventStore(new DurableFileLog(dir.toString()),
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 30_000);
    }

    /** 直接构造一批死信（提交 -> 领取 -> 标记死信），返回事件 ID。 */
    private List<String> makeDeadLetters(DurableWALEventStore store, String keyPrefix, String type,
                                         int count, DeadLetterReason reason) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = store.commit(keyPrefix + "-" + i, type, "{}", null, i).event().eventId();
            EventStore.ClaimedEvent c = store.claimNext(1000 + i).orElseThrow();
            store.markDeadLetter(id, c.leaseToken(), reason, "模拟故障-" + i, 2000 + i);
            ids.add(id);
        }
        return ids;
    }

    private static void await(String what, Supplier<Boolean> cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.get()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("等待超时: " + what);
    }

    @Test
    void batchRecoveryRestoresFilteredDeadLetters(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        List<String> fixable = makeDeadLetters(store, "fix", "FIXABLE", 4, DeadLetterReason.NON_RETRYABLE);
        List<String> other = makeDeadLetters(store, "other", "OTHER", 2, DeadLetterReason.VALIDATION_FAILED);
        log.info("【输入】4 条 FIXABLE 死信 + 2 条 OTHER 死信；按 type=FIXABLE 过滤批量恢复, concurrency=2 rate=100/s");

        HandlerRegistry registry = new HandlerRegistry();
        ControllableHandler handler = new ControllableHandler("FIXABLE", ControllableHandler.Mode.OK, 0);
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        try {
            String recoveryId = recovery.startRecovery(
                    new RecoveryRequest(null, "FIXABLE", null, null, 2, 100));
            await("恢复完成", () -> {
                RecoveryProgress p = recovery.progress(recoveryId);
                return p.status() == RecoveryStatus.COMPLETED;
            }, 10_000);

            RecoveryProgress p = recovery.progress(recoveryId);
            log.info("【状态变化】recoveryId={} status={} total={} replayed={} succeeded={} deadAgain={}",
                    recoveryId, p.status(), p.total(), p.replayed(), p.succeeded(), p.deadAgain());
            assertEquals(4, p.total(), "条件筛选只应命中 4 条 FIXABLE 死信");
            assertEquals(4, p.succeeded());
            assertEquals(0, p.deadAgain());
            for (String id : fixable) {
                assertEquals(EventStatus.SUCCEEDED, store.findByEventId(id).orElseThrow().status());
                assertTrue(store.get("effect:" + id).isPresent(), "恢复后副作用必须落盘: " + id);
                List<EventTransition> replays = store.historyOf(id).stream()
                        .filter(t -> t.trigger() == TransitionTrigger.REPLAY).toList();
                assertEquals(1, replays.size(), "每条事件只应被重放一次");
                assertTrue(replays.get(0).detail().contains("BATCH_RECOVERY"),
                        "重放历史必须记录批量恢复来源");
            }
            for (String id : other) {
                assertEquals(EventStatus.DEAD_LETTER, store.findByEventId(id).orElseThrow().status(),
                        "未命中过滤条件的死信不得被恢复");
            }
            log.info("【判定依据】4 条 FIXABLE 全部 SUCCEEDED 且历史含 BATCH_RECOVERY 重放记录；2 条 OTHER 保持死信");
        } finally {
            dispatcher.stop();
            recovery.shutdown();
        }
    }

    @Test
    void stopThenResumeAndDuplicateSubmissionAreIdempotent(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        List<String> ids = makeDeadLetters(store, "idem", "FIXABLE", 6, DeadLetterReason.RETRIES_EXHAUSTED);
        log.info("【输入】6 条死信；第一次恢复限速 2/s 中途停止，再以相同条件 + 重复显式ID列表 发起第二次恢复");

        HandlerRegistry registry = new HandlerRegistry();
        ControllableHandler handler = new ControllableHandler("FIXABLE", ControllableHandler.Mode.OK, 0);
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        try {
            // 第一次：慢速恢复，重放至少 1 条后停止
            String first = recovery.startRecovery(new RecoveryRequest(null, "FIXABLE", null, null, 1, 2));
            await("第一次恢复至少重放 1 条", () -> recovery.progress(first).replayed() >= 1, 5_000);
            recovery.stop(first);
            await("第一次恢复停止", () -> recovery.progress(first).status() == RecoveryStatus.STOPPED, 5_000);
            RecoveryProgress stopped = recovery.progress(first);
            log.info("【状态变化】第一次恢复被停止: replayed={}/{} status={}",
                    stopped.replayed(), stopped.total(), stopped.status());
            assertTrue(stopped.replayed() < 6, "慢速恢复应在中途被停止");

            // 第二次：相同过滤条件 + 显式列表（每条 ID 重复两次，且包含全部 6 条）
            List<String> explicit = new ArrayList<>();
            for (int round = 0; round < 2; round++) {
                explicit.addAll(ids);
            }
            String second = recovery.startRecovery(
                    new RecoveryRequest(explicit, "FIXABLE", null, null, 2, 100));
            await("第二次恢复完成", () -> recovery.progress(second).status() == RecoveryStatus.COMPLETED, 10_000);

            RecoveryProgress done = recovery.progress(second);
            log.info("【状态变化】第二次恢复完成: total={} replayed={} succeeded={}",
                    done.total(), done.replayed(), done.succeeded());
            assertEquals(6, countByStatus(store, ids, EventStatus.SUCCEEDED), "全部事件最终一致跑完");
            for (String id : ids) {
                assertEquals(EventStatus.SUCCEEDED, store.findByEventId(id).orElseThrow().status());
                long replayCount = store.historyOf(id).stream()
                        .filter(t -> t.trigger() == TransitionTrigger.REPLAY).count();
                assertEquals(1, replayCount, "同一事件跨两次恢复也只能被重放一次: " + id);
                assertEquals(1, handler.perEventInvocations.get(id).intValue(),
                        "OK 处理器对每条事件只应执行一次（无重复副作用）: " + id);
                assertTrue(store.get("effect:" + id).isPresent());
            }
            assertEquals(6, handler.invocationCount.get(), "处理器总调用次数必须等于事件数（恰好一次）");
            log.info("【判定依据】6 条全部 SUCCEEDED；每条 REPLAY 历史=1、处理器调用=1 => 停止续跑与重复提交均幂等");
        } finally {
            dispatcher.stop();
            recovery.shutdown();
        }
    }

    private static int countByStatus(DurableWALEventStore store, List<String> ids, EventStatus status) {
        int n = 0;
        for (String id : ids) {
            if (store.findByEventId(id).orElseThrow().status() == status) {
                n++;
            }
        }
        return n;
    }

    @Test
    void newEventsAreNotStarvedDuringRecovery(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        List<String> old = makeDeadLetters(store, "old", "SLOW", 5, DeadLetterReason.NON_RETRYABLE);
        log.info("【输入】5 条慢处理死信（每条处理 400ms）以 concurrency=1 恢复；恢复期间提交 1 条新事件");

        HandlerRegistry registry = new HandlerRegistry();
        EventHandler slowHandler = new EventHandler() {
            @Override
            public String type() {
                return "SLOW";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Map.of("effect:" + event.eventId(), "done");
            }
        };
        registry.register(slowHandler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        try {
            String recoveryId = recovery.startRecovery(
                    new RecoveryRequest(null, "SLOW", null, null, 1, 50));
            // 等恢复真正开始（至少 1 条在途）
            await("恢复开始", () -> recovery.progress(recoveryId).replayed() >= 1, 5_000);

            String newId = store.commit("new-1", "SLOW", "{}", null,
                    System.currentTimeMillis()).event().eventId();
            long newSubmitAt = System.currentTimeMillis();
            await("新事件被处理", () -> store.findByEventId(newId).orElseThrow().status()
                    == EventStatus.SUCCEEDED, 5_000);
            long newLatency = System.currentTimeMillis() - newSubmitAt;
            RecoveryProgress during = recovery.progress(recoveryId);
            log.info("【状态变化】新事件 {}ms 内处理完成；此时恢复进度 replayed={}/{} succeeded={}",
                    newLatency, during.replayed(), during.total(), during.succeeded());
            assertTrue(newLatency < 5 * 400, "新事件必须插队被处理，不能被恢复流量饿死");

            await("恢复完成", () -> recovery.progress(recoveryId).status() == RecoveryStatus.COMPLETED, 15_000);
            for (String id : old) {
                assertEquals(EventStatus.SUCCEEDED, store.findByEventId(id).orElseThrow().status());
            }
            log.info("【判定依据】新事件在恢复进行中快速完成，且 5 条老事件最终也全部一致跑完 => 不饿死、不丢失");
        } finally {
            dispatcher.stop();
            recovery.shutdown();
        }
    }

    @Test
    void unfixableEventsGoDeadAgainAndProgressReflectsIt(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        List<String> ids = makeDeadLetters(store, "broken", "BROKEN", 2, DeadLetterReason.NON_RETRYABLE);
        log.info("【输入】2 条根因未修复的死信（处理器仍抛永久故障），批量恢复后应再次进入死信");

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(new ControllableHandler("BROKEN", ControllableHandler.Mode.PERMANENT, 0));
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        try {
            String recoveryId = recovery.startRecovery(
                    new RecoveryRequest(null, "BROKEN", null, null, 2, 100));
            await("恢复完成", () -> recovery.progress(recoveryId).status() == RecoveryStatus.COMPLETED, 10_000);
            RecoveryProgress p = recovery.progress(recoveryId);
            log.info("【状态变化】status={} total={} replayed={} succeeded={} deadAgain={}",
                    p.status(), p.total(), p.replayed(), p.succeeded(), p.deadAgain());
            assertEquals(2, p.deadAgain(), "根因未修复的事件应再次进入死信并计入进度");
            assertEquals(0, p.succeeded());
            for (String id : ids) {
                assertEquals(EventStatus.DEAD_LETTER, store.findByEventId(id).orElseThrow().status());
            }
            log.info("【判定依据】deadAgain=2 且事件终态仍 DEAD_LETTER => 恢复结果可观测、可再次发起");
        } finally {
            dispatcher.stop();
            recovery.shutdown();
        }
    }

    @Test
    void restartDuringRecoveryLeavesNoPartialState(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        List<String> ids = makeDeadLetters(store, "crash", "FIXABLE", 4, DeadLetterReason.NON_RETRYABLE);
        log.info("【输入】4 条死信，慢速恢复进行中模拟进程强杀（丢弃全部内存实例，仅从 WAL 重启）");

        DefaultDeadLetterRecoveryService recovery1 = new DefaultDeadLetterRecoveryService(store);
        String first = recovery1.startRecovery(new RecoveryRequest(null, "FIXABLE", null, null, 1, 5));
        await("崩溃前至少重放 1 条", () -> recovery1.progress(first).replayed() >= 1, 5_000);
        int replayedBeforeCrash = recovery1.progress(first).replayed();
        store.flush();
        recovery1.shutdown(); // 模拟进程退出：恢复任务内存状态丢失，WAL 已落盘
        log.info("【状态变化】崩溃前已重放 {} 条；进程退出", replayedBeforeCrash);

        // 重启：全新存储与恢复服务从同一 WAL 重建
        DurableWALEventStore restarted = newStore(dir);
        long pendingAfterRestart = restarted.findByStatus(EventStatus.PENDING).size();
        long deadAfterRestart = restarted.findByStatus(EventStatus.DEAD_LETTER).size();
        log.info("【状态变化】重启后 PENDING={} DEAD_LETTER={}", pendingAfterRestart, deadAfterRestart);
        assertEquals(replayedBeforeCrash, pendingAfterRestart, "已重放的事件重启后必须保持 PENDING，不回退");
        assertEquals(4 - replayedBeforeCrash, deadAfterRestart, "未重放的事件必须仍是死信，不半截");
        // 历史与状态一致：每条事件历史末态 == 当前状态
        for (String id : ids) {
            List<EventTransition> tl = restarted.historyOf(id);
            assertEquals(restarted.findByEventId(id).orElseThrow().status(),
                    tl.get(tl.size() - 1).toStatus(), "重启后历史末态必须等于当前状态: " + id);
        }

        // 用相同条件再次发起恢复：幂等续跑，最终全部一致跑完
        HandlerRegistry registry = new HandlerRegistry();
        ControllableHandler handler = new ControllableHandler("FIXABLE", ControllableHandler.Mode.OK, 0);
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(restarted, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery2 = new DefaultDeadLetterRecoveryService(restarted);
        dispatcher.start();
        try {
            String second = recovery2.startRecovery(
                    new RecoveryRequest(null, "FIXABLE", null, null, 2, 100));
            await("重启后续跑完成", () -> recovery2.progress(second).status() == RecoveryStatus.COMPLETED,
                    10_000);
            assertEquals(4, countByStatus(restarted, ids, EventStatus.SUCCEEDED));
            for (String id : ids) {
                long replays = restarted.historyOf(id).stream()
                        .filter(t -> t.trigger() == TransitionTrigger.REPLAY).count();
                assertEquals(1, replays, "跨重启每条事件只被重放一次: " + id);
                assertEquals(1, handler.perEventInvocations.get(id).intValue(),
                        "每条事件副作用恰好一次: " + id);
            }
            log.info("【判定依据】重启后无半截状态；续跑后 4 条全部 SUCCEEDED，每条 REPLAY=1、处理=1 => 幂等恢复");
        } finally {
            dispatcher.stop();
            recovery2.shutdown();
        }
    }

    @Test
    void explicitListSkipsNonDeadLetterEvents(@TempDir Path dir) {        DurableWALEventStore store = newStore(dir);
        List<String> dead = makeDeadLetters(store, "d", "FIXABLE", 2, DeadLetterReason.NON_RETRYABLE);
        String aliveId = store.commit("alive", "FIXABLE", "{}", null, 100).event().eventId();
        log.info("【输入】显式列表包含 2 条死信 + 1 条非死信事件 + 重复 ID");

        HandlerRegistry registry = new HandlerRegistry();
        registry.register(new ControllableHandler("FIXABLE", ControllableHandler.Mode.OK, 0));
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 50, 500, 0), 10, 2);
        DefaultDeadLetterRecoveryService recovery = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        try {
            List<String> explicit = List.of(dead.get(0), dead.get(1), aliveId, dead.get(0));
            String recoveryId = recovery.startRecovery(
                    new RecoveryRequest(explicit, null, null, null, 2, 100));
            await("恢复完成", () -> recovery.progress(recoveryId).status() == RecoveryStatus.COMPLETED, 10_000);
            RecoveryProgress p = recovery.progress(recoveryId);
            log.info("【状态变化】total={} replayed={} succeeded={}", p.total(), p.replayed(), p.succeeded());
            assertEquals(2, p.total(), "非死信与重复 ID 必须在快照阶段被剔除");
            assertEquals(2, p.succeeded());
            long aliveReplays = store.historyOf(aliveId).stream()
                    .filter(t -> t.trigger() == TransitionTrigger.REPLAY).count();
            assertEquals(0, aliveReplays, "非死信事件不得被重放");
            log.info("【判定依据】total=2（剔除非死信/重复），非死信事件无 REPLAY 历史 => 显式列表幂等");
        } finally {
            dispatcher.stop();
            recovery.shutdown();
        }
    }
}

package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量死信恢复：无条件整批恢复、零命中如实报告、条件筛选/显式列表、限速与在途并发上限、
 * 进度可查、中途停止、幂等重复提交、停止后续跑、新事件不被饿死、崩溃重启后续跑无重复副作用。
 */
class BatchRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(BatchRecoveryTests.class);

    private DurableWALEventStore newStore(Path dir) {
        return new DurableWALEventStore(new DurableFileLog(dir.toString()),
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), 30_000);
    }

    /** 提交并直接把事件打进死信（绕过耗时重试，专注恢复链路）。 */
    private String deadLetter(DurableWALEventStore store, String key, String type, long at) {
        store.commit(key, type, "{}", null, at);
        String eventId = store.findByIdempotencyKey(key).orElseThrow().eventId();
        EventStore.ClaimedEvent c = store.claimNext(at).orElseThrow();
        store.markDeadLetter(c.event().eventId(), c.leaseToken(),
                DeadLetterReason.NON_RETRYABLE, "模拟永久故障", at + 10);
        return eventId;
    }

    /**
     * 测试用“正常分发链路”驱动器：持续领取到期事件并标记成功，
     * 把已重放的事件推进到终态（等价于生产上一直在跑的 EventDispatcher）。
     */
    private static final class SettlingDriver implements AutoCloseable {
        private final EventStore store;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger succeeded = new AtomicInteger();
        private final Thread thread;

        SettlingDriver(EventStore store) {
            this.store = store;
            this.thread = new Thread(this::loop, "test-settling-driver");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        private void loop() {
            while (running.get()) {
                try {
                    store.claimNext(System.currentTimeMillis()).ifPresent(c -> {
                        store.markSucceeded(c.event().eventId(), c.leaseToken(), null,
                                System.currentTimeMillis());
                        succeeded.incrementAndGet();
                    });
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable ignored) {
                    // 租约竞争等瞬时异常：继续驱动
                }
            }
        }

        @Override
        public void close() {
            running.set(false);
            thread.interrupt();
        }
    }

    private RecoveryProgress awaitTerminal(DeadLetterRecoveryService svc, String jobId, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            RecoveryProgress p = svc.progress(jobId).orElseThrow();
            if (p.status() == RecoveryProgress.Status.COMPLETED
                    || p.status() == RecoveryProgress.Status.STOPPED
                    || p.status() == RecoveryProgress.Status.FAILED
                    || p.status() == RecoveryProgress.Status.NO_TARGETS) {
                return p;
            }
            Thread.sleep(10);
        }
        fail("任务未在 " + timeoutMillis + "ms 内到达终态: " + svc.progress(jobId).orElseThrow());
        return null;
    }

    private void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("等待条件超时: " + what);
    }

    @Test
    void noCriteriaRecoversAllCurrentDeadLetters(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        List<String> all = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            all.add(deadLetter(store, "all-a-" + i, "TYPE_A", 100 + i));
        }
        for (int i = 0; i < 2; i++) {
            all.add(deadLetter(store, "all-b-" + i, "TYPE_B", 200 + i));
        }
        assertEquals(6, store.findDeadLetters().size());
        log.info("【输入】6 条死信(TYPE_A=4, TYPE_B=2)，发起恢复时不给任何条件：无 eventIds、无类型/原因/时间筛选");

        try (SettlingDriver driver = new SettlingDriver(store)) {
            String jobId = svc.startRecovery(new RecoveryRequest(null, null, null, null, 200.0, 2));
            RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);

            assertEquals(RecoveryProgress.Status.COMPLETED, done.status());
            assertEquals(6, done.total(), "不给任何条件时目标必须是当前全部死信");
            assertEquals(6, done.replayed(), "全部死信都应被真正重放，不允许空跑报完成");
            assertEquals(0, done.skipped());
            assertEquals(0, done.failed());
            assertEquals(0, store.findDeadLetters().size(), "恢复后不应再有任何死信");
            for (String id : all) {
                assertTrue(store.historyOf(id).stream()
                        .anyMatch(r -> r.action() == TransitionAction.REPLAYED
                                && r.trigger().equals("batch-recovery:" + jobId)));
            }
            log.info("【状态变化】进度 total=6 replayed=6 skipped=0 failed=0，死信 6 -> 0，6 条事件均被驱动到终态");
            log.info("【判定依据】无 eventIds 且无筛选条件 => 语义为恢复当前全部死信；每条事件历史含 REPLAYED(trigger=batch-recovery:jobId)");
        }
    }

    @Test
    void noMatchReportsNoTargetsInsteadOfFakeSuccess(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        for (int i = 0; i < 3; i++) {
            deadLetter(store, "m-" + i, "TYPE_A", 100 + i);
        }
        log.info("【输入】3 条 TYPE_A 死信，按 eventType=TYPE_B 筛选恢复（一条都命中不了）");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "TYPE_B", null, null, 100.0, 2));
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);

        assertEquals(RecoveryProgress.Status.NO_TARGETS, done.status(),
                "零命中必须如实报告 NO_TARGETS，不得与“有目标且全部成功”的 COMPLETED 混淆");
        assertEquals(0, done.total());
        assertEquals(0, done.replayed(), "实际恢复条数必须如实为 0");
        assertEquals(0, done.skipped());
        assertEquals(0, done.failed());
        assertEquals(0, done.inFlight());
        assertNotNull(done.finishedAtMillis());
        assertEquals(3, store.findDeadLetters().size(), "未命中的死信必须保持原状");
        log.info("【状态变化】进度 status=NO_TARGETS total=0 replayed=0，死信仍为 3");
        log.info("【判定依据】筛选未命中 => total=0 且状态 NO_TARGETS，与 COMPLETED 明确区分，线上不会误判恢复已生效");
    }

    @Test
    void noCriteriaOnEmptyDeadLettersReportsNoTargets(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        assertEquals(0, store.findDeadLetters().size());
        log.info("【输入】当前没有任何死信，发起不给任何条件的恢复");

        String jobId = svc.startRecovery(new RecoveryRequest(null, null, null, null, 100.0, 2));
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);

        assertEquals(RecoveryProgress.Status.NO_TARGETS, done.status());
        assertEquals(0, done.total());
        assertEquals(0, done.replayed());
        log.info("【状态变化】进度 status=NO_TARGETS total=0 replayed=0");
        log.info("【判定依据】无条件且当前无死信 => 零目标如实报告 NO_TARGETS，而非伪装成恢复成功");
    }

    @Test
    void recoversBatchByFilterWithRateLimitAndProgress(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        List<String> typeA = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            typeA.add(deadLetter(store, "a-" + i, "TYPE_A", 100 + i));
        }
        for (int i = 0; i < 3; i++) {
            deadLetter(store, "b-" + i, "TYPE_B", 200 + i);
        }
        assertEquals(8, store.findDeadLetters().size());
        log.info("【输入】8 条死信(TYPE_A=5, TYPE_B=3)，按 type=TYPE_A 筛选恢复，限速 20/s 并发(在途上限) 2");

        try (SettlingDriver driver = new SettlingDriver(store)) {
            long start = System.currentTimeMillis();
            String jobId = svc.startRecovery(new RecoveryRequest(
                    null, "TYPE_A", null, null, 20.0, 2));
            RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);
            long elapsed = System.currentTimeMillis() - start;

            assertEquals(RecoveryProgress.Status.COMPLETED, done.status());
            assertEquals(5, done.total());
            assertEquals(5, done.replayed());
            assertEquals(0, done.failed());
            assertTrue(elapsed >= 150, "5 条按 20/s 限速，耗时应受速率约束(>=150ms)，实际=" + elapsed + "ms");
            assertEquals(3, store.findDeadLetters().size(), "TYPE_B 死信不应被本批恢复");
            for (String id : typeA) {
                EventRecord ev = store.findByEventId(id).orElseThrow();
                assertEquals(EventStatus.SUCCEEDED, ev.status(),
                        "任务完成意味着已重放事件均已在处理链路上到达终态");
                assertTrue(store.historyOf(id).stream()
                        .anyMatch(r -> r.action() == TransitionAction.REPLAYED
                                && r.trigger().equals("batch-recovery:" + jobId)),
                        "历史必须记录批量恢复来源: " + id);
            }
            log.info("【状态变化】进度 total=5 replayed=5 skipped=0 failed=0, 耗时={}ms, TYPE_B 仍为死信=3", elapsed);
            log.info("【判定依据】筛选只命中 TYPE_A；耗时 >=150ms 证明限速生效；每条事件历史含 REPLAYED(trigger=batch-recovery:jobId)");
        }
    }

    @Test
    void explicitIdsAndDuplicateSubmissionAreIdempotent(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ids.add(deadLetter(store, "e-" + i, "T", 100 + i));
        }
        log.info("【输入】4 条死信，按显式 ID 列表恢复；随后用同一批 ID 重复提交第二次恢复");

        try (SettlingDriver driver = new SettlingDriver(store)) {
            String job1 = svc.startRecovery(new RecoveryRequest(ids, null, null, null, 100.0, 2));
            RecoveryProgress p1 = awaitTerminal(svc, job1, 10_000);
            assertEquals(4, p1.replayed());
            assertEquals(0, store.findDeadLetters().size());

            // 同一批重复提交：事件已不在死信态 -> 全部幂等跳过，无重复副作用
            String job2 = svc.startRecovery(new RecoveryRequest(ids, null, null, null, 100.0, 2));
            RecoveryProgress p2 = awaitTerminal(svc, job2, 10_000);
            assertEquals(RecoveryProgress.Status.COMPLETED, p2.status());
            assertEquals(0, p2.replayed(), "重复提交同一批不得再次重放");
            assertEquals(4, p2.skipped(), "已恢复的事件应计为幂等跳过");
            for (String id : ids) {
                long replays = store.historyOf(id).stream()
                        .filter(r -> r.action() == TransitionAction.REPLAYED).count();
                assertEquals(1, replays, "每条事件只能有一条 REPLAYED 历史: " + id);
            }
            log.info("【状态变化】第一次 replayed=4；第二次 replayed=0 skipped=4；每条事件 REPLAYED 历史=1");
            log.info("【判定依据】replay 仅接受 DEAD_LETTER，重复提交命中幂等跳过分支 => 无重复副作用");
        }
    }

    @Test
    void concurrencyCapsInFlightRecoveredEvents(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        for (int i = 0; i < 6; i++) {
            deadLetter(store, "c-" + i, "T", 100 + i);
        }
        log.info("【输入】6 条死信，速率 1000/s（限速不构成约束）、并发(在途上限)=2，且先不让任何事件到达终态");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 1000.0, 2));
        // 在途上限=2：重放 2 条后名额占满，主循环必须暂停注入
        waitFor(() -> svc.progress(jobId).orElseThrow().replayed() == 2, 5_000, "重放 2 条");
        Thread.sleep(300);
        RecoveryProgress blocked = svc.progress(jobId).orElseThrow();
        assertEquals(2, blocked.replayed(), "在途名额占满后不得继续重放，实际=" + blocked.replayed());
        assertEquals(2, blocked.inFlight(), "在途数必须等于并发上限 2");
        assertEquals(4, store.findDeadLetters().size(), "仅 2 条离开死信态");
        log.info("【状态变化】在途名额占满: replayed=2 inFlight=2/2 剩余死信=4（300ms 内无新增重放）");

        // 手动把 1 条在途事件推到终态 -> 释放 1 个名额 -> 主循环立即再注入 1 条
        EventStore.ClaimedEvent c = store.claimNext(System.currentTimeMillis()).orElseThrow();
        store.markSucceeded(c.event().eventId(), c.leaseToken(), null, System.currentTimeMillis());
        log.info("【输入】手动将事件 {} 推到终态 SUCCEEDED，释放一个在途名额", c.event().eventId());
        waitFor(() -> svc.progress(jobId).orElseThrow().replayed() == 3, 5_000, "名额释放后再重放 1 条");
        Thread.sleep(300);
        RecoveryProgress afterOne = svc.progress(jobId).orElseThrow();
        assertEquals(3, afterOne.replayed(), "释放 1 个名额后只应再重放 1 条");
        assertEquals(2, afterOne.inFlight(), "在途数重新顶到上限 2");
        assertEquals(3, store.findDeadLetters().size());
        log.info("【状态变化】释放 1 个名额后: replayed=3 inFlight=2/2 剩余死信=3");

        svc.stop(jobId);
        RecoveryProgress stopped = awaitTerminal(svc, jobId, 10_000);
        assertEquals(RecoveryProgress.Status.STOPPED, stopped.status());
        assertEquals(3, stopped.replayed(), "停止后不得再有新增重放");
        log.info("【判定依据】replayed 从 2 到 3 完全由“在途事件到达终态释放名额”触发，"
                + "且任意时刻 inFlight<=2 => 并发上限真正约束在处理链路上流转的恢复事件数，而非只约束重放动作本身");
    }

    @Test
    void stopHaltsRecoveryAndLaterJobResumesWithoutDuplicates(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        for (int i = 0; i < 20; i++) {
            deadLetter(store, "s-" + i, "T", 100 + i);
        }
        log.info("【输入】20 条死信，限速 10/s 启动恢复，进行中请求停止，随后再发起新任务续跑");

        try (SettlingDriver driver = new SettlingDriver(store)) {
            String job1 = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 10.0, 2));
            // 等待至少处理掉一部分，确认进度可查
            RecoveryProgress mid = null;
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                mid = svc.progress(job1).orElseThrow();
                if (mid.processed() >= 2) {
                    break;
                }
                Thread.sleep(10);
            }
            assertNotNull(mid);
            assertTrue(mid.processed() >= 2, "停止前应已处理至少 2 条，实际=" + mid.processed());
            assertEquals(RecoveryProgress.Status.RUNNING, mid.status());
            log.info("【状态变化】进行中进度可查: processed={}/{} status=RUNNING", mid.processed(), mid.total());

            svc.stop(job1);
            RecoveryProgress stopped = awaitTerminal(svc, job1, 10_000);
            assertEquals(RecoveryProgress.Status.STOPPED, stopped.status());
            assertTrue(stopped.processed() < 20, "停止后不应处理完全部 20 条，实际=" + stopped.processed());
            int remaining = store.findDeadLetters().size();
            assertEquals(20 - stopped.replayed(), remaining, "未重放的必须保持死信态");
            log.info("【状态变化】任务停止: replayed={} 剩余死信={}", stopped.replayed(), remaining);

            // 停止后再发起新任务续跑：最终全部恢复，且每条只被重放一次
            String job2 = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 100.0, 2));
            RecoveryProgress p2 = awaitTerminal(svc, job2, 10_000);
            assertEquals(remaining, p2.replayed(), "续跑任务只应处理剩余死信");
            assertEquals(0, store.findDeadLetters().size(), "最终所有死信都应被恢复");
            for (int i = 0; i < 20; i++) {
                String id = store.findByIdempotencyKey("s-" + i).orElseThrow().eventId();
                long replays = store.historyOf(id).stream()
                        .filter(r -> r.action() == TransitionAction.REPLAYED).count();
                assertEquals(1, replays, "停止+续跑后每条事件仍只重放一次: s-" + i);
            }
            log.info("【判定依据】停止后剩余={} 与死信数一致；续跑 replayed={}；20 条事件 REPLAYED 历史均=1 => 无重复无副作用",
                    remaining, p2.replayed());
        }
    }

    @Test
    void newEventsAreNotStarvedDuringRecovery(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        for (int i = 0; i < 5; i++) {
            deadLetter(store, "old-" + i, "T", 100 + i);
        }
        log.info("【输入】5 条死信恢复中，同时提交 2 条新事件，验证新事件在有限次领取内被处理");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 500.0, 2));
        store.commit("new-1", "T", "{}", null, System.currentTimeMillis());
        store.commit("new-2", "T", "{}", null, System.currentTimeMillis());

        // 恢复的老事件 + 新事件全部 PENDING，按 nextAttemptAt 公平领取：7 次领取内必须全部领到
        // （领取+置成功同时把恢复事件推到终态，使恢复任务的在途名额得以释放）
        List<String> claimed = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (claimed.size() < 7 && System.currentTimeMillis() < deadline) {
            store.claimNext(System.currentTimeMillis()).ifPresentOrElse(c -> {
                claimed.add(c.event().idempotencyKey());
                store.markSucceeded(c.event().eventId(), c.leaseToken(), null, System.currentTimeMillis());
            }, () -> {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertEquals(7, claimed.size(), "7 条事件(5 恢复+2 新)应在限时内全部领到，存在饿死: " + claimed);
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);
        assertEquals(RecoveryProgress.Status.COMPLETED, done.status());
        assertEquals(5, done.replayed());
        assertTrue(claimed.contains("new-1") && claimed.contains("new-2"),
                "新事件必须在 7 次领取内被处理（不被恢复流量饿死）: " + claimed);
        assertEquals(7, store.findByStatus(EventStatus.SUCCEEDED).size());
        log.info("【状态变化】领取顺序={}", claimed);
        log.info("【判定依据】7 条事件(5 恢复+2 新)连续 7 次领取全部命中且成功 => 新事件不被饿死");
    }

    @Test
    void recoveryResultSurvivesRestartAndResumeHasNoDuplicateSideEffects(@TempDir Path dir) throws Exception {
        DurableWALEventStore before = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(before);
        for (int i = 0; i < 6; i++) {
            deadLetter(before, "r-" + i, "T", 100 + i);
        }
        log.info("【输入】6 条死信，恢复任务跑到一半时模拟进程强杀（停止任务 + 重建存储），重启后续跑");

        int replayedBefore;
        try (SettlingDriver driver = new SettlingDriver(before)) {
            String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 20.0, 1));
            // 等处理一部分后停止任务（模拟崩溃前只有部分重放落盘）
            waitFor(() -> svc.progress(jobId).orElseThrow().replayed() >= 2, 5_000, "崩溃前已重放>=2");
            svc.stop(jobId);
            RecoveryProgress stopped = awaitTerminal(svc, jobId, 10_000);
            before.flush();
            replayedBefore = stopped.replayed();
            log.info("【状态变化】崩溃前已重放={} 剩余死信={}", replayedBefore, 6 - replayedBefore);
        }

        // 重启：已重放的保持 PENDING/SUCCEEDED，未重放的保持死信，历史完整
        DurableWALEventStore after = newStore(dir);
        int pendingOrSucceeded = after.findByStatus(EventStatus.PENDING).size()
                + after.findByStatus(EventStatus.SUCCEEDED).size();
        assertEquals(replayedBefore, pendingOrSucceeded, "重启后已重放事件必须保持非死信态");
        assertEquals(6 - replayedBefore, after.findDeadLetters().size());
        for (int i = 0; i < 6; i++) {
            String id = after.findByIdempotencyKey("r-" + i).orElseThrow().eventId();
            List<TransitionRecord> timeline = after.historyOf(id);
            TransitionRecord last = timeline.get(timeline.size() - 1);
            EventStatus status = after.findByEventId(id).orElseThrow().status();
            assertEquals(last.toStatus(), status,
                    "重启后历史末笔必须与状态一致: r-" + i + " lastAction=" + last.action());
        }
        // 重启后续跑：剩余死信被恢复，最终一致，且每条事件只被重放一次（无重复副作用）
        DefaultDeadLetterRecoveryService svc2 = new DefaultDeadLetterRecoveryService(after);
        try (SettlingDriver driver2 = new SettlingDriver(after)) {
            String job2 = svc2.startRecovery(new RecoveryRequest(null, "T", null, null, 100.0, 2));
            RecoveryProgress p2 = awaitTerminal(svc2, job2, 10_000);
            assertEquals(6 - replayedBefore, p2.replayed());
            assertEquals(0, after.findDeadLetters().size());
            waitFor(() -> after.findByStatus(EventStatus.SUCCEEDED).size() == 6, 5_000, "6 条全部到达终态");
            for (int i = 0; i < 6; i++) {
                String id = after.findByIdempotencyKey("r-" + i).orElseThrow().eventId();
                long replays = after.historyOf(id).stream()
                        .filter(r -> r.action() == TransitionAction.REPLAYED).count();
                assertEquals(1, replays, "停止+重启+续跑后每条事件仍只重放一次: r-" + i);
            }
            log.info("【判定依据】重启后 非死信={}/死信={} 与崩溃前一致；续跑 replayed={} 后 6 条全部 SUCCEEDED，"
                    + "每条 REPLAYED 历史=1 => 无半截状态、无重复副作用", replayedBefore, 6 - replayedBefore, p2.replayed());
        }
    }
}

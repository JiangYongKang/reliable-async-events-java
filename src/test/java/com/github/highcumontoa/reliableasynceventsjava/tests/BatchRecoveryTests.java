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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量死信恢复：不给条件的全量恢复、零命中如实上报、条件筛选/显式列表、限速与
 * 并发上限（管住“已恢复未到终态”的在途事件数）、进度可查、中途停止、幂等重复提交、
 * 停止后续跑、新事件不被饿死、停止+重启续跑无重复副作用。
 *
 * <p>并发上限的语义是“已重放但未达终态的事件占满额度就暂停注入”，因此本类测试
 * 一律配套回收线程（{@link Drainer}，模拟分发器把事件推进到终态）或真实
 * {@link EventDispatcher}，与生产环境一致。</p>
 */
class BatchRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(BatchRecoveryTests.class);

    private DurableWALEventStore newStore(Path dir) {
        return newStore(dir, 30_000);
    }

    private DurableWALEventStore newStore(Path dir, long leaseMillis) {
        return new DurableWALEventStore(new DurableFileLog(dir.toString()),
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), leaseMillis);
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

    /** 模拟分发器的回收线程：持续领取到期事件并标记成功，把恢复事件推进到终态、释放并发额度。 */
    private static final class Drainer {
        private final DurableWALEventStore store;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<String> claimedOrder = new CopyOnWriteArrayList<>();
        private final Thread thread;

        Drainer(DurableWALEventStore store) {
            this.store = store;
            this.thread = new Thread(this::loop, "test-drainer");
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        private void loop() {
            while (running.get()) {
                try {
                    Optional<EventStore.ClaimedEvent> c = store.claimNext(System.currentTimeMillis());
                    if (c.isEmpty()) {
                        Thread.sleep(5);
                        continue;
                    }
                    claimedOrder.add(c.get().event().idempotencyKey());
                    store.markSucceeded(c.get().event().eventId(), c.get().leaseToken(),
                            null, System.currentTimeMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        void stop() throws InterruptedException {
            running.set(false);
            thread.join(2000);
        }
    }

    private RecoveryProgress awaitTerminal(DeadLetterRecoveryService svc, String jobId, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            RecoveryProgress p = svc.progress(jobId).orElseThrow();
            if (p.status() == RecoveryProgress.Status.COMPLETED
                    || p.status() == RecoveryProgress.Status.COMPLETED_EMPTY
                    || p.status() == RecoveryProgress.Status.STOPPED
                    || p.status() == RecoveryProgress.Status.FAILED) {
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
    void recoversBatchByFilterWithRateLimitAndProgress(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        List<String> typeA = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            typeA.add(deadLetter(store, "a-" + i, "TYPE_A", 100 + i));
        }
        for (int i = 0; i < 3; i++) {
            deadLetter(store, "b-" + i, "TYPE_B", 200 + i);
        }
        assertEquals(8, store.findDeadLetters().size());
        drainer.start();
        log.info("【输入】8 条死信(TYPE_A=5, TYPE_B=3)，按 type=TYPE_A 筛选恢复，限速 20/s 并发 2");

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
            assertEquals(EventStatus.SUCCEEDED, ev.status(), "恢复后事件被回收线程推进到终态: " + id);
            List<TransitionRecord> timeline = store.historyOf(id);
            TransitionRecord replay = timeline.stream()
                    .filter(r -> r.action() == TransitionAction.REPLAYED).findFirst().orElseThrow();
            assertEquals("batch-recovery:" + jobId, replay.trigger(), "历史必须记录批量恢复来源");
        }
        drainer.stop();
        log.info("【状态变化】进度 total=5 replayed=5 skipped=0 failed=0, 耗时={}ms, TYPE_B 仍为死信=3", elapsed);
        log.info("【判定依据】筛选只命中 TYPE_A；耗时 >=150ms 证明限速生效；每条事件历史含 trigger=batch-recovery:jobId 的 REPLAYED");
    }

    @Test
    void noCriteriaRecoversAllCurrentDeadLetters(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        List<String> all = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            all.add(deadLetter(store, "a-" + i, "TYPE_A", 100 + i));
        }
        for (int i = 0; i < 2; i++) {
            all.add(deadLetter(store, "b-" + i, "TYPE_B", 200 + i));
        }
        all.add(deadLetter(store, "c-0", "TYPE_C", 300));
        assertEquals(6, store.findDeadLetters().size());
        drainer.start();
        log.info("【输入】6 条死信(TYPE_A=3, TYPE_B=2, TYPE_C=1)，恢复调用不给任何条件"
                + "（eventIds/eventType/reason/before 全空），期望全量恢复");

        String jobId = svc.startRecovery(new RecoveryRequest(null, null, null, null, 200.0, 2));
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);

        assertEquals(RecoveryProgress.Status.COMPLETED, done.status(),
                "不给任何条件必须真的恢复全部死信，而不是空跑报完成");
        assertEquals(6, done.total());
        assertEquals(6, done.replayed());
        assertEquals(0, done.failed());
        assertEquals(0, store.findDeadLetters().size(), "所有死信都应被恢复");
        for (String id : all) {
            assertEquals(EventStatus.SUCCEEDED, store.findByEventId(id).orElseThrow().status());
            assertTrue(store.historyOf(id).stream().anyMatch(r -> r.action() == TransitionAction.REPLAYED
                    && r.trigger().equals("batch-recovery:" + jobId)), "每条事件都应有本任务的 REPLAYED 历史: " + id);
        }
        drainer.stop();
        log.info("【状态变化】total=6 replayed=6, 死信 6->0, 6 条事件全部 SUCCEEDED");
        log.info("【判定依据】未指定任何条件 = 全量恢复当前所有死信（启动时快照），不再空跑");
    }

    @Test
    void filterMatchingNothingReportsEmptyInsteadOfSuccess(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        for (int i = 0; i < 3; i++) {
            deadLetter(store, "a-" + i, "TYPE_A", 100 + i);
        }
        drainer.start();
        log.info("【输入】3 条 TYPE_A 死信，按 type=NO_SUCH_TYPE 筛选恢复（一条都命中不了）");

        String emptyJob = svc.startRecovery(new RecoveryRequest(null, "NO_SUCH_TYPE", null, null, 100.0, 2));
        RecoveryProgress empty = awaitTerminal(svc, emptyJob, 10_000);

        assertEquals(RecoveryProgress.Status.COMPLETED_EMPTY, empty.status(),
                "零命中必须如实上报 COMPLETED_EMPTY，不能与恢复成功混淆");
        assertEquals(0, empty.total());
        assertEquals(0, empty.replayed());
        assertEquals(0, empty.skipped());
        assertEquals(0, empty.failed());
        assertEquals(0, empty.inFlight());
        assertNotNull(empty.finishedAtMillis());
        assertEquals(3, store.findDeadLetters().size(), "零命中任务不得动任何死信");
        log.info("【状态变化】零命中任务: status=COMPLETED_EMPTY total=0 replayed=0, 死信仍为 3");

        // 对照组：真实命中 3 条的恢复任务 -> COMPLETED 且 replayed=3，两种结果明确可区分
        String realJob = svc.startRecovery(new RecoveryRequest(null, "TYPE_A", null, null, 100.0, 2));
        RecoveryProgress real = awaitTerminal(svc, realJob, 10_000);
        assertEquals(RecoveryProgress.Status.COMPLETED, real.status());
        assertEquals(3, real.replayed());
        assertEquals(0, store.findDeadLetters().size());
        assertNotEquals(empty.status(), real.status(), "零命中与真实恢复成功的状态必须可区分");
        drainer.stop();
        log.info("【状态变化】对照任务: status=COMPLETED total=3 replayed=3, 死信 3->0");
        log.info("【判定依据】COMPLETED_EMPTY(无目标,恢复 0 条) != COMPLETED(有目标且全部处理)，线上不会误判恢复已生效");
    }

    @Test
    void explicitIdsAndDuplicateSubmissionAreIdempotent(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ids.add(deadLetter(store, "e-" + i, "T", 100 + i));
        }
        drainer.start();
        log.info("【输入】4 条死信，按显式 ID 列表恢复；随后用同一批 ID 重复提交第二次恢复");

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
        drainer.stop();
        log.info("【状态变化】第一次 replayed=4；第二次 replayed=0 skipped=4；每条事件 REPLAYED 历史=1");
        log.info("【判定依据】replay 仅接受 DEAD_LETTER，重复提交命中幂等跳过分支 => 无重复副作用");
    }

    @Test
    void stopHaltsRecoveryAndLaterJobResumesWithoutDuplicates(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        for (int i = 0; i < 20; i++) {
            deadLetter(store, "s-" + i, "T", 100 + i);
        }
        drainer.start();
        log.info("【输入】20 条死信，限速 10/s 启动恢复，进行中请求停止，随后再发起新任务续跑");

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
        drainer.stop();
        log.info("【判定依据】停止后剩余={} 与死信数一致；续跑 replayed={}；20 条事件 REPLAYED 历史均=1 => 无重复无副作用",
                remaining, p2.replayed());
    }

    @Test
    void newEventsAreNotStarvedDuringRecovery(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        Drainer drainer = new Drainer(store);
        for (int i = 0; i < 5; i++) {
            deadLetter(store, "old-" + i, "T", 100 + i);
        }
        drainer.start();
        log.info("【输入】5 条死信恢复中，同时提交 2 条新事件，验证新事件与恢复事件一样被公平领取处理");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 500.0, 2));
        store.commit("new-1", "T", "{}", null, System.currentTimeMillis());
        store.commit("new-2", "T", "{}", null, System.currentTimeMillis());
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);
        assertEquals(5, done.replayed());

        // 恢复的老事件 + 新事件按 nextAttemptAt 公平领取：7 条全部在有限时间内被回收线程处理
        waitFor(() -> drainer.claimedOrder.size() == 7, 10_000, "7 条事件(5 恢复+2 新)应全部被领取");
        assertTrue(drainer.claimedOrder.contains("new-1") && drainer.claimedOrder.contains("new-2"),
                "新事件必须被处理（不被恢复流量饿死）: " + drainer.claimedOrder);
        waitFor(() -> store.findByStatus(EventStatus.SUCCEEDED).size() == 7, 10_000, "7 条事件应全部成功");
        drainer.stop();
        log.info("【状态变化】领取顺序={}", drainer.claimedOrder);
        log.info("【判定依据】7 条事件(5 恢复+2 新)全部被领取且成功 => 新事件不被饿死");
    }

    @Test
    void recoveryResultSurvivesRestart(@TempDir Path dir) throws Exception {
        DurableWALEventStore before = newStore(dir, 500);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(before);
        Drainer drainer = new Drainer(before);
        for (int i = 0; i < 6; i++) {
            deadLetter(before, "r-" + i, "T", 100 + i);
        }
        drainer.start();
        log.info("【输入】6 条死信，恢复任务跑到一半时模拟进程强杀（停止任务 + 重建存储）");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 20.0, 2));
        // 等处理一部分后停止任务（模拟崩溃前只有部分重放落盘）
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            RecoveryProgress p = svc.progress(jobId).orElseThrow();
            if (p.replayed() >= 2) {
                break;
            }
            Thread.sleep(10);
        }
        svc.stop(jobId);
        RecoveryProgress stopped = awaitTerminal(svc, jobId, 10_000);
        drainer.stop();
        before.flush();
        int replayedBefore = stopped.replayed();
        log.info("【状态变化】崩溃前已重放={} 剩余死信={}", replayedBefore, 6 - replayedBefore);

        // 重启：已重放的保持 PENDING/SUCCEEDED，未重放的保持死信，历史完整
        DurableWALEventStore after = newStore(dir, 500);
        int pendingAfter = after.findByStatus(EventStatus.PENDING).size();
        int succeededAfter = after.findByStatus(EventStatus.SUCCEEDED).size();
        assertEquals(replayedBefore, pendingAfter + succeededAfter,
                "已重放的事件（在途 PENDING + 已终态 SUCCEEDED）总数必须与崩溃前一致");
        assertEquals(6 - replayedBefore, after.findDeadLetters().size());
        for (int i = 0; i < 6; i++) {
            String id = after.findByIdempotencyKey("r-" + i).orElseThrow().eventId();
            List<TransitionRecord> timeline = after.historyOf(id);
            TransitionRecord last = timeline.get(timeline.size() - 1);
            EventStatus status = after.findByEventId(id).orElseThrow().status();
            assertEquals(last.toStatus(), status,
                    "重启后历史末笔必须与状态一致: r-" + i + " lastAction=" + last.action());
        }
        // 重启后续跑：剩余死信被恢复，最终一致
        DefaultDeadLetterRecoveryService svc2 = new DefaultDeadLetterRecoveryService(after);
        Drainer drainer2 = new Drainer(after);
        drainer2.start();
        String job2 = svc2.startRecovery(new RecoveryRequest(null, "T", null, null, 100.0, 2));
        RecoveryProgress p2 = awaitTerminal(svc2, job2, 10_000);
        assertEquals(6 - replayedBefore, p2.replayed());
        assertEquals(0, after.findDeadLetters().size());
        waitFor(() -> after.findByStatus(EventStatus.SUCCEEDED).size() == 6, 10_000,
                "6 条事件最终都应成功");
        drainer2.stop();
        log.info("【判定依据】重启后 在途+成功={} 死信={} 与崩溃前一致；续跑 replayed={} 后 6 条全部成功 => 无半截状态",
                replayedBefore, 6 - replayedBefore, p2.replayed());
    }

    /** 阻塞式处理器：释放前一直占着 worker，用于观察并发上限是否真的管住在途恢复事件。 */
    private static final class BlockingHandler implements EventHandler {
        private final String type;
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger inHandler = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();

        BlockingHandler(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Map<String, String> handle(EventRecord event) {
            int now = inHandler.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            log.info("  [阻塞处理器] 进入 eventId={} 当前在处理={} 峰值={}", event.eventId(), now, maxConcurrent.get());
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inHandler.decrementAndGet();
            }
            return Map.of("effect:" + event.eventId(), "done");
        }

        void release() {
            release.countDown();
        }
    }

    @Test
    void concurrencyCapsRecoveredEventsInFlight(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        HandlerRegistry registry = new HandlerRegistry();
        BlockingHandler handler = new BlockingHandler("GATED");
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry,
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), 10, 4);
        for (int i = 0; i < 6; i++) {
            deadLetter(store, "g-" + i, "GATED", 100 + i);
        }
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        dispatcher.start();
        log.info("【输入】6 条死信，处理器阻塞不释放，限速 1000/s（足够快）并发 2："
                + "验证并发上限管住“已恢复未到终态”的在途事件数，而不是只约束重放动作本身");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "GATED", null, null, 1000.0, 2));
        // 等 2 条进入处理器（在途=2，额度占满）
        waitFor(() -> handler.inHandler.get() == 2, 10_000, "2 条恢复事件应进入处理器");
        RecoveryProgress mid = svc.progress(jobId).orElseThrow();
        assertEquals(2, mid.inFlight(), "在途(已恢复未到终态)应等于并发上限 2");
        // 额度占满期间：即使速率足够，也不再注入新的重放
        Thread.sleep(400);
        RecoveryProgress held = svc.progress(jobId).orElseThrow();
        assertEquals(2, held.replayed(), "2 条在途未达终态时不得继续重放，实际=" + held.replayed());
        assertEquals(4, store.findDeadLetters().size(), "其余 4 条必须保持死信等待额度");
        assertEquals(RecoveryProgress.Status.RUNNING, held.status());
        log.info("【状态变化】额度占满: replayed=2 inFlight=2 剩余死信=4，注入暂停（速率 1000/s 也推不动）");

        handler.release();
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);
        assertEquals(RecoveryProgress.Status.COMPLETED, done.status());
        assertEquals(6, done.replayed());
        assertEquals(0, done.inFlight(), "全部到达终态后在途应归零");
        waitFor(() -> store.findByStatus(EventStatus.SUCCEEDED).size() == 6, 10_000, "6 条应全部成功");
        assertTrue(handler.maxConcurrent.get() <= 2,
                "处理器观测到的并发峰值不得超过并发上限 2，实际=" + handler.maxConcurrent.get());
        dispatcher.stop();
        log.info("【状态变化】释放后 6 条全部 SUCCEEDED，处理器观测并发峰值={}", handler.maxConcurrent.get());
        log.info("【判定依据】在途=2 时注入暂停（replayed 停在 2、死信保持 4）；峰值并发<=2 => "
                + "concurrency 真正管住处理链路上流转的恢复事件数");
    }

    @Test
    void stopRestartResumeProducesNoDuplicateSideEffects(@TempDir Path dir) throws Exception {
        DurableWALEventStore before = newStore(dir, 1000);
        HandlerRegistry registry = new HandlerRegistry();
        ControllableHandler handler = new ControllableHandler("RESUME", ControllableHandler.Mode.OK, 0);
        registry.register(handler);
        EventDispatcher dispatcher1 = new EventDispatcher(before, registry,
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), 10, 2);
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ids.add(deadLetter(before, "rs-" + i, "RESUME", 100 + i));
        }
        DefaultDeadLetterRecoveryService svc1 = new DefaultDeadLetterRecoveryService(before);
        dispatcher1.start();
        log.info("【输入】6 条死信，恢复跑到一半停止任务并模拟进程重启，随后续跑；"
                + "验证每条事件处理恰好一次、业务副作用不重复");

        String job1 = svc1.startRecovery(new RecoveryRequest(null, null, null, null, 50.0, 2));
        waitFor(() -> svc1.progress(job1).orElseThrow().replayed() >= 2, 10_000, "崩溃前应已重放至少 2 条");
        svc1.stop(job1);
        RecoveryProgress stopped = awaitTerminal(svc1, job1, 10_000);
        assertEquals(RecoveryProgress.Status.STOPPED, stopped.status());
        dispatcher1.stop();
        before.flush();
        int replayedBefore = stopped.replayed();
        log.info("【状态变化】停止+重启: 崩溃前已重放={} 剩余死信={}", replayedBefore, 6 - replayedBefore);

        // 重启：新存储 + 新分发器 + 新恢复服务（处理器沿用同一实例，跨重启统计处理次数）
        DurableWALEventStore after = newStore(dir, 1000);
        DefaultDeadLetterRecoveryService svc2 = new DefaultDeadLetterRecoveryService(after);
        EventDispatcher dispatcher2 = new EventDispatcher(after, registry,
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), 10, 2);
        dispatcher2.start();
        String job2 = svc2.startRecovery(new RecoveryRequest(null, null, null, null, 100.0, 2));
        RecoveryProgress p2 = awaitTerminal(svc2, job2, 10_000);
        assertEquals(RecoveryProgress.Status.COMPLETED, p2.status());
        assertEquals(6 - replayedBefore, p2.replayed(), "续跑只应恢复剩余死信");
        waitFor(() -> after.findByStatus(EventStatus.SUCCEEDED).size() == 6, 10_000,
                "6 条事件最终都应成功");
        assertEquals(0, after.findDeadLetters().size());
        for (String id : ids) {
            long replays = after.historyOf(id).stream()
                    .filter(r -> r.action() == TransitionAction.REPLAYED).count();
            assertEquals(1, replays, "每条事件只能被重放一次: " + id);
            assertEquals(1, handler.perEventInvocations.getOrDefault(id, 0),
                    "每条事件只能被处理一次（副作用不重复）: " + id);
            assertTrue(after.get("effect:" + id).isPresent(), "成功副作用必须落盘: " + id);
        }
        dispatcher2.stop();
        log.info("【状态变化】续跑 replayed={}，6 条全部 SUCCEEDED，每条处理次数=1、REPLAYED 历史=1", p2.replayed());
        log.info("【判定依据】停止+重启续跑后：处理次数与重放历史均恰好一次、副作用 KV 各一条 => 无重复副作用");
    }
}

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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批量死信恢复：条件筛选/显式列表、限速与并发、进度可查、中途停止、
 * 幂等重复提交、停止后续跑、新事件不被饿死、重启后状态一致。
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

    private RecoveryProgress awaitTerminal(DeadLetterRecoveryService svc, String jobId, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            RecoveryProgress p = svc.progress(jobId).orElseThrow();
            if (p.status() == RecoveryProgress.Status.COMPLETED
                    || p.status() == RecoveryProgress.Status.STOPPED
                    || p.status() == RecoveryProgress.Status.FAILED) {
                return p;
            }
            Thread.sleep(10);
        }
        fail("任务未在 " + timeoutMillis + "ms 内到达终态: " + svc.progress(jobId).orElseThrow());
        return null;
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
            assertEquals(EventStatus.PENDING, ev.status());
            List<TransitionRecord> timeline = store.historyOf(id);
            TransitionRecord last = timeline.get(timeline.size() - 1);
            assertEquals(TransitionAction.REPLAYED, last.action());
            assertEquals("batch-recovery:" + jobId, last.trigger(), "历史必须记录批量恢复来源");
        }
        log.info("【状态变化】进度 total=5 replayed=5 skipped=0 failed=0, 耗时={}ms, TYPE_B 仍为死信=3", elapsed);
        log.info("【判定依据】筛选只命中 TYPE_A；耗时 >=150ms 证明限速生效；每条事件末笔历史 trigger=batch-recovery:jobId");
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

    @Test
    void stopHaltsRecoveryAndLaterJobResumesWithoutDuplicates(@TempDir Path dir) throws Exception {
        DurableWALEventStore store = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(store);
        for (int i = 0; i < 20; i++) {
            deadLetter(store, "s-" + i, "T", 100 + i);
        }
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
        log.info("【判定依据】停止后剩余={} 与死信数一致；续跑 replayed={}；20 条事件 REPLAYED 历史均=1 => 无重复无副作用",
                remaining, p2.replayed());
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
        RecoveryProgress done = awaitTerminal(svc, jobId, 10_000);
        assertEquals(5, done.replayed());

        // 恢复的老事件 + 新事件全部 PENDING，按 nextAttemptAt 公平领取：7 次领取内必须全部领到
        List<String> claimed = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            final int round = i;
            EventStore.ClaimedEvent c = store.claimNext(System.currentTimeMillis() + 1000 + i)
                    .orElseThrow(() -> new AssertionError("第 " + round + " 次领取为空，存在饿死"));
            claimed.add(c.event().idempotencyKey());
            store.markSucceeded(c.event().eventId(), c.leaseToken(), null,
                    System.currentTimeMillis() + 2000 + i);
        }
        assertTrue(claimed.contains("new-1") && claimed.contains("new-2"),
                "新事件必须在 7 次领取内被处理（不被恢复流量饿死）: " + claimed);
        assertEquals(7, store.findByStatus(EventStatus.SUCCEEDED).size());
        log.info("【状态变化】领取顺序={}", claimed);
        log.info("【判定依据】7 条事件(5 恢复+2 新)连续 7 次领取全部命中且成功 => 新事件不被饿死");
    }

    @Test
    void recoveryResultSurvivesRestart(@TempDir Path dir) throws Exception {
        DurableWALEventStore before = newStore(dir);
        DefaultDeadLetterRecoveryService svc = new DefaultDeadLetterRecoveryService(before);
        for (int i = 0; i < 6; i++) {
            deadLetter(before, "r-" + i, "T", 100 + i);
        }
        log.info("【输入】6 条死信，恢复任务跑到一半时模拟进程强杀（停止任务 + 重建存储）");

        String jobId = svc.startRecovery(new RecoveryRequest(null, "T", null, null, 20.0, 1));
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
        before.flush();
        int replayedBefore = stopped.replayed();
        log.info("【状态变化】崩溃前已重放={} 剩余死信={}", replayedBefore, 6 - replayedBefore);

        // 重启：已重放的保持 PENDING，未重放的保持死信，历史完整
        DurableWALEventStore after = newStore(dir);
        assertEquals(replayedBefore, after.findByStatus(EventStatus.PENDING).size());
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
        String job2 = svc2.startRecovery(new RecoveryRequest(null, "T", null, null, 100.0, 2));
        RecoveryProgress p2 = awaitTerminal(svc2, job2, 10_000);
        assertEquals(6 - replayedBefore, p2.replayed());
        assertEquals(0, after.findDeadLetters().size());
        assertEquals(6, after.findByStatus(EventStatus.PENDING).size());
        log.info("【判定依据】重启后 PENDING={}/死信={} 与崩溃前一致；续跑 replayed={} 后 6 条全部 PENDING => 无半截状态",
                replayedBefore, 6 - replayedBefore, p2.replayed());
    }
}

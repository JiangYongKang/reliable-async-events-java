package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事件存储：幂等提交、领取租约、状态流转、死信原因区分与重启恢复。
 */
class EventStoreStateMachineTests {

    private static final Logger log = LoggerFactory.getLogger(EventStoreStateMachineTests.class);

    private ExponentialBackoffRetryPolicy retryPolicy() {
        return new ExponentialBackoffRetryPolicy(3, 100, 1000, 0);
    }

    @Test
    void idempotentCommitDoesNotCreateSecondEvent(@TempDir Path dir) {
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        long t0 = 1_000;
        SubmissionResult first = store.commit("order-1", "ORDER_PAID", "{\"amount\":100}",
                Map.of("order:1", "PAID"), t0);
        SubmissionResult second = store.commit("order-1", "ORDER_PAID", "{\"amount\":999}",
                Map.of("order:1", "OVERWRITTEN"), t0 + 10);

        assertFalse(first.duplicate());
        assertTrue(second.duplicate(), "同一幂等键重复提交必须命中既有事件");
        assertEquals(first.event().eventId(), second.event().eventId());
        assertEquals("{\"amount\":100}", second.event().payload(), "重复提交不得覆盖既有载荷");
        assertEquals("PAID", store.get("order:1").orElseThrow(), "重复提交不得重复/覆盖业务副作用");
        assertEquals(1, store.findByStatus(EventStatus.PENDING).size());
        log.info("【输入】同一幂等键 order-1 提交两次（第二次 payload/副作用不同）");
        log.info("【状态变化】事件数=1，eventId 相同；业务键 order:1 保持 PAID。判定依据：commit 在锁内先查 idempotencyIndex");
    }

    @Test
    void claimLeaseAndSuccessAreConditional(@TempDir Path dir) {
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        store.commit("k1", "T", "{}", null, 0);

        Optional<EventStore.ClaimedEvent> c1 = store.claimNext(0);
        Optional<EventStore.ClaimedEvent> c2 = store.claimNext(0);
        assertTrue(c1.isPresent());
        assertTrue(c2.isEmpty(), "已被租约持有的事件不能被第二次领取");
        log.info("【输入】领取 k1 后立即再次领取；【判定依据】leaseToken!=0 且租约未到期 => 第二次为空");

        EventRecord ev = c1.get().event();
        long lease = c1.get().leaseToken();
        assertEquals(1, ev.attemptCount());

        assertThrows(OptimisticLockingFailureException.class,
                () -> store.markSucceeded(ev.eventId(), lease + 999, null, 10),
                "错误租约的成功提交必须被拒绝");
        store.markSucceeded(ev.eventId(), lease, Map.of("result:1", "done"), 10);
        assertEquals(EventStatus.SUCCEEDED, store.findByEventId(ev.eventId()).orElseThrow().status());
        assertEquals("done", store.get("result:1").orElseThrow());
        assertTrue(store.claimNext(1000).isEmpty(), "终态事件不再被领取");
        log.info("【状态变化】错误租约抛 OptimisticLockingFailure；正确租约 -> SUCCEEDED 且副作用可见");
    }

    @Test
    void retriesBackoffThenDeadLetterWithDistinctReasons(@TempDir Path dir) {
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        store.commit("k-retry", "T", "{}", null, 0);

        int attempt = 0;
        // 前两次瞬时故障进入 RETRYING，第三次耗尽 -> DEAD_LETTER(RETRIES_EXHAUSTED)
        for (int round = 0; round < 3; round++) {
            long now = round * 1000;
            EventStore.ClaimedEvent c = store.claimNext(now).orElseThrow();
            attempt = c.event().attemptCount();
            store.scheduleRetry(c.event().eventId(), c.leaseToken(), "connection reset",
                    now + 100, now);
        }
        EventRecord dead = store.findByStatus(EventStatus.DEAD_LETTER).get(0);
        assertEquals(DeadLetterReason.RETRIES_EXHAUSTED, dead.deadLetterReason());
        assertEquals(3, attempt);
        assertTrue(dead.lastError().contains("connection reset"));
        log.info("【输入】连续 3 次瞬时故障(maxAttempts=3)");
        log.info("【状态变化】PENDING->RETRYING x2 -> DEAD_LETTER；原因={} attempt={}", dead.deadLetterReason(), attempt);

        // 不可重试 / 校验失败两种原因可区分
        store.commit("k-perm", "T", "{}", null, 0);
        EventStore.ClaimedEvent perm = store.claimNext(5000).orElseThrow();
        store.markDeadLetter(perm.event().eventId(), perm.leaseToken(),
                DeadLetterReason.NON_RETRYABLE, "账户已注销", 5000);

        var deadLetters = store.findDeadLetters();
        assertEquals(2, deadLetters.size());
        AtomicInteger permCount = new AtomicInteger();
        deadLetters.forEach(d -> {
            if (d.deadLetterReason() == DeadLetterReason.NON_RETRYABLE) {
                permCount.incrementAndGet();
                assertTrue(d.lastError().contains("账户已注销"));
            }
        });
        assertEquals(1, permCount.get(), "RETRIES_EXHAUSTED 与 NON_RETRYABLE 必须可区分");
        log.info("【判定依据】死信列表中两种 reason 各自独立携带错误信息，不静默丢弃");
    }

    @Test
    void replayMovesDeadLetterBackToPending(@TempDir Path dir) {
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        store.commit("k-dl", "T", "{}", null, 0);
        EventStore.ClaimedEvent c = store.claimNext(0).orElseThrow();
        store.markDeadLetter(c.event().eventId(), c.leaseToken(),
                DeadLetterReason.VALIDATION_FAILED, "缺字段 x", 0);

        EventRecord replayed = store.replay(c.event().eventId(), 1000);
        assertEquals(EventStatus.PENDING, replayed.status());
        assertEquals(0, replayed.attemptCount(), "重放应重置尝试计数");
        assertNull(replayed.deadLetterReason());
        EventStore.ClaimedEvent again = store.claimNext(1000).orElseThrow();
        store.markSucceeded(again.event().eventId(), again.leaseToken(),
                Map.of("replay:1", "ok"), 1000);
        assertEquals(EventStatus.SUCCEEDED,
                store.findByEventId(c.event().eventId()).orElseThrow().status());
        log.info("【输入】VALIDATION_FAILED 死信修复后 replay；【状态变化】DEAD_LETTER->PENDING(重置次数)->SUCCEEDED");
    }

    @Test
    void stateSurvivesRestart(@TempDir Path dir) {
        DurableWALEventStore store1 = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        store1.commit("persist-1", "T", "{\"v\":1}", Map.of("b:1", "x"), 0);
        EventStore.ClaimedEvent c = store1.claimNext(0).orElseThrow();
        store1.markSucceeded(c.event().eventId(), c.leaseToken(), Map.of("b:2", "y"), 5);
        store1.commit("persist-2", "T", "{}", null, 0);
        store1.flush();
        log.info("【输入】重启前：1 条 SUCCEEDED + 1 条 PENDING + 2 个业务键，全部来自 WAL 重放");

        DurableWALEventStore store2 = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), 30_000);
        assertEquals(1, store2.findByStatus(EventStatus.SUCCEEDED).size());
        assertEquals(1, store2.findByStatus(EventStatus.PENDING).size());
        assertEquals("x", store2.get("b:1").orElseThrow());
        assertEquals("y", store2.get("b:2").orElseThrow());
        assertTrue(store2.findByIdempotencyKey("persist-1").isPresent());
        assertTrue(store2.findByIdempotencyKey("persist-2").isPresent());
        log.info("【判定依据】重启后幂等索引、状态机、业务 KV 全部从 WAL 恢复，事件不丢失");
    }

    @Test
    void staleLeaseIsReclaimedAfterTimeout(@TempDir Path dir) {
        long lease = 1000;
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()),
                retryPolicy(), lease);
        store.commit("k-stuck", "T", "{}", null, 0);
        EventStore.ClaimedEvent c1 = store.claimNext(0).orElseThrow();
        // 模拟消费者崩溃：租约超时后其它消费者可重新领取
        EventStore.ClaimedEvent c2 = store.claimNext(lease + 1).orElseThrow();
        assertNotEquals(c1.leaseToken(), c2.leaseToken());
        assertEquals(2, c2.event().attemptCount());
        // 旧租约迟到的成功提交必须失败，避免重复副作用
        assertThrows(OptimisticLockingFailureException.class,
                () -> store.markSucceeded(c1.event().eventId(), c1.leaseToken(), null, lease + 2));
        store.markSucceeded(c2.event().eventId(), c2.leaseToken(), Map.of("once", "1"), lease + 2);
        assertEquals("1", store.get("once").orElseThrow());
        log.info("【输入】租约超时 1ms 后重新领取；【判定依据】旧 token 提交被拒、新 token 成功，副作用恰好一次");
    }
}

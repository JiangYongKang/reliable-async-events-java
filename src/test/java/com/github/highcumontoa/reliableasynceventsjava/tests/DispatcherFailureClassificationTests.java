package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败分类与状态流转：瞬时故障自动重试恢复、永久故障/校验失败进入可区分死信、死信重放成功。
 *
 * <p>采用确定性驱动：领取与退避到期均由测试显式控制时钟，不依赖真实等待，
 * 因此本地一条命令即可稳定复现。</p>
 */
class DispatcherFailureClassificationTests {

    private static final Logger log = LoggerFactory.getLogger(DispatcherFailureClassificationTests.class);

    private record Harness(DurableWALEventStore store, HandlerRegistry registry,
                           EventDispatcher dispatcher, ExponentialBackoffRetryPolicy policy) {
    }

    private Harness harness(Path dir, ControllableHandler handler, int maxAttempts) {
        ExponentialBackoffRetryPolicy policy =
                new ExponentialBackoffRetryPolicy(maxAttempts, 100, 1000, 0, new java.util.Random(42));
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, 30_000);
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry, policy, 50, 2);
        return new Harness(store, registry, dispatcher, policy);
    }


    @Test
    void transientFailureRetriesAndEventuallySucceeds(@TempDir Path dir) {
        ControllableHandler handler = new ControllableHandler("FLAKY",
                ControllableHandler.Mode.TRANSIENT_N_TIMES, 2);
        Harness h = harness(dir, handler, 5);

        SubmissionResult r = h.store().commit("biz-flaky", "FLAKY", "{\"order\":\"A1\"}", null, 0);
        String id = r.event().eventId();
        log.info("【输入】提交瞬时故障事件 biz-flaky，处理器前 2 次抛连接超时，第 3 次成功");

        // 第 1 次：瞬时故障 -> RETRYING
        h.dispatcher().process(h.store().claimNext(0).orElseThrow());
        EventRecord after1 = h.store().findByEventId(id).orElseThrow();
        assertEquals(EventStatus.RETRYING, after1.status());
        assertTrue(after1.nextAttemptAtMillis() >= 100, "应按退避策略安排下次尝试");
        assertTrue(after1.lastError().contains("connection timeout"));
        log.info("【状态变化】第1次后 status=RETRYING nextAttemptAt={} 依据=非声明型异常=>瞬时", after1.nextAttemptAtMillis());

        // 第 2 次：在退避窗口内不可被领取；到期后再次失败
        assertTrue(h.store().claimNext(after1.nextAttemptAtMillis() - 1).isEmpty(), "退避窗口未到不得领取");
        h.dispatcher().process(h.store().claimNext(after1.nextAttemptAtMillis()).orElseThrow());
        EventRecord after2 = h.store().findByEventId(id).orElseThrow();
        assertEquals(EventStatus.RETRYING, after2.status());
        log.info("【状态变化】窗口内领取为空；到期后第2次仍失败 status=RETRYING nextAttemptAt={}", after2.nextAttemptAtMillis());

        // 第 3 次：恢复成功
        h.dispatcher().process(h.store().claimNext(after2.nextAttemptAtMillis()).orElseThrow());
        EventRecord done = h.store().findByEventId(id).orElseThrow();
        assertEquals(EventStatus.SUCCEEDED, done.status());
        assertEquals(3, handler.invocationCount.get(), "处理器应恰好被调用 3 次");
        assertEquals("processed@3", h.store().get("effect:" + id).orElseThrow());
        log.info("【状态变化】第3次成功 -> SUCCEEDED；业务副作用 effect:{} = processed@3（失败的前两次无副作用落盘）", id);
    }

    @Test
    void permanentFailureGoesStraightToDeadLetterWithoutRetry(@TempDir Path dir) {
        ControllableHandler handler = new ControllableHandler("FATAL",
                ControllableHandler.Mode.PERMANENT, 0);
        Harness h = harness(dir, handler, 5);
        h.store().commit("biz-fatal", "FATAL", "{}", null, 0);

        h.dispatcher().process(h.store().claimNext(0).orElseThrow());

        List<EventRecord> dead = h.store().findDeadLetters();
        assertEquals(1, dead.size());
        assertEquals(DeadLetterReason.NON_RETRYABLE, dead.get(0).deadLetterReason());
        assertEquals(1, handler.invocationCount.get(), "永久故障不得重试");
        assertTrue(dead.get(0).lastError().contains("资源不存在"));
        log.info("【输入】处理器抛 NonRetryableEventException");
        log.info("【状态变化】PENDING -> DEAD_LETTER[NON_RETRYABLE]，仅调用 1 次，依据=处理器显式声明永久故障");
    }

    @Test
    void validationFailureAtConsumptionIsDistinctDeadLetter(@TempDir Path dir) {
        ControllableHandler handler = new ControllableHandler("BAD",
                ControllableHandler.Mode.VALIDATION, 0);
        Harness h = harness(dir, handler, 5);
        h.store().commit("biz-bad", "BAD", "{}", null, 0);

        h.dispatcher().process(h.store().claimNext(0).orElseThrow());

        EventRecord dead = h.store().findDeadLetters().get(0);
        assertEquals(DeadLetterReason.VALIDATION_FAILED, dead.deadLetterReason());
        log.info("【状态变化】消费期校验失败 -> DEAD_LETTER[VALIDATION_FAILED]，与 NON_RETRYABLE/RETRIES_EXHAUSTED 明确区分");
    }

    @Test
    void retriesExhaustedProducesDistinctDeadLetter(@TempDir Path dir) {
        ControllableHandler handler = new ControllableHandler("ALWAYS",
                ControllableHandler.Mode.TRANSIENT_N_TIMES, Integer.MAX_VALUE);
        Harness h = harness(dir, handler, 3);
        SubmissionResult r = h.store().commit("biz-always", "ALWAYS", "{}", null, 0);
        String id = r.event().eventId();

        long now = 0;
        EventRecord cur;
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claimed = h.store().claimNext(now).orElseThrow();
            h.dispatcher().process(claimed);
            cur = h.store().findByEventId(id).orElseThrow();
            if (attempt < 3) {
                assertEquals(EventStatus.RETRYING, cur.status(), "第 " + attempt + " 次后应在重试等待");
                now = cur.nextAttemptAtMillis();
            } else {
                assertEquals(EventStatus.DEAD_LETTER, cur.status(), "第 3 次失败应进入死信");
                assertEquals(DeadLetterReason.RETRIES_EXHAUSTED, cur.deadLetterReason());
            }
        }
        assertEquals(3, handler.invocationCount.get(), "达到最大尝试次数后不得再调用");
        assertTrue(h.store().claimNext(now + 1_000_000).isEmpty(), "死信事件不再被自动领取");
        log.info("【输入】持续瞬时故障 maxAttempts=3");
        log.info("【状态变化】RETRYING x2 -> DEAD_LETTER[RETRIES_EXHAUSTED]，错误信息保留最后一次故障原因，不无限重试");
    }

    @Test
    void deadLetterReplayAfterFixCompletesExactlyOnce(@TempDir Path dir) {
        // 先永久故障进入死信；修复处理器后重放，事件成功且副作用唯一
        ControllableHandler bad = new ControllableHandler("FIXABLE",
                ControllableHandler.Mode.PERMANENT, 0);
        Harness h = harness(dir, bad, 3);
        SubmissionResult r = h.store().commit("biz-fix", "FIXABLE", "{\"v\":1}", null, 0);
        String id = r.event().eventId();
        h.dispatcher().process(h.store().claimNext(0).orElseThrow());
        assertEquals(DeadLetterReason.NON_RETRYABLE,
                h.store().findByEventId(id).orElseThrow().deadLetterReason());
        log.info("【输入】首次处理永久故障进入死信[NON_RETRYABLE]");

        // 修复：替换为成功处理器，用新 registry 组装新分发器
        ControllableHandler fixed = new ControllableHandler("FIXABLE",
                ControllableHandler.Mode.OK, 0);
        HandlerRegistry fixedRegistry = new HandlerRegistry();
        fixedRegistry.register(fixed);
        EventDispatcher fixedDispatcher = new EventDispatcher(h.store(), fixedRegistry, h.policy(), 50, 2);

        EventRecord replayed = h.store().replay(id, 10_000);
        assertEquals(EventStatus.PENDING, replayed.status());
        fixedDispatcher.process(h.store().claimNext(10_000).orElseThrow());

        EventRecord done = h.store().findByEventId(id).orElseThrow();
        assertEquals(EventStatus.SUCCEEDED, done.status());
        assertEquals(1, fixed.invocationCount.get(), "修复后处理器只被调用一次");
        assertEquals("processed@1", h.store().get("effect:" + id).orElseThrow());
        log.info("【状态变化】修复后 replay：DEAD_LETTER -> PENDING -> SUCCEEDED，副作用 effect:{} 唯一存在（重放不产生重复副作用）", id);
    }
}

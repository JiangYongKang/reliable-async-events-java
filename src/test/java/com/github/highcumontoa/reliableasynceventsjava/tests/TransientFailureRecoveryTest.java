package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 瞬时故障场景：前 N 次处理失败并按退避自动重试，故障恢复后最终完成；
 * 校验副作用在失败回滚后只在成功时生效一次。
 */
class TransientFailureRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(TransientFailureRecoveryTest.class);

    @Test
    void transientFailuresAreRetriedThenRecovered() {
        // 退避 10ms，保证一轮 runOnce 即可在多次调用间快速到期
        TestSupport.Harness h = TestSupport.newHarness(5, Duration.ofMillis(10),
                Duration.ofMinutes(2), 4);

        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sideEffects = new AtomicInteger();
        h.register("FLAKY", payload -> {
            int n = attempts.incrementAndGet();
            log.info("输入(handler) 第 {} 次执行 payload={}", n, payload);
            if (n <= 2) {
                log.info("判定: 瞬时故障（模拟网络抖动），抛出 RetryableEventException");
                throw new RetryableEventException("模拟瞬时故障 第" + n + "次");
            }
            log.info("判定: 故障已恢复，提交副作用");
            sideEffects.incrementAndGet();
        });

        SubmitResult submitted = h.eventService.submitStandalone("flaky-1", "FLAKY", "{}");

        // 第 1 次：失败 -> PENDING（退避）
        String r1 = h.dispatcher.claimAndProcess(submitted.eventId());
        log.info("第1轮结果={}（预期 PENDING）", r1);
        assertEquals("PENDING", r1);

        // 等待退避到期（含抖动），期间事件不会被提前领取；到期后持续驱动直到完成
        EventRecord done = waitUntilTerminal(h, submitted.eventId(), Duration.ofSeconds(2), 10);
        log.info("最终 status={} attempts={} 处理器调用次数={} 副作用次数={}",
                done.status(), done.attempts(), attempts.get(), sideEffects.get());

        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(2, done.attempts(), "失败计数应为 2");
        assertEquals(1, sideEffects.get(), "失败事务回滚，副作用只成功一次");
        assertEquals(3, attempts.get(), "处理器应被调用 3 次（1 次成功 + 2 次失败）");

        List<EventTransition> ts = h.eventService.getTransitions(submitted.eventId());
        ts.forEach(t -> log.info("流水: {} -> {} reason={} detail={}",
                t.fromStatus(), t.toStatus(), t.reason(), t.detail()));
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.RETRY_BACKOFF),
                "必须存在 RETRY_BACKOFF 流转记录");
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.COMPLETED));
    }

    @Test
    void unknownEventTypeIsNonRetryableDeadLetter() {
        TestSupport.Harness h = TestSupport.newHarness();
        SubmitResult s = h.eventService.submitStandalone("no-handler", "MISSING_TYPE", "{}");
        log.info("输入: 提交未注册类型事件 id={}", s.eventId());

        String r = h.dispatcher.claimAndProcess(s.eventId());
        EventRecord dead = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("判定结果={} 最终状态={} 死信原因={}", r, dead.status(), dead.deadReason());
        assertEquals("DEAD_LETTER", r);
        assertEquals(DeadReason.NON_RETRYABLE, dead.deadReason());
        assertNotNull(dead.errorDetail());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 周期性 runOnce 直到事件进入 COMPLETED/DEAD_LETTER 或超时；每轮前先让出退避窗口。 */
    static EventRecord waitUntilTerminal(TestSupport.Harness h, long eventId,
                                         Duration timeout, long stepMillis) {
        long deadline = System.nanoTime() + timeout.toNanos();
        EventRecord rec = null;
        while (System.nanoTime() < deadline) {
            sleep(stepMillis);
            h.dispatcher.runOnce();
            rec = h.eventService.getEvent(eventId).orElseThrow();
            if (rec.status() == EventStatus.COMPLETED || rec.status() == EventStatus.DEAD_LETTER) {
                return rec;
            }
        }
        throw new AssertionError("事件在超时内未到达终态，最后状态: "
                + (rec == null ? "null" : rec.status() + " attempts=" + rec.attempts()));
    }
}

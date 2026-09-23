package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死信三类原因必须互相区分、可查询、可解释：
 * RETRY_EXHAUSTED / NON_RETRYABLE / VALIDATION_FAILED。
 */
class DeadLetterClassificationTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterClassificationTest.class);

    @Test
    void transientFailureUntilMaxAttemptsBecomesRetryExhausted() {
        TestSupport.Harness h = TestSupport.newHarness(3, Duration.ofMillis(5),
                Duration.ofMinutes(2), 4);
        h.register("ALWAYS_TRANSIENT", payload -> {
            log.info("输入(handler): payload={} 判定: 抛瞬时故障", payload);
            throw new RetryableEventException("下游持续不可用");
        });

        SubmitResult s = h.eventService.submitStandalone("exhaust-1", "ALWAYS_TRANSIENT", "{}");
        EventRecord dead = TransientFailureRecoveryTest.waitUntilTerminal(
                h, s.eventId(), Duration.ofSeconds(2), 5);

        log.info("判定: 最终状态={} 死信原因={} attempts={}/{} error={}",
                dead.status(), dead.deadReason(), dead.attempts(), dead.maxAttempts(),
                dead.errorDetail());
        assertEquals(EventStatus.DEAD_LETTER, dead.status());
        assertEquals(DeadReason.RETRY_EXHAUSTED, dead.deadReason());
        assertEquals(3, dead.attempts());
        assertTrue(dead.errorDetail().contains("重试已达上限"));

        List<EventTransition> ts = h.eventService.getTransitions(s.eventId());
        ts.forEach(t -> log.info("流水: {} -> {} reason={} attempts={}",
                t.fromStatus(), t.toStatus(), t.reason(), t.attempts()));
        assertEquals(TransitionReason.RETRY_EXHAUSTED, ts.get(ts.size() - 1).reason());
    }

    @Test
    void nonRetryableFailureGoesStraightToDeadLetter() {
        TestSupport.Harness h = TestSupport.newHarness();
        h.register("BIZ_FATAL", payload -> {
            log.info("输入(handler): payload={} 判定: 业务永久失败", payload);
            throw new NonRetryableEventException("账户状态不允许该操作");
        });

        SubmitResult s = h.eventService.submitStandalone("fatal-1", "BIZ_FATAL", "{}");
        String r = h.dispatcher.claimAndProcess(s.eventId());
        EventRecord dead = h.eventService.getEvent(s.eventId()).orElseThrow();

        log.info("判定结果={} 死信原因={} attempts={}（预期不重试，attempts=1）",
                r, dead.deadReason(), dead.attempts());
        assertEquals("DEAD_LETTER", r);
        assertEquals(DeadReason.NON_RETRYABLE, dead.deadReason());
        assertEquals(1, dead.attempts(), "永久故障不得重试");
        assertTrue(dead.errorDetail().contains("账户状态不允许"));
    }

    @Test
    void invalidInputAtSubmitTimeBecomesValidationFailedDeadLetter() {
        TestSupport.Harness h = TestSupport.newHarness();

        // 非法 JSON：提交即死信，不会进入调度
        SubmitResult badJson = h.eventService.submitStandalone("bad-json", "DEMO", "{not-json");
        EventRecord rec1 = h.eventService.getEvent(badJson.eventId()).orElseThrow();
        log.info("输入: payload='{not-json' 判定: 提交即死信 status={} reason={}",
                rec1.status(), rec1.deadReason());
        assertEquals(EventStatus.DEAD_LETTER, rec1.status());
        assertEquals(DeadReason.VALIDATION_FAILED, rec1.deadReason());

        // 空 type / 空 payload 也区分记录
        SubmitResult blankType = h.eventService.submitStandalone("bad-type", "  ", "{}");
        EventRecord rec2 = h.eventService.getEvent(blankType.eventId()).orElseThrow();
        log.info("输入: type=' ' 判定: reason={} detail={}", rec2.deadReason(), rec2.errorDetail());
        assertEquals(DeadReason.VALIDATION_FAILED, rec2.deadReason());

        // 调度器绝不拾取死信
        int processed = h.dispatcher.runOnce();
        log.info("调度一轮处理条数={}（预期 0，死信不会被静默重试/丢弃）", processed);
        assertEquals(0, processed);
        assertEquals(2, h.eventService.listDeadLetters().size());
    }

    @Test
    void invalidPayloadDetectedAtHandleTimeBecomesValidationFailed() {
        TestSupport.Harness h = TestSupport.newHarness();
        h.register("STRICT", payload -> {
            if (!payload.contains("requiredField")) {
                throw new InvalidEventException("缺少 requiredField");
            }
        });

        SubmitResult s = h.eventService.submitStandalone("strict-1", "STRICT", "{}");
        String r = h.dispatcher.claimAndProcess(s.eventId());
        EventRecord dead = h.eventService.getEvent(s.eventId()).orElseThrow();

        log.info("判定结果={} 死信原因={} attempts={} detail={}",
                r, dead.deadReason(), dead.attempts(), dead.errorDetail());
        assertEquals("DEAD_LETTER", r);
        assertEquals(DeadReason.VALIDATION_FAILED, dead.deadReason());
        assertEquals(1, dead.attempts(), "校验失败不得重试");
    }
}

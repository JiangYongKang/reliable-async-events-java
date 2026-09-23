package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 正常链路：提交 -> 领取 -> 完成；并验证重复领取/重放时副作用只生效一次。
 */
class HappyPathTest {

    private static final Logger log = LoggerFactory.getLogger(HappyPathTest.class);

    @Test
    void eventIsProcessedExactlyOnceAndTransitionsAreRecorded() {
        TestSupport.Harness h = TestSupport.newHarness();
        AtomicInteger sideEffects = new AtomicInteger();
        h.register("DEMO", payload -> {
            log.info("输入(handler): payload={} 判定: 执行副作用", payload);
            sideEffects.incrementAndGet();
        });

        SubmitResult submitted = h.eventService.submitStandalone("key-1", "DEMO", "{\"v\":1}");
        log.info("输入: 提交 key=key-1 判定: eventId={} status={} duplicate={}",
                submitted.eventId(), submitted.status(), submitted.duplicate());

        int processed = h.dispatcher.runOnce();
        assertEquals(1, processed);

        EventRecord done = h.eventService.getEvent(submitted.eventId()).orElseThrow();
        log.info("判定: 最终 status={} attempts={}", done.status(), done.attempts());
        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(1, sideEffects.get(), "副作用应恰好执行一次");

        // 再次驱动调度：已完成事件不会被重复领取
        int secondRound = h.dispatcher.runOnce();
        log.info("再次调度，处理条数={}（预期 0），副作用计数={}（预期 1）", secondRound, sideEffects.get());
        assertEquals(0, secondRound);
        assertEquals(1, sideEffects.get());

        // 流转流水可查询、原因可解释：ENQUEUED -> CLAIMED -> COMPLETED
        List<EventTransition> ts = h.eventService.getTransitions(submitted.eventId());
        ts.forEach(t -> log.info("流水: {} -> {} reason={} attempts={} detail={}",
                t.fromStatus(), t.toStatus(), t.reason(), t.attempts(), t.detail()));
        assertEquals(3, ts.size());
        assertEquals(TransitionReason.ENQUEUED, ts.get(0).reason());
        assertEquals(TransitionReason.CLAIMED, ts.get(1).reason());
        assertEquals(TransitionReason.COMPLETED, ts.get(2).reason());
    }

    @Test
    void sameIdempotencyKeyIsDeduplicated() {
        TestSupport.Harness h = TestSupport.newHarness();
        SubmitResult first = h.eventService.submitStandalone("dup-key", "DEMO", "{\"v\":1}");
        SubmitResult second = h.eventService.submitStandalone("dup-key", "DEMO", "{\"v\":2}");

        log.info("第一次提交 id={} duplicate={}", first.eventId(), first.duplicate());
        log.info("重复提交 id={} duplicate={}（预期与第一次同 id）", second.eventId(), second.duplicate());

        assertTrue(first.created());
        assertTrue(second.duplicate());
        assertEquals(first.eventId(), second.eventId());
        assertEquals(1, h.repository.findByStatus(EventStatus.PENDING, 10).size(),
                "同一幂等键只能有一条事件");
    }
}

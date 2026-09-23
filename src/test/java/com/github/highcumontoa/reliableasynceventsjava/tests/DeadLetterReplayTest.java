package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死信恢复：
 * 1. 永久故障修复后重放 -> 最终完成，副作用一次；
 * 2. 重试耗尽死信，重放后成功；
 * 3. 非死信状态重放为空操作；
 * 4. 并发重放只有一个生效（不产生重复副作用）。
 */
class DeadLetterReplayTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayTest.class);

    @Test
    void nonRetryableDeadLetterIsReplayedAfterCauseFixed() {
        TestSupport.Harness h = TestSupport.newHarness();
        AtomicBoolean fixed = new AtomicBoolean(false);
        AtomicInteger calls = new AtomicInteger();
        h.register("PAYMENT", payload -> {
            calls.incrementAndGet();
            log.info("输入(handler) 已修复={} 第 {} 次调用", fixed.get(), calls.get());
            if (!fixed.get()) {
                throw new NonRetryableEventException("支付渠道账户被冻结");
            }
        });

        SubmitResult s = h.eventService.submitStandalone("pay-1", "PAYMENT", "{}");
        String r1 = h.dispatcher.claimAndProcess(s.eventId());
        EventRecord dead = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("修复前: 结果={} status={} reason={}", r1, dead.status(), dead.deadReason());
        assertEquals(DeadReason.NON_RETRYABLE, dead.deadReason());

        // 修复前再次调度不应有任何变化
        assertEquals(0, h.dispatcher.runOnce());

        // 运维修复根因 -> 重放
        fixed.set(true);
        boolean replayed = h.eventService.replayDeadLetter(s.eventId(), "支付账户已解冻");
        log.info("修复后重放 replayed={}", replayed);
        assertTrue(replayed);

        int processed = h.dispatcher.runOnce();
        EventRecord done = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("重放后 runOnce={} status={} 处理器调用次数={}", processed, done.status(), calls.get());
        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(2, calls.get(), "失败一次 + 重放成功一次");

        // completed 事件不能再次重放
        assertFalse(h.eventService.replayDeadLetter(s.eventId(), "重复重放请求"));
        List<EventTransition> ts = h.eventService.getTransitions(s.eventId());
        ts.forEach(t -> log.info("流水: {} -> {} reason={} detail={}",
                t.fromStatus(), t.toStatus(), t.reason(), t.detail()));
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.REPLAYED));
    }

    @Test
    void retryExhaustedDeadLetterCanBeReplayed() {
        TestSupport.Harness h = TestSupport.newHarness(2, Duration.ofMillis(5),
                Duration.ofMinutes(2), 2);
        AtomicInteger failuresLeft = new AtomicInteger(3);
        AtomicInteger calls = new AtomicInteger();
        h.register("EXT", payload -> {
            calls.incrementAndGet();
            if (failuresLeft.getAndDecrement() > 0) {
                throw new RetryableEventException("外部系统不可用");
            }
        });

        SubmitResult s = h.eventService.submitStandalone("ext-1", "EXT", "{}");
        EventRecord dead = TransientFailureRecoveryTest.waitUntilTerminal(
                h, s.eventId(), Duration.ofSeconds(2), 5);
        log.info("重试耗尽: status={} reason={} attempts={}",
                dead.status(), dead.deadReason(), dead.attempts());
        assertEquals(DeadReason.RETRY_EXHAUSTED, dead.deadReason());

        // 外部系统已恢复（剩余失败次数不再生效因为已经耗尽），重放从头计数
        assertTrue(h.eventService.replayDeadLetter(s.eventId(), "外部系统恢复，重新投递"));
        EventRecord reset = h.eventService.getEvent(s.eventId()).orElseThrow();
        assertEquals(EventStatus.PENDING, reset.status());
        assertEquals(0, reset.attempts(), "重放后失败计数清零，重新享受完整重试预算");

        EventRecord done = TransientFailureRecoveryTest.waitUntilTerminal(
                h, s.eventId(), Duration.ofSeconds(2), 5);
        assertEquals(EventStatus.COMPLETED, done.status());
    }

    @Test
    void concurrentReplaysOnlyOneTakesEffect() throws Exception {
        TestSupport.Harness h = TestSupport.newHarness();
        AtomicBoolean fixed = new AtomicBoolean(false);
        AtomicInteger calls = new AtomicInteger();
        h.register("SVC", payload -> {
            calls.incrementAndGet();
            if (!fixed.get()) {
                throw new NonRetryableEventException("配置缺失");
            }
        });
        SubmitResult s = h.eventService.submitStandalone("svc-1", "SVC", "{}");
        h.dispatcher.claimAndProcess(s.eventId());
        assertEquals(DeadReason.NON_RETRYABLE,
                h.eventService.getEvent(s.eventId()).orElseThrow().deadReason());

        int replays = 16;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < replays; i++) {
            results.add(pool.submit(() -> {
                gate.await();
                return h.eventService.replayDeadLetter(s.eventId(), "并发重放");
            }));
        }
        fixed.set(true);
        gate.countDown();
        int success = 0;
        for (Future<Boolean> f : results) {
            if (f.get()) {
                success++;
            }
        }
        pool.shutdown();
        log.info("{} 个并发重放，成功 {} 个（预期 1）", replays, success);
        assertEquals(1, success, "并发重放必须只有一个生效");

        int processed = h.dispatcher.runOnce();
        EventRecord done = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("重放处理条数={} 最终状态={} 副作用调用={}（预期 1）",
                processed, done.status(), calls.get());
        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(2, calls.get(), "失败一次 + 重放成功一次，无重复副作用");
    }
}

package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消费者崩溃/重启场景：
 * 1. 处理前崩溃（PROCESSING 租约遗留）：租约到期回收后重新处理完成；
 * 2. 副作用已落库但完成标记丢失：回收后命中消费端去重，不产生重复副作用。
 */
class LeaseRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseRecoveryTest.class);

    @Test
    void staleLeaseBeforeSideEffectIsReclaimedAndProcessed() {
        // 租约 TTL 80ms，测试中等待其过期
        TestSupport.Harness h = TestSupport.newHarness(5, Duration.ofMillis(5),
                Duration.ofMillis(80), 4);
        AtomicInteger calls = new AtomicInteger();
        h.register("RECOVER", payload -> {
            log.info("输入(handler): payload={} 判定: 执行副作用", payload);
            calls.incrementAndGet();
        });

        SubmitResult s = h.eventService.submitStandalone("crash-before", "RECOVER", "{}");

        // 模拟消费者在"领取之后、执行之前"崩溃：事件停留在 PROCESSING，且租约时间已过期
        String token = "dead-worker-token";
        new TransactionTemplate(h.txManager).execute(st -> {
            boolean claimed = h.repository.claim(s.eventId(), token, Instant.now());
            assertTrue(claimed);
            h.jdbc.update("UPDATE outbox_event SET claimed_at=? WHERE id=?",
                    Timestamp.from(Instant.now().minusMillis(40)), s.eventId());
            return null;
        });
        EventRecord stuck = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("模拟崩溃后: status={} claimToken={} claimedAt={}",
                stuck.status(), stuck.claimToken(), stuck.claimedAt());
        assertEquals(EventStatus.PROCESSING, stuck.status());

        // 租约未过期：调度器不应插手他人的处理权
        int processedEarly = h.dispatcher.runOnce();
        log.info("租约未过期时 runOnce 处理条数={}（预期 0）", processedEarly);
        assertEquals(0, processedEarly);

        // 租约过期后：回收 -> 重新领取 -> 完成
        sleep(100);
        int processed = h.dispatcher.runOnce();
        EventRecord done = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("租约过期后 runOnce={} 最终 status={} 副作用次数={}",
                processed, done.status(), calls.get());
        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(1, calls.get());

        List<EventTransition> ts = h.eventService.getTransitions(s.eventId());
        ts.forEach(t -> log.info("流水: {} -> {} reason={}", t.fromStatus(), t.toStatus(), t.reason()));
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.LEASE_STALE_RECLAIMED));
    }

    @Test
    void staleLeaseAfterSideEffectDoesNotDuplicateSideEffect() {
        TestSupport.Harness h = TestSupport.newHarness(5, Duration.ofMillis(5),
                Duration.ofMillis(80), 4);
        AtomicInteger calls = new AtomicInteger();
        h.register("RECOVER2", payload -> {
            calls.incrementAndGet();
            log.info("处理器被调用（如果去重生效则不应出现本行）");
        });

        SubmitResult s = h.eventService.submitStandalone("crash-after", "RECOVER2", "{}");

        // 模拟：副作用与去重标记已随业务事务提交（外部系统视角已经成功），
        // 但消费者在回写 COMPLETED 前崩溃 —— 事件仍 PROCESSING
        String token = "dead-worker-token";
        new TransactionTemplate(h.txManager).execute(st -> {
            assertTrue(h.repository.claim(s.eventId(), token, Instant.now()));
            // 模拟"副作用+去重标记"已提交：崩溃前的事务其实已完成这一步
            assertTrue(h.repository.markProcessingIfFirst(s.eventId()));
            // 回拨租约时间，模拟该消费者已崩溃且租约已过期
            h.jdbc.update("UPDATE outbox_event SET claimed_at=? WHERE id=?",
                    Timestamp.from(Instant.now().minusMillis(200)), s.eventId());
            return null;
        });
        log.info("模拟崩溃: 事件停在 PROCESSING，但 processed_event 去重标记已存在");

        sleep(100);
        int processed = h.dispatcher.runOnce();
        EventRecord done = h.eventService.getEvent(s.eventId()).orElseThrow();
        log.info("回收重跑后 runOnce={} status={} 处理器调用次数={}（预期 0，命中去重）",
                processed, done.status(), calls.get());
        assertEquals(EventStatus.COMPLETED, done.status());
        assertEquals(0, calls.get(), "副作用已生效过，恢复后绝不重复执行");

        List<EventTransition> ts = h.eventService.getTransitions(s.eventId());
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.LEASE_STALE_RECLAIMED));
        assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.COMPLETED
                && "去重命中，副作用此前已生效".equals(t.detail())));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

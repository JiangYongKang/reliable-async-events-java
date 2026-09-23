package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重启恢复：模拟“处理中进程崩溃”（事件已被领取但未落终态）。
 * 重启后：已 SUCCEEDED 的事件不重复处理；卡在领取态的事件在租约超时后被重新领取并完成，
 * 全程不丢失、副作用恰好一次。
 */
class RestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTests.class);

    @Test
    void inFlightEventIsRecoveredAfterRestart(@TempDir Path dir) {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(3, 10, 100, 0);
        long lease = 1000;

        // ---- 崩溃前：1 条成功完成；2 条被领取但处理结果未落盘（模拟进程在处理中崩溃）
        DurableWALEventStore before = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, lease);
        before.commit("done-before-crash", "R", "{}", null, 0);
        before.commit("in-flight-1", "R", "{}", null, 0);
        before.commit("in-flight-2", "R", "{}", null, 0);

        // 明确挑选一条作为“崩溃前已完成”，另两条保持在途，避免依赖领取顺序
        String doneKey = "done-before-crash";
        String doneId = before.findByIdempotencyKey(doneKey).orElseThrow().eventId();
        EventStore.ClaimedEvent claimed1 = before.claimNext(0).orElseThrow();
        EventStore.ClaimedEvent claimed2 = before.claimNext(1).orElseThrow();
        EventStore.ClaimedEvent claimed3 = before.claimNext(2).orElseThrow();
        java.util.Map<String, EventStore.ClaimedEvent> byId = new java.util.HashMap<>();
        for (EventStore.ClaimedEvent c : new EventStore.ClaimedEvent[]{claimed1, claimed2, claimed3}) {
            byId.put(c.event().eventId(), c);
        }
        EventStore.ClaimedEvent done = byId.get(doneId);
        before.markSucceeded(done.event().eventId(), done.leaseToken(),
                Map.of("effect:" + doneId, "once"), 1);
        // 另两条不调用任何终态方法，模拟领取后进程立刻崩溃
        before.flush();
        log.info("【输入】崩溃前：done-before-crash 已 SUCCEEDED，in-flight-1/2 已领取(租约)但未落终态；WAL 已 fsync");

        // ---- 重启：全新的存储实例从同一 WAL 重建
        AtomicInteger handleCalls = new AtomicInteger();
        EventHandler handler = new EventHandler() {
            @Override
            public String type() {
                return "R";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                handleCalls.incrementAndGet();
                return Map.of("effect:" + event.eventId(), "once");
            }
        };
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handler);
        DurableWALEventStore after = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, lease);
        EventDispatcher dispatcher = new EventDispatcher(after, registry, policy, 1, 2);

        // 重启后立即查询：SUCCEEDED 仍是 SUCCEEDED，在途事件仍是 PENDING 且带未释放租约
        assertEquals(EventStatus.SUCCEEDED,
                after.findByIdempotencyKey(doneKey).orElseThrow().status());
        assertEquals(2, after.findByStatus(EventStatus.PENDING).size());
        log.info("【状态变化】重启恢复：成功事件保持 SUCCEEDED；2 条在途事件重建为带租约的 PENDING，等待租约超时回收");

        // 租约未超时前不应被领取；超过 lease 后被重新领取并完成
        assertTrue(after.claimNext(500).isEmpty(), "租约未到期，在途事件不得被重复领取");
        long restartTime = 2000;
        for (int i = 0; i < 2; i++) {
            EventStore.ClaimedEvent c = after.claimNext(restartTime + i * 10L).orElseThrow();
            dispatcher.process(c);
        }

        assertEquals(3, after.findByStatus(EventStatus.SUCCEEDED).size(), "三条事件最终全部成功，无丢失");
        assertEquals(0, after.findByStatus(EventStatus.DEAD_LETTER).size());
        assertEquals(2, handleCalls.get(), "仅 2 条崩溃前在途事件被重新处理，崩溃前已成功的不重复处理");
        for (String key : new String[]{"done-before-crash", "in-flight-1", "in-flight-2"}) {
            String id = after.findByIdempotencyKey(key).orElseThrow().eventId();
            assertEquals("once", after.get("effect:" + id).orElseThrow(),
                    "事件 " + key + " 副作用唯一");
        }
        log.info("【判定依据】重启后处理器仅被调用 2 次；3/3 SUCCEEDED，每个 effect:* KV 唯一 => 不重复不丢失");
    }
}

package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发消费安全：多分发器/多线程同时领取与处理时，每条事件恰好被成功处理一次，
 * 业务副作用不重复；瞬时失败事件最终恢复成功。
 */
class ConcurrentConsumptionTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentConsumptionTests.class);

    @Test
    void concurrentWorkersProcessEachEventExactlyOnce(@TempDir Path dir) throws Exception {
        int maxAttempts = 5;
        ExponentialBackoffRetryPolicy policy =
                new ExponentialBackoffRetryPolicy(maxAttempts, 1, 10, 0, new java.util.Random(1));
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, 30_000);

        int eventCount = 200;
        for (int i = 0; i < eventCount; i++) {
            store.commit("k-" + i, "WORK", "{}", null, 0);
        }

        // 成功处理器：统计每个事件的成功处理次数；副作用以 KV 计数模拟“入账一次”
        Map<String, AtomicInteger> successPerEvent = new ConcurrentHashMap<>();
        EventHandler handler = new EventHandler() {
            @Override
            public String type() {
                return "WORK";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                successPerEvent.computeIfAbsent(event.eventId(), x -> new AtomicInteger()).incrementAndGet();
                return Map.of("done:" + event.eventId(), "1");
            }
        };
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handler);

        int dispatcherCount = 4;
        ExecutorService pool = Executors.newFixedThreadPool(dispatcherCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(dispatcherCount);
        AtomicBoolean stop = new AtomicBoolean(false);
        List<EventDispatcher> dispatcherList = new java.util.concurrent.CopyOnWriteArrayList<>();

        log.info("【输入】{} 条事件 + {} 个并发分发器（各自多 worker），全部成功处理器", eventCount, dispatcherCount);

        for (int d = 0; d < dispatcherCount; d++) {
            EventDispatcher dispatcher = new EventDispatcher(store, registry, policy, 1, 4);
            dispatcherList.add(dispatcher);
            pool.submit(() -> {
                try {
                    start.await();
                    dispatcher.start();
                    while (!stop.get()) {
                        Thread.sleep(2);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finish.countDown();
                }
            });
        }
        start.countDown();

        // 等待全部事件成功（上限保护）
        long deadline = System.currentTimeMillis() + 20_000;
        while (store.findByStatus(EventStatus.SUCCEEDED).size() < eventCount
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        stop.set(true);
        dispatcherList.forEach(EventDispatcher::stop);
        assertTrue(finish.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(eventCount, store.findByStatus(EventStatus.SUCCEEDED).size(),
                "所有事件必须最终成功，不丢失");
        assertEquals(0, store.findByStatus(EventStatus.DEAD_LETTER).size());
        assertEquals(0, store.findByStatus(EventStatus.RETRYING).size());

        for (int i = 0; i < eventCount; i++) {
            String id = store.findByIdempotencyKey("k-" + i).orElseThrow().eventId();
            int successes = successPerEvent.getOrDefault(id, new AtomicInteger()).get();
            assertEquals(1, successes, "事件 " + id + " 成功处理次数必须恰好为 1，实际=" + successes);
            assertEquals("1", store.get("done:" + id).orElseThrow(), "副作用 KV 唯一");
        }
        log.info("【状态变化】{}/{} SUCCEEDED，无死信无重试残留；逐条校验 successCount==1 且 done:* KV 唯一",
                store.findByStatus(EventStatus.SUCCEEDED).size(), eventCount);
    }

    @Test
    void concurrentConsumptionWithTransientFailuresRecovers(@TempDir Path dir) throws Exception {
        ExponentialBackoffRetryPolicy policy =
                new ExponentialBackoffRetryPolicy(5, 1, 20, 0, new java.util.Random(7));
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, 30_000);
        int eventCount = 50;
        for (int i = 0; i < eventCount; i++) {
            store.commit("t-" + i, "FLAKY", "{}", null, 0);
        }

        // 每个事件前 2 次处理随机瞬时失败，之后成功；记录每次调用
        Map<String, AtomicInteger> callsPerEvent = new ConcurrentHashMap<>();
        Set<String> sideEffects = ConcurrentHashMap.newKeySet();
        EventHandler handler = new EventHandler() {
            @Override
            public String type() {
                return "FLAKY";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                int n = callsPerEvent.computeIfAbsent(event.eventId(), x -> new AtomicInteger()).incrementAndGet();
                if (n <= 2) {
                    throw new RuntimeException("随机瞬时故障 503（第 " + n + " 次）");
                }
                sideEffects.add("eff:" + event.eventId());
                return Map.of("eff:" + event.eventId(), "ok");
            }
        };
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(handler);
        EventDispatcher dispatcher = new EventDispatcher(store, registry, policy, 1, 6);
        dispatcher.start();
        log.info("【输入】{} 条事件，每个事件前 2 次处理必瞬时失败，6 worker 并发", eventCount);
        try {
            long deadline = System.currentTimeMillis() + 20_000;
            while (store.findByStatus(EventStatus.SUCCEEDED).size() < eventCount
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        } finally {
            dispatcher.stop();
        }
        assertEquals(eventCount, store.findByStatus(EventStatus.SUCCEEDED).size());
        assertEquals(0, store.findByStatus(EventStatus.DEAD_LETTER).size());
        for (int i = 0; i < eventCount; i++) {
            String id = store.findByIdempotencyKey("t-" + i).orElseThrow().eventId();
            int calls = callsPerEvent.get(id).get();
            assertTrue(calls >= 3, "前两次失败，至少调用 3 次，实际=" + calls);
            assertEquals("ok", store.get("eff:" + id).orElseThrow(), "成功副作用恰好一份");
        }
        log.info("【判定依据】{}/{} 事件经瞬时失败重试后恢复 SUCCEEDED；失败尝试无副作用落盘，成功副作用唯一",
                store.findByStatus(EventStatus.SUCCEEDED).size(), eventCount);
    }
}

package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发场景：
 * 1. 同一幂等键并发重复提交 —— 只有一条事件；
 * 2. 多线程并发消费同一批事件 —— 每条只被一个消费者处理，副作用恰好一次。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    @Test
    void concurrentDuplicateSubmissionsProduceSingleEvent() throws Exception {
        TestSupport.Harness h = TestSupport.newHarness();
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmitResult>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                SubmitResult r = h.eventService.submitStandalone(
                        "same-key", "DEMO", "{\"i\":" + Thread.currentThread().getId() + "}");
                log.info("并发提交线程={} 判定: eventId={} duplicate={}",
                        Thread.currentThread().getId(), r.eventId(), r.duplicate());
                return r;
            }));
        }
        start.countDown();

        Set<Long> ids = new java.util.TreeSet<>();
        int duplicates = 0;
        for (Future<SubmitResult> f : futures) {
            SubmitResult r = f.get(5, TimeUnit.SECONDS);
            ids.add(r.eventId());
            if (r.duplicate()) {
                duplicates++;
            }
        }
        pool.shutdown();

        log.info("判定: {} 个并发提交 -> 唯一事件ID集合={}，duplicate 返回 {}/{}",
                threads, ids, duplicates, threads);
        assertEquals(1, ids.size(), "同一幂等键只能产生一条事件");
        assertEquals(threads - 1, duplicates, "除首次外全部应判为重复");
        assertEquals(1, h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE idempotency_key='same-key'", Integer.class)
                .intValue());
    }

    @Test
    void concurrentConsumersProcessEachEventExactlyOnce() throws Exception {
        TestSupport.Harness h = TestSupport.newHarness(20, Duration.ofMillis(5),
                Duration.ofMinutes(2), 8);

        int eventCount = 60;
        ConcurrentHashMap<Long, AtomicInteger> perEventCalls = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Boolean> transientArmed = new ConcurrentHashMap<>();
        for (long i = 1; i <= eventCount; i++) {
            if (Math.floorMod(i * 7, 11) == 0) {
                transientArmed.put(i, Boolean.TRUE); // 5 条事件：首次尝试瞬时失败一次
            }
        }
        AtomicInteger sideEffects = new AtomicInteger();
        h.register("DEMO", payload -> {
            long id = parseId(payload);
            perEventCalls.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            // 该事件的首次处理遇到一次瞬时故障，之后自动恢复
            if (transientArmed.remove(id) != null) {
                log.info("event={} 首次处理注入瞬时故障", id);
                throw new RetryableEventException("偶发瞬时故障 event=" + id);
            }
            sideEffects.incrementAndGet();
        });

        List<SubmitResult> submitted = new ArrayList<>();
        for (long i = 1; i <= eventCount; i++) {
            submitted.add(h.eventService.submitStandalone("k" + i, "DEMO", "{\"id\":" + i + "}"));
        }
        log.info("输入: 提交 {} 条事件，启动并发消费（claimToken 乐观领取 + 去重表）", eventCount);

        // 多个消费者线程同时驱动 runOnce，制造对同一事件的竞争
        ExecutorService consumers = Executors.newFixedThreadPool(6);
        CountDownLatch stop = new CountDownLatch(1);
        for (int c = 0; c < 6; c++) {
            consumers.submit(() -> {
                try {
                    while (stop.getCount() > 0) {
                        h.dispatcher.runOnce();
                        if (stop.await(10, TimeUnit.MILLISECONDS)) {
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 等待全部完成（重试退避仅 5ms 起跳）
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Integer completed = h.jdbc.queryForObject(
                    "SELECT COUNT(*) FROM outbox_event WHERE status='COMPLETED'", Integer.class);
            if (completed != null && completed == eventCount) {
                break;
            }
            Thread.sleep(20);
        }
        stop.countDown();
        consumers.shutdown();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        Integer completed = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE status='COMPLETED'", Integer.class);
        Integer dead = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE status='DEAD_LETTER'", Integer.class);
        log.info("判定: COMPLETED={} DEAD_LETTER={} 副作用总次数={}", completed, dead, sideEffects.get());

        assertEquals(eventCount, completed, "全部事件应最终完成");
        assertEquals(0, dead, "本场景不应出现死信");
        assertEquals(eventCount, sideEffects.get(), "每条事件副作用恰好一次");
        assertEquals(eventCount, perEventCalls.size());
        String multiProcessed = perEventCalls.entrySet().stream()
                .filter(e -> e.getValue().get() > 1)
                .map(e -> e.getKey() + "x" + e.getValue().get())
                .collect(Collectors.joining(","));
        log.info("被多次调用处理器的事件: [{}]（允许多次尝试，但副作用只一次）", multiProcessed);
        // 处理器可能被多次调用（重试），但成功路径的副作用必须只发生一次（已由 sideEffects 断言）
        assertEquals(eventCount,
                h.jdbc.queryForObject("SELECT COUNT(*) FROM processed_event", Integer.class).intValue(),
                "消费端去重表每条事件恰好一行");
    }

    private static long parseId(String payload) {
        int i = payload.indexOf("\"id\":");
        int j = i + 5;
        while (j < payload.length() && !Character.isDigit(payload.charAt(j))) {
            j++;
        }
        int k = j;
        while (k < payload.length() && Character.isDigit(payload.charAt(k))) {
            k++;
        }
        return Long.parseLong(payload.substring(j, k));
    }
}

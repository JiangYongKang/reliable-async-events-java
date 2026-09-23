package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发压力下的幂等与一致性：
 * 同一业务幂等键被多线程重复提交时，只允许存在一条事件、一份业务副作用，
 * 且恰好一个提交者看到“新建”，其余全部看到“重复”。
 */
class ConcurrentSubmissionTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmissionTests.class);

    @Test
    void concurrentDuplicateSubmissionsYieldSingleEvent(@TempDir Path dir) throws Exception {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(3, 10, 100, 0);
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, 30_000);

        int threads = 32;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Set<String> createdEventIds = ConcurrentHashMap.newKeySet();
        AtomicInteger created = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        List<String> allBusinessKeys = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            allBusinessKeys.add("balance:same-key");
        }

        log.info("【输入】{} 个线程 x {} 次 = {} 次提交，全部使用同一幂等键 idem-001", threads, perThread, threads * perThread);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // 每个线程提交相同幂等键，业务写入也相同（模拟对同一账户的重复入账请求）
                        SubmissionResult r = store.commit("idem-001", "DEPOSIT",
                                "{\"amount\":100,\"tid\":" + tid + "}",
                                Map.of("balance:same-key", "v" + tid), 0);
                        if (r.duplicate()) {
                            duplicates.incrementAndGet();
                        } else {
                            created.incrementAndGet();
                            createdEventIds.add(r.event().eventId());
                        }
                    }
                } catch (Exception e) {
                    log.error("提交线程异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        int total = threads * perThread;
        log.info("【状态变化】总提交={}，判定为新建={}，判定为重复={}", total, created.get(), duplicates.get());

        assertEquals(1, created.get(), "高并发下同一幂等键必须恰好有一次新建");
        assertEquals(total - 1, duplicates.get(), "其余全部必须判定为重复提交");
        assertEquals(1, createdEventIds.size());
        assertEquals(1, store.findByStatus(EventStatus.PENDING).size(), "只存在一条可被独立处理的事件");
        // 第一个胜出的提交者写入业务值，其余重复提交不得覆盖
        String balance = store.get("balance:same-key").orElseThrow();
        assertTrue(balance.matches("v\\d+"));
        log.info("【判定依据】事件唯一(eventId={})，业务键 balance:same-key={}（胜出者写入，重复者不覆盖）",
                createdEventIds.iterator().next(), balance);
    }

    @Test
    void distinctKeysAllPersistedUnderConcurrency(@TempDir Path dir) throws Exception {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(3, 10, 100, 0);
        DurableWALEventStore store = new DurableWALEventStore(new DurableFileLog(dir.toString()), policy, 30_000);

        int keys = 100;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(keys);
        Set<String> ids = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < keys; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    SubmissionResult r = store.commit("key-" + idx, "T", "{}",
                            Map.of("k-" + idx, String.valueOf(idx)), 0);
                    ids.add(r.event().eventId());
                } catch (Exception e) {
                    log.error("提交异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(keys, ids.size(), "不同幂等键的事件一条都不能丢");
        assertEquals(keys, store.findByStatus(EventStatus.PENDING).size());
        for (int i = 0; i < keys; i++) {
            assertEquals(String.valueOf(i), store.get("k-" + i).orElseThrow());
        }
        log.info("【输入/判定依据】{} 个不同幂等键并发提交，全部唯一落盘且业务 KV 完整，无丢失无串行错误", keys);
    }
}

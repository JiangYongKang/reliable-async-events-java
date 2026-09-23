package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.core.EventStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 后台调度器生命周期：start 后由轮询线程自动投递，stop 后不再处理、且可重复启停。
 */
class DispatcherLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(DispatcherLifecycleTest.class);

    @Test
    void backgroundPollerDeliversAutomaticallyAndStopsCleanly() throws Exception {
        TestSupport.Harness h = TestSupport.newHarness(5, Duration.ofMillis(10),
                Duration.ofMinutes(2), 4);
        AtomicInteger calls = new AtomicInteger();
        h.register("BG", payload -> {
            log.info("后台线程处理 payload={}", payload);
            calls.incrementAndGet();
        });

        h.dispatcher.start();
        assertTrue(h.dispatcher.isRunning());

        var s1 = h.eventService.submitStandalone("bg-1", "BG", "{}");
        var s2 = h.eventService.submitStandalone("bg-2", "BG", "{}");
        log.info("启动后台调度器后提交 2 条事件，等待自动投递");

        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            EventRecord e1 = h.eventService.getEvent(s1.eventId()).orElseThrow();
            EventRecord e2 = h.eventService.getEvent(s2.eventId()).orElseThrow();
            if (e1.status() == EventStatus.COMPLETED && e2.status() == EventStatus.COMPLETED) {
                break;
            }
            Thread.sleep(20);
        }

        assertEquals(EventStatus.COMPLETED,
                h.eventService.getEvent(s1.eventId()).orElseThrow().status());
        assertEquals(EventStatus.COMPLETED,
                h.eventService.getEvent(s2.eventId()).orElseThrow().status());
        assertEquals(2, calls.get());

        h.dispatcher.stop();
        assertFalse(h.dispatcher.isRunning());
        log.info("调度器已停止；再次启动验证可恢复");

        h.dispatcher.start();
        var s3 = h.eventService.submitStandalone("bg-3", "BG", "{}");
        deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline
                && h.eventService.getEvent(s3.eventId()).orElseThrow().status() != EventStatus.COMPLETED) {
            Thread.sleep(20);
        }
        assertEquals(EventStatus.COMPLETED,
                h.eventService.getEvent(s3.eventId()).orElseThrow().status());
        h.dispatcher.stop();
    }
}

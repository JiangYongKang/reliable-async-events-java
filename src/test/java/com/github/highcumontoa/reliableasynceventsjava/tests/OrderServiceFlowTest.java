package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.InvalidEventException;
import com.github.highcumontoa.reliableasynceventsjava.demo.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 演示业务端到端：
 * 1. 下单与事件同事务提交，事件被消费产生投影；
 * 2. 同 requestId 并发重复下单，只产生一个订单/一条事件/一个投影；
 * 3. 瞬时故障后自动恢复，最终一致；
 * 4. 输入非法 -> 订单不存在 + VALIDATION_FAILED 死信；
 * 5. 永久故障 -> 死信，修复注入后重放完成。
 */
class OrderServiceFlowTest {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceFlowTest.class);

    @Test
    void placeOrderAndEventCompleteWithProjection() {
        TestSupport.Harness h = TestSupport.newHarness();
        OrderService orders = new OrderService(h.eventService, h.txManager, h.dataSource);
        h.register(OrderService.EVENT_TYPE,
                new OrderCreatedHandler(h.dataSource, new FaultSimulator()));

        OrderResult r = orders.placeOrder(new OrderCommand("req-1", "book", 2));
        log.info("下单结果 orderId={} eventId={} duplicated={}", r.orderId(), r.eventId(), r.duplicated());

        int processed = h.dispatcher.runOnce();
        Integer projectionCount = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_projection WHERE order_id=?", Integer.class, r.orderId());
        log.info("调度处理条数={} 投影数={}（预期 1/1）", processed, projectionCount);
        assertEquals(1, processed);
        assertEquals(1, projectionCount);
    }

    @Test
    void concurrentDuplicateOrdersResultInSingleOrderAndEvent() throws Exception {
        TestSupport.Harness h = TestSupport.newHarness();
        OrderService orders = new OrderService(h.eventService, h.txManager, h.dataSource);
        h.register(OrderService.EVENT_TYPE,
                new OrderCreatedHandler(h.dataSource, new FaultSimulator()));

        int n = 20;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<OrderResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return orders.placeOrder(new OrderCommand("req-same", "phone", 1));
            }));
        }
        gate.countDown();

        List<OrderResult> results = new ArrayList<>();
        for (Future<OrderResult> f : futures) {
            results.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();

        long distinctOrders = results.stream().map(OrderResult::orderId).distinct().count();
        long distinctEvents = results.stream().map(OrderResult::eventId).distinct().count();
        long duplicated = results.stream().filter(OrderResult::duplicated).count();
        log.info("{} 个并发下单: 不同订单={} 不同事件={} 返回duplicated={}",
                n, distinctOrders, distinctEvents, duplicated);
        assertEquals(1, distinctOrders);
        assertEquals(1, distinctEvents);
        assertEquals(n - 1, duplicated);

        h.dispatcher.runOnce();
        Integer ordersInDb = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE request_id='req-same'", Integer.class);
        Integer eventsInDb = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE idempotency_key='req-same'", Integer.class);
        Integer projections = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_projection", Integer.class);
        log.info("持久层: orders={} outbox={} projections={}（全部应为 1）",
                ordersInDb, eventsInDb, projections);
        assertEquals(1, ordersInDb);
        assertEquals(1, eventsInDb);
        assertEquals(1, projections, "重复下单/重复消费都不得产生重复投影");
    }

    @Test
    void transientFaultThenRecoveryLeadsToFinalConsistency() {
        TestSupport.Harness h = TestSupport.newHarness(5, Duration.ofMillis(10),
                Duration.ofMinutes(2), 2);
        OrderService orders = new OrderService(h.eventService, h.txManager, h.dataSource);
        FaultSimulator faults = new FaultSimulator();
        h.register(OrderService.EVENT_TYPE, new OrderCreatedHandler(h.dataSource, faults));

        OrderResult r = orders.placeOrder(new OrderCommand("req-recover", "laptop", 1));
        faults.armTransientFailures(String.valueOf(r.orderId()), 2);
        log.info("为订单 {} 注入 2 次瞬时故障", r.orderId());

        var done = TransientFailureRecoveryTest.waitUntilTerminal(
                h, r.eventId(), Duration.ofSeconds(3), 10);
        Integer projection = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_projection WHERE order_id=?", Integer.class, r.orderId());
        log.info("最终: event status={} attempts={} 投影={}（预期 COMPLETED/2/1）",
                done.status(), done.attempts(), projection);
        assertEquals("COMPLETED", done.status().name());
        assertEquals(2, done.attempts());
        assertEquals(1, projection);
        assertEquals(0, faults.transientFailuresRemaining(String.valueOf(r.orderId())));
    }

    @Test
    void invalidOrderInputLeavesNoOrderAndCreatesValidationDeadLetter() {
        TestSupport.Harness h = TestSupport.newHarness();
        OrderService orders = new OrderService(h.eventService, h.txManager, h.dataSource);

        assertThrows(InvalidEventException.class,
                () -> orders.placeOrder(new OrderCommand("req-bad", "book", 0)));
        log.info("非法下单(quantity=0) 已被拒绝并返回 4xx 语义异常");

        Integer orderCount = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE request_id='req-bad'", Integer.class);
        assertEquals(0, orderCount, "非法输入不得产生订单");

        var dead = h.eventService.findByIdempotencyKey("req-bad").orElseThrow();
        log.info("对应事件 status={} reason={} detail={}",
                dead.status(), dead.deadReason(), dead.errorDetail());
        assertEquals("DEAD_LETTER", dead.status().name());
        assertEquals("VALIDATION_FAILED", dead.deadReason().name());
        assertEquals(1, h.eventService.listDeadLetters().size());
    }

    @Test
    void permanentFaultDeadLetterThenReplayAfterFix() {
        TestSupport.Harness h = TestSupport.newHarness();
        OrderService orders = new OrderService(h.eventService, h.txManager, h.dataSource);
        FaultSimulator faults = new FaultSimulator();
        h.register(OrderService.EVENT_TYPE, new OrderCreatedHandler(h.dataSource, faults));

        OrderResult r = orders.placeOrder(new OrderCommand("req-fatal", "tv", 1));
        faults.armPermanent(String.valueOf(r.orderId()));
        String result = h.dispatcher.claimAndProcess(r.eventId());
        log.info("永久故障注入下处理结果={}", result);
        assertEquals("DEAD_LETTER", result);
        assertEquals(0, h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_projection WHERE order_id=?", Integer.class, r.orderId()));

        // 运维修复
        faults.clear(String.valueOf(r.orderId()));
        assertTrue(h.eventService.replayDeadLetter(r.eventId(), "永久故障已修复"));
        h.dispatcher.runOnce();

        Integer projection = h.jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_projection WHERE order_id=?", Integer.class, r.orderId());
        log.info("修复重放后投影数={}（预期 1）", projection);
        assertEquals(1, projection);
    }
}

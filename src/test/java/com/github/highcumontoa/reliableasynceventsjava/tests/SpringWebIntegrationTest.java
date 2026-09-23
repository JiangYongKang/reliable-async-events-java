package com.github.highcumontoa.reliableasynceventsjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 真实 Spring Boot 装配下端到端验证（随机端口 + 内存库 + 快速轮询）：
 * HTTP 下单 -> 调度器自动消费 -> HTTP 查询完成；重复下单幂等；死信可查可重放。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.datasource.url=jdbc:h2:mem:webdb;DB_CLOSE_DELAY=-1",
        "reliable.events.poll-interval=50ms",
        "reliable.events.initial-backoff=20ms",
        "reliable.events.lease-ttl=2s"
})
class SpringWebIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static HttpHeaders json() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    @Test
    void orderLifecycleOverHttpIsEventuallyConsistentAndIdempotent() {
        String requestId = "web-" + System.nanoTime();
        String body = "{\"requestId\":\"" + requestId + "\",\"product\":\"keyboard\",\"quantity\":1}";

        // 第一次下单
        var resp1 = rest.postForEntity(url("/api/orders"),
                new HttpEntity<>(body, json()), Map.class);
        assertEquals(200, resp1.getStatusCode().value());
        Map<?, ?> order1 = resp1.getBody();
        assertNotNull(order1);
        Number eventId = (Number) order1.get("eventId");
        System.out.println("[web-test] 第一次下单返回: " + order1);

        // 重复下单：同一 requestId，返回同一 orderId/eventId，duplicated=true
        var resp2 = rest.postForEntity(url("/api/orders"),
                new HttpEntity<>(body, json()), Map.class);
        Map<?, ?> order2 = resp2.getBody();
        System.out.println("[web-test] 重复下单返回: " + order2);
        assertEquals(Boolean.TRUE, order2.get("duplicated"));
        assertEquals(order1.get("orderId"), order2.get("orderId"));

        // 等待调度器自动处理完成（无需手工触发）
        Map<?, ?> event = waitForStatus(eventId.longValue(), "COMPLETED", 5000);
        System.out.println("[web-test] 事件最终: " + event);
        assertEquals("COMPLETED", event.get("status"));

        // 流转流水可查询
        @SuppressWarnings("unchecked")
        java.util.List<Map<?, ?>> transitions = rest.getForObject(
                url("/api/events/" + eventId + "/transitions"), java.util.List.class);
        transitions.forEach(t -> System.out.println("[web-test] 流水: " + t.get("from")
                + " -> " + t.get("to") + " reason=" + t.get("reason")));
        assertTrue(transitions.size() >= 3);

        // 非法输入 -> 400 + VALIDATION_FAILED 死信可在死信列表中查到
        String badBody = "{\"requestId\":\"" + requestId + "-bad\",\"product\":\"x\",\"quantity\":-3}";
        var bad = rest.postForEntity(url("/api/orders"),
                new HttpEntity<>(badBody, json()), Map.class);
        assertEquals(400, bad.getStatusCode().value());
        assertEquals("VALIDATION_FAILED", bad.getBody().get("error"));

        @SuppressWarnings("unchecked")
        java.util.List<Map<?, ?>> deadLetters = rest.getForObject(url("/api/events"), java.util.List.class);
        System.out.println("[web-test] 死信列表条数=" + deadLetters.size());
        assertTrue(deadLetters.stream().anyMatch(e -> "VALIDATION_FAILED".equals(e.get("deadReason"))));
    }

    private Map<?, ?> waitForStatus(long eventId, String expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            @SuppressWarnings("unchecked")
            Map<?, ?> event = rest.getForObject(url("/api/events/" + eventId), Map.class);
            if (expected.equals(event.get("status")) || "DEAD_LETTER".equals(event.get("status"))) {
                return event;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("事件 " + eventId + " 在 " + timeoutMs + "ms 内未到达 " + expected);
    }
}

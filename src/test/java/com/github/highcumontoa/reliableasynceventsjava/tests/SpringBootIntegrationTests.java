package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.Application;
import com.github.highcumontoa.reliableasynceventsjava.events.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spring 端到端集成：REST 提交 -> 后台分发器自动消费/重试/死信 -> REST 查询/重放。
 * 使用真实线程与本地 WAL，整个过程不依赖外部中间件或网络。
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestEventHandlers.class)
class SpringBootIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(SpringBootIntegrationTests.class);
    private static final Path STORE_DIR;

    static {
        try {
            STORE_DIR = Files.createTempDirectory("reliable-events-it-");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("reliable-events.store-dir", () -> STORE_DIR.toString());
        registry.add("reliable-events.max-attempts", () -> 3);
        registry.add("reliable-events.base-backoff-millis", () -> 5);
        registry.add("reliable-events.max-backoff-millis", () -> 20);
        registry.add("reliable-events.jitter-millis", () -> 0);
        registry.add("reliable-events.poll-interval-millis", () -> 2);
        registry.add("reliable-events.worker-count", () -> 3);
        registry.add("reliable-events.lease-millis", () -> 30000);
    }

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate rest;
    @Autowired
    EventStore store;
    @Autowired
    BusinessStateStore businessState;

    private String url(String path) {
        return "http://localhost:" + port + "/api/events" + path;
    }

    private String postJson(String path, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url(path), new HttpEntity<>(json, headers), String.class).getBody();
    }

    @Test
    void endToEndPublishConsumeDeadLetterReplay() throws Exception {
        // 1) 成功事件：业务写入与事件同批落盘，后台自动消费
        String okJson = "{\"idempotencyKey\":\"web-ok\",\"type\":\"WEB_OK\",\"payload\":{\"n\":1},"
                + "\"businessState\":{\"biz:web-ok\":\"CREATED\"}}";
        String body = postJson("", okJson);
        log.info("【输入】REST 提交成功事件 web-ok，响应={}", body);
        assertTrue(body.contains("\"duplicate\":false"));

        // 2) 永久故障事件 -> 死信
        postJson("", "{\"idempotencyKey\":\"web-dl\",\"type\":\"WEB_FATAL\",\"payload\":{}}");
        log.info("【输入】REST 提交永久故障事件 web-dl（WEB_FATAL 处理器抛 NonRetryableEventException）");

        // 3) 非法 JSON 提交被 400 拒绝，不落事件
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        var bad = rest.postForEntity(url(""), new HttpEntity<>("{not-json", h), Map.class);
        assertEquals(400, bad.getStatusCode().value());
        log.info("【判定依据】非法 JSON 提交在 HTTP 解析层即返回 400，不产生任何事件: {}", bad.getBody());

        // 等待后台处理
        waitFor(() -> "CREATED".equals(businessState.get("biz:web-ok").orElse(null))
                && store.findByIdempotencyKey("web-ok").orElseThrow().status() == EventStatus.SUCCEEDED, 5000);
        waitFor(() -> store.findByIdempotencyKey("web-dl").orElseThrow().status() == EventStatus.DEAD_LETTER, 5000);

        EventRecord dl = store.findByIdempotencyKey("web-dl").orElseThrow();
        assertEquals(DeadLetterReason.NON_RETRYABLE, dl.deadLetterReason());
        assertEquals("yes", businessState.get("consumed:web-ok").orElseThrow());
        log.info("【状态变化】web-ok=SUCCEEDED(biz:web-ok=CREATED, 消费副作用 consumed:web-ok=yes)；web-dl=DEAD_LETTER[{}]",
                dl.deadLetterReason());

        // 幂等：重复提交返回 duplicate=true
        String dup = postJson("", "{\"idempotencyKey\":\"web-ok\",\"type\":\"WEB_OK\",\"payload\":{\"n\":2}}");
        assertTrue(dup.contains("\"duplicate\":true"));
        log.info("【判定依据】重复 REST 提交 web-ok 返回 duplicate=true，不产生第二条事件");

        // 死信列表可查询
        String list = rest.getForEntity(url("/dead-letters"), String.class).getBody();
        assertTrue(list.contains(dl.eventId()));

        // 重放：置回 PENDING；处理器仍永久失败，后台再次转入死信（流转与原因可解释）
        String replayResp = postJson("/" + dl.eventId() + "/replay", "");
        assertTrue(replayResp.contains("PENDING"));
        waitFor(() -> store.findByEventId(dl.eventId()).orElseThrow().status() == EventStatus.DEAD_LETTER, 5000);
        log.info("【状态变化】重放 DEAD_LETTER -> PENDING -> (仍永久故障) DEAD_LETTER，原因持续可查询");
    }

    private void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("等待条件超时未满足");
    }

}

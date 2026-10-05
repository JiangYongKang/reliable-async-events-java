package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.Application;
import com.github.highcumontoa.reliableasynceventsjava.events.BusinessStateStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REST 端到端：流转历史查询（单事件时间线 + 跨事件检索分页）与死信批量恢复
 * （发起 / 进度 / 停止）接口。
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestEventHandlers.class)
class SpringBootHistoryRecoveryApiTests {

    private static final Logger log = LoggerFactory.getLogger(SpringBootHistoryRecoveryApiTests.class);
    private static final Path STORE_DIR;

    static {
        try {
            STORE_DIR = Files.createTempDirectory("reliable-events-api-it-");
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
    void historyAndBatchRecoveryEndpoints() throws Exception {
        // 1) 一条成功事件 + 一条永久故障死信
        postJson("", "{\"idempotencyKey\":\"api-ok\",\"type\":\"WEB_OK\",\"payload\":{\"n\":1}}");
        postJson("", "{\"idempotencyKey\":\"api-dl\",\"type\":\"WEB_FATAL\",\"payload\":{}}");
        log.info("【输入】REST 提交 api-ok(WEB_OK) 与 api-dl(WEB_FATAL 永久故障)");
        waitFor(() -> store.findByIdempotencyKey("api-ok").orElseThrow().status()
                == EventStatus.SUCCEEDED, 5000);
        waitFor(() -> store.findByIdempotencyKey("api-dl").orElseThrow().status()
                == EventStatus.DEAD_LETTER, 5000);
        String okId = store.findByIdempotencyKey("api-ok").orElseThrow().eventId();
        String dlId = store.findByIdempotencyKey("api-dl").orElseThrow().eventId();

        // 2) 单事件完整时间线
        String timeline = rest.getForEntity(url("/" + okId + "/history"), String.class).getBody();
        log.info("【状态变化】api-ok 时间线={}", timeline);
        assertTrue(timeline.contains("\"trigger\":\"SUBMIT\"")
                && timeline.contains("\"trigger\":\"CLAIM\"")
                && timeline.contains("\"trigger\":\"SUCCEED\""), "时间线必须包含提交/领取/成功");
        assertTrue(timeline.contains("\"toStatus\":\"SUCCEEDED\""));

        // 3) 跨事件检索：按结果状态 + 分页限量
        String search = rest.getForEntity(
                url("/history/search?toStatus=SUCCEEDED&limit=1"), String.class).getBody();
        log.info("【状态变化】按 toStatus=SUCCEEDED&limit=1 检索={}", search);
        assertTrue(search.contains("\"limit\":1"), "limit 必须生效（限量返回）");
        assertTrue(search.contains("\"total\":"), "响应必须携带总数用于分页");
        String byType = rest.getForEntity(
                url("/history/search?type=WEB_OK&trigger=SUBMIT"), String.class).getBody();
        assertTrue(byType.contains(okId), "按类型+触发来源检索必须命中 api-ok 的提交记录");

        // 4) 批量恢复：按类型筛选发起 -> 进度可查 -> 根因未修复再次死信
        String started = postJson("/dead-letters/recovery",
                "{\"type\":\"WEB_FATAL\",\"concurrency\":1,\"ratePerSecond\":50}");
        log.info("【输入】发起批量恢复(WEB_FATAL, concurrency=1, rate=50/s)，响应={}", started);
        assertTrue(started.contains("recoveryId"));
        String recoveryId = started.replaceAll(".*\"recoveryId\":\"([^\"]+)\".*", "$1");
        waitFor(() -> {
            String p = rest.getForEntity(url("/dead-letters/recovery/" + recoveryId), String.class).getBody();
            return p.contains("\"status\":\"COMPLETED\"");
        }, 10_000);
        String progress = rest.getForEntity(url("/dead-letters/recovery/" + recoveryId), String.class).getBody();
        log.info("【状态变化】批量恢复进度={}", progress);
        assertTrue(progress.contains("\"total\":1"), "筛选应命中 1 条 WEB_FATAL 死信");
        assertTrue(progress.contains("\"deadAgain\":1"), "根因未修复，应再次进入死信");
        assertEquals(EventStatus.DEAD_LETTER, store.findByEventId(dlId).orElseThrow().status());

        // 5) 重放来源写入历史；停止接口对已完成任务安全
        String dlTimeline = rest.getForEntity(url("/" + dlId + "/history"), String.class).getBody();
        assertTrue(dlTimeline.contains("BATCH_RECOVERY"), "批量恢复的重放来源必须写入历史");
        String stopped = postJson("/dead-letters/recovery/" + recoveryId + "/stop", "");
        assertTrue(stopped.contains(recoveryId), "对已完成的任务请求停止应安全返回当前进度");
        log.info("【判定依据】时间线含 SUBMIT/CLAIM/SUCCEED；检索 limit/total 生效；"
                + "批量恢复进度 total=1 deadAgain=1；历史含 BATCH_RECOVERY 来源");
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

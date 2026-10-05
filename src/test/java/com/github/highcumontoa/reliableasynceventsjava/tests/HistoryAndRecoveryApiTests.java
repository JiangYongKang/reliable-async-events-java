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
 * REST 层集成：流转历史查询（单事件时间线 + 跨事件检索）与批量死信恢复
 * （发起 -> 进度 -> 完成）通过 HTTP 接口端到端验证。
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestEventHandlers.class)
class HistoryAndRecoveryApiTests {

    private static final Logger log = LoggerFactory.getLogger(HistoryAndRecoveryApiTests.class);
    private static final Path STORE_DIR;

    static {
        try {
            STORE_DIR = Files.createTempDirectory("reliable-events-history-it-");
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

    private String url(String path) {
        return "http://localhost:" + port + "/api/events" + path;
    }

    private String postJson(String path, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url(path), new HttpEntity<>(json, headers), String.class).getBody();
    }

    @Test
    void historyAndBatchRecoveryOverRest() throws Exception {
        // 1) 一条成功事件 + 两条永久故障事件（进死信）
        postJson("", "{\"idempotencyKey\":\"api-ok\",\"type\":\"WEB_OK\",\"payload\":{}}");
        postJson("", "{\"idempotencyKey\":\"api-dl-1\",\"type\":\"WEB_FATAL\",\"payload\":{}}");
        postJson("", "{\"idempotencyKey\":\"api-dl-2\",\"type\":\"WEB_FATAL\",\"payload\":{}}");
        log.info("【输入】REST 提交 1 条成功事件(api-ok) + 2 条永久故障事件(api-dl-1/2)");
        waitFor(() -> store.findByIdempotencyKey("api-ok").orElseThrow().status() == EventStatus.SUCCEEDED
                && store.findDeadLetters().size() == 2, 5000);

        // 2) 单事件时间线：SUBMITTED -> CLAIMED -> SUCCEEDED
        String okId = store.findByIdempotencyKey("api-ok").orElseThrow().eventId();
        String timeline = rest.getForEntity(url("/" + okId + "/history"), String.class).getBody();
        assertTrue(timeline.contains("SUBMITTED") && timeline.contains("CLAIMED")
                && timeline.contains("SUCCEEDED"), "时间线必须包含完整生命周期: " + timeline);
        log.info("【状态变化】GET /{eventId}/history 返回 api-ok 时间线={}", timeline);

        // 3) 跨事件检索：按动作=DEAD_LETTER 过滤，分页 limit=1
        String deadPage = rest.getForEntity(
                url("/history?action=DEAD_LETTER&offset=0&limit=1"), String.class).getBody();
        assertTrue(deadPage.contains("\"totalCount\":2"), "死信流转总数=2: " + deadPage);
        assertTrue(deadPage.contains("DEAD_LETTER"));
        log.info("【状态变化】GET /history?action=DEAD_LETTER&limit=1 分页结果={}", deadPage);

        // 4) 批量恢复：按类型筛选 WEB_FATAL，限速 50/s 并发 2
        String recoverResp = postJson("/dead-letters/recover",
                "{\"eventType\":\"WEB_FATAL\",\"ratePerSecond\":50,\"concurrency\":2}");
        assertTrue(recoverResp.contains("jobId"), "响应必须携带任务 ID: " + recoverResp);
        String jobId = recoverResp.replaceAll(".*\"jobId\":\"([^\"]+)\".*", "$1");
        log.info("【输入】POST /dead-letters/recover 按类型筛选 WEB_FATAL，jobId={}", jobId);

        // 5) 轮询进度直到完成（WEB_FATAL 处理器仍失败，重放后会再次死信，但重放动作本身完成）
        waitFor(() -> {
            String p = rest.getForEntity(url("/recoveries/" + jobId), String.class).getBody();
            return p.contains("COMPLETED");
        }, 5000);
        String progress = rest.getForEntity(url("/recoveries/" + jobId), String.class).getBody();
        assertTrue(progress.contains("\"replayed\":2"), "两条死信都应被重放: " + progress);
        log.info("【状态变化】GET /recoveries/{jobId} 最终进度={}", progress);

        // 6) 重放后历史中出现 batch-recovery 触发来源；事件被后台再次消费并回到死信
        String dl1 = store.findByIdempotencyKey("api-dl-1").orElseThrow().eventId();
        waitFor(() -> store.historyOf(dl1).stream()
                .anyMatch(r -> r.action() == TransitionAction.REPLAYED
                        && r.trigger().equals("batch-recovery:" + jobId)), 5000);
        waitFor(() -> store.findByEventId(dl1).orElseThrow().status() == EventStatus.DEAD_LETTER, 5000);
        String dlTimeline = rest.getForEntity(url("/" + dl1 + "/history"), String.class).getBody();
        assertTrue(dlTimeline.contains("REPLAYED") && dlTimeline.contains("batch-recovery"));
        log.info("【判定依据】api-dl-1 时间线含 REPLAYED(trigger=batch-recovery:{}) 且最终回到死信 => "
                + "恢复链路、历史记录、状态机三者一致", jobId);

        // 7) 停止接口幂等：对已完成的任务 stop 返回当前进度而不是报错
        String stopResp = postJson("/recoveries/" + jobId + "/stop", "");
        assertTrue(stopResp.contains("jobId"));
        log.info("【判定依据】对已完成任务 stop 幂等返回进度={}", stopResp);
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

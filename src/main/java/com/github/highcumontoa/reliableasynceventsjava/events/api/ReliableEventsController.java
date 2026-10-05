package com.github.highcumontoa.reliableasynceventsjava.events.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterReason;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterRecoveryService;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterService;
import com.github.highcumontoa.reliableasynceventsjava.events.EventException;
import com.github.highcumontoa.reliableasynceventsjava.events.EventPublisher;
import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventValidationException;
import com.github.highcumontoa.reliableasynceventsjava.events.RecoveryProgress;
import com.github.highcumontoa.reliableasynceventsjava.events.RecoveryRequest;
import com.github.highcumontoa.reliableasynceventsjava.events.SubmissionResult;
import com.github.highcumontoa.reliableasynceventsjava.events.TransitionAction;
import com.github.highcumontoa.reliableasynceventsjava.events.TransitionPage;
import com.github.highcumontoa.reliableasynceventsjava.events.TransitionQuery;
import com.github.highcumontoa.reliableasynceventsjava.events.TransitionRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 演示用 REST 接口：提交事件、查询状态、列出/重放死信。
 */
@RestController
@RequestMapping("/api/events")
public class ReliableEventsController {

    private final EventPublisher publisher;
    private final DeadLetterService deadLetterService;
    private final DeadLetterRecoveryService recoveryService;
    private final EventStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    public ReliableEventsController(EventPublisher publisher, DeadLetterService deadLetterService,
                                    DeadLetterRecoveryService recoveryService, EventStore store) {
        this.publisher = publisher;
        this.deadLetterService = deadLetterService;
        this.recoveryService = recoveryService;
        this.store = store;
    }

    public record PublishRequest(String idempotencyKey, String type, JsonNode payload,
                                 Map<String, String> businessState) {
    }

    @PostMapping
    public ResponseEntity<SubmissionResult> publish(@RequestBody PublishRequest request) {
        if (request == null || request.payload() == null) {
            throw new EventValidationException("提交被拒绝：payload 不能为空");
        }
        String payload;
        try {
            payload = mapper.writeValueAsString(request.payload());
        } catch (Exception e) {
            throw new EventValidationException("提交被拒绝：payload 无法序列化: " + e.getMessage());
        }
        SubmissionResult result = publisher.publish(request.idempotencyKey(), request.type(),
                payload, request.businessState());
        return ResponseEntity.status(result.duplicate() ? HttpStatus.OK : HttpStatus.CREATED).body(result);
    }

    @GetMapping("/{eventId}")
    public EventRecord getByEventId(@PathVariable String eventId) {
        return store.findByEventId(eventId)
                .orElseThrow(() -> new EventException("事件不存在: " + eventId));
    }

    @GetMapping("/by-key/{idempotencyKey}")
    public EventRecord getByIdempotencyKey(@PathVariable String idempotencyKey) {
        return store.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new EventException("幂等键不存在: " + idempotencyKey));
    }

    @GetMapping("/dead-letters")
    public List<EventRecord> deadLetters() {
        return deadLetterService.list();
    }

    @PostMapping("/{eventId}/replay")
    public EventRecord replay(@PathVariable String eventId) {
        return deadLetterService.replay(eventId);
    }

    // ------------------------------------------------------------------ 流转历史

    /** 单个事件的完整流转时间线（提交 -> 领取/重试/成功/死信/重放 ...）。 */
    @GetMapping("/{eventId}/history")
    public List<TransitionRecord> historyOf(@PathVariable String eventId) {
        store.findByEventId(eventId)
                .orElseThrow(() -> new EventException("事件不存在: " + eventId));
        return store.historyOf(eventId);
    }

    /** 跨事件检索流转历史：按类型 / 动作 / 目标状态 / 时间窗口过滤，分页返回。 */
    @GetMapping("/history")
    public TransitionPage searchHistory(@RequestParam(required = false) String type,
                                        @RequestParam(required = false) TransitionAction action,
                                        @RequestParam(required = false) EventStatus toStatus,
                                        @RequestParam(required = false) Long fromMillis,
                                        @RequestParam(required = false) Long toMillis,
                                        @RequestParam(defaultValue = "0") int offset,
                                        @RequestParam(defaultValue = "100") int limit) {
        return store.searchTransitions(new TransitionQuery(
                type, action, toStatus, fromMillis, toMillis, offset, limit));
    }

    // ------------------------------------------------------------------ 批量恢复

    public record RecoverRequestBody(List<String> eventIds, String eventType, DeadLetterReason reason,
                                     Long deadLetteredBeforeMillis, Double ratePerSecond,
                                     Integer concurrency) {
    }

    public record RecoverResponse(String jobId, RecoveryProgress progress) {
    }

    /** 发起批量死信恢复：按条件筛选或显式事件列表，受控速率/并发。 */
    @PostMapping("/dead-letters/recover")
    public ResponseEntity<RecoverResponse> startRecovery(@RequestBody RecoverRequestBody body) {
        RecoveryRequest request = new RecoveryRequest(
                body == null ? null : body.eventIds(),
                body == null ? null : body.eventType(),
                body == null ? null : body.reason(),
                body == null ? null : body.deadLetteredBeforeMillis(),
                body == null || body.ratePerSecond() == null ? 10.0 : body.ratePerSecond(),
                body == null || body.concurrency() == null ? 2 : body.concurrency());
        String jobId = recoveryService.startRecovery(request);
        RecoveryProgress progress = recoveryService.progress(jobId).orElseThrow();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new RecoverResponse(jobId, progress));
    }

    /** 查询批量恢复任务进度。 */
    @GetMapping("/recoveries/{jobId}")
    public RecoveryProgress recoveryProgress(@PathVariable String jobId) {
        return recoveryService.progress(jobId)
                .orElseThrow(() -> new EventException("恢复任务不存在: " + jobId));
    }

    /** 中途停止批量恢复任务（幂等）。 */
    @PostMapping("/recoveries/{jobId}/stop")
    public RecoveryProgress stopRecovery(@PathVariable String jobId) {
        return recoveryService.stop(jobId)
                .orElseThrow(() -> new EventException("恢复任务不存在: " + jobId));
    }

    @ExceptionHandler(EventValidationException.class)
    public ResponseEntity<Map<String, String>> handleValidation(EventValidationException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "errorType", "VALIDATION_FAILED", "message", e.getMessage()));
    }

    @ExceptionHandler(EventException.class)
    public ResponseEntity<Map<String, String>> handleEvent(EventException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "errorType", "EVENT_EXCEPTION", "message", e.getMessage()));
    }
}

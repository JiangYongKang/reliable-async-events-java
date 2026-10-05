package com.github.highcumontoa.reliableasynceventsjava.events.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterReason;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterService;
import com.github.highcumontoa.reliableasynceventsjava.events.EventException;
import com.github.highcumontoa.reliableasynceventsjava.events.EventPublisher;
import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventValidationException;
import com.github.highcumontoa.reliableasynceventsjava.events.SubmissionResult;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventHistoryStore;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventTransition;
import com.github.highcumontoa.reliableasynceventsjava.events.history.HistoryQuery;
import com.github.highcumontoa.reliableasynceventsjava.events.history.TransitionTrigger;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.DeadLetterRecoveryService;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.RecoveryProgress;
import com.github.highcumontoa.reliableasynceventsjava.events.recovery.RecoveryRequest;
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
 * 演示用 REST 接口：提交事件、查询状态、流转历史检索、列出/重放死信、批量恢复死信。
 */
@RestController
@RequestMapping("/api/events")
public class ReliableEventsController {

    private final EventPublisher publisher;
    private final DeadLetterService deadLetterService;
    private final EventStore store;
    private final EventHistoryStore historyStore;
    private final DeadLetterRecoveryService recoveryService;
    private final ObjectMapper mapper = new ObjectMapper();

    public ReliableEventsController(EventPublisher publisher, DeadLetterService deadLetterService,
                                    EventStore store, EventHistoryStore historyStore,
                                    DeadLetterRecoveryService recoveryService) {
        this.publisher = publisher;
        this.deadLetterService = deadLetterService;
        this.store = store;
        this.historyStore = historyStore;
        this.recoveryService = recoveryService;
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

    // ---------------------------------------------------------------- 流转历史

    /** 单个事件的完整流转时间线。 */
    @GetMapping("/{eventId}/history")
    public List<EventTransition> historyOf(@PathVariable String eventId) {
        return historyStore.historyOf(eventId);
    }

    /** 跨事件历史检索：按类型 / 结果状态 / 触发来源 / 时间窗口过滤，offset+limit 分页（limit 上限 1000）。 */
    @GetMapping("/history/search")
    public Map<String, Object> searchHistory(
            @RequestParam(required = false) String type,
            @RequestParam(required = false) EventStatus toStatus,
            @RequestParam(required = false) TransitionTrigger trigger,
            @RequestParam(required = false) Long from,
            @RequestParam(required = false) Long to,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "100") int limit) {
        HistoryQuery q = new HistoryQuery(type, toStatus, trigger, from, to, offset, limit);
        return Map.of(
                "total", historyStore.count(q),
                "offset", q.offset(),
                "limit", q.limit(),
                "records", historyStore.query(q));
    }

    // ---------------------------------------------------------------- 批量恢复

    public record RecoveryHttpRequest(List<String> eventIds, String type, DeadLetterReason reason,
                                      Long deadLetteredBeforeMillis, Integer concurrency,
                                      Double ratePerSecond) {
    }

    /** 发起批量恢复：按条件筛选或显式事件列表，可控制并发与速率。 */
    @PostMapping("/dead-letters/recovery")
    public ResponseEntity<Map<String, String>> startRecovery(@RequestBody RecoveryHttpRequest body) {
        RecoveryRequest request = new RecoveryRequest(
                body.eventIds(), body.type(), body.reason(), body.deadLetteredBeforeMillis(),
                body.concurrency() == null ? 0 : body.concurrency(),
                body.ratePerSecond() == null ? 0 : body.ratePerSecond());
        String recoveryId = recoveryService.startRecovery(request);
        return ResponseEntity.accepted().body(Map.of("recoveryId", recoveryId));
    }

    @GetMapping("/dead-letters/recovery/{recoveryId}")
    public RecoveryProgress recoveryProgress(@PathVariable String recoveryId) {
        RecoveryProgress p = recoveryService.progress(recoveryId);
        if (p == null) {
            throw new EventException("恢复任务不存在: " + recoveryId);
        }
        return p;
    }

    @PostMapping("/dead-letters/recovery/{recoveryId}/stop")
    public RecoveryProgress stopRecovery(@PathVariable String recoveryId) {
        try {
            recoveryService.stop(recoveryId);
        } catch (IllegalArgumentException e) {
            throw new EventException(e.getMessage());
        }
        return recoveryService.progress(recoveryId);
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

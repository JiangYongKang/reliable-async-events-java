package com.github.highcumontoa.reliableasynceventsjava.events.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterService;
import com.github.highcumontoa.reliableasynceventsjava.events.EventException;
import com.github.highcumontoa.reliableasynceventsjava.events.EventPublisher;
import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventValidationException;
import com.github.highcumontoa.reliableasynceventsjava.events.SubmissionResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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
    private final EventStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    public ReliableEventsController(EventPublisher publisher, DeadLetterService deadLetterService,
                                    EventStore store) {
        this.publisher = publisher;
        this.deadLetterService = deadLetterService;
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

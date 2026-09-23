package com.github.highcumontoa.reliableasynceventsjava.web;

import com.github.highcumontoa.reliableasynceventsjava.core.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.core.EventTransition;
import com.github.highcumontoa.reliableasynceventsjava.engine.ReliableEventService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 查询与运维接口：事件详情、流转流水、死信列表、死信重放。
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final ReliableEventService eventService;

    public EventController(ReliableEventService eventService) {
        this.eventService = eventService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getEvent(@PathVariable long id) {
        return eventService.getEvent(id)
                .map(e -> ResponseEntity.ok(toMap(e)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<Map<String, Object>> listDeadLetters() {
        return eventService.listDeadLetters().stream().map(EventController::toMap).toList();
    }

    @GetMapping("/{id}/transitions")
    public ResponseEntity<List<Map<String, Object>>> transitions(@PathVariable long id) {
        if (eventService.getEvent(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(eventService.getTransitions(id).stream()
                .map(EventController::toTransitionMap).toList());
    }

    @PostMapping("/{id}/replay")
    public Map<String, Object> replay(@PathVariable long id,
                                      @RequestBody(required = false) Map<String, String> body) {
        String note = body == null ? "运维重放" : body.getOrDefault("note", "运维重放");
        boolean ok = eventService.replayDeadLetter(id, note);
        return Map.of("eventId", id, "replayed", ok, "status", ok ? "PENDING" : "UNCHANGED");
    }

    static Map<String, Object> toMap(EventRecord e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", e.id());
        m.put("idempotencyKey", e.idempotencyKey());
        m.put("type", e.type());
        m.put("payload", e.payload());
        m.put("status", e.status());
        m.put("deadReason", e.deadReason());
        m.put("errorDetail", e.errorDetail());
        m.put("attempts", e.attempts());
        m.put("maxAttempts", e.maxAttempts());
        m.put("nextAttemptAt", e.nextAttemptAt());
        m.put("claimedAt", e.claimedAt());
        m.put("createdAt", e.createdAt());
        m.put("updatedAt", e.updatedAt());
        return m;
    }

    static Map<String, Object> toTransitionMap(EventTransition t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", t.id());
        m.put("eventId", t.eventId());
        m.put("from", t.fromStatus());
        m.put("to", t.toStatus());
        m.put("reason", t.reason());
        m.put("detail", t.detail());
        m.put("attempts", t.attempts());
        m.put("at", t.createdAt());
        return m;
    }
}

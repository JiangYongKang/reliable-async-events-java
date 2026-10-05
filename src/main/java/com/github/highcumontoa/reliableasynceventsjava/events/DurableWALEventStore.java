package com.github.highcumontoa.reliableasynceventsjava.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventHistoryStore;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventTransition;
import com.github.highcumontoa.reliableasynceventsjava.events.history.HistoryQuery;
import com.github.highcumontoa.reliableasynceventsjava.events.history.TransitionTrigger;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableLog;
import com.github.highcumontoa.reliableasynceventsjava.events.log.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 {@link DurableLog} 的可靠事件存储。
 *
 * <p>全部状态（事件状态机 + 业务副作用 KV）都是 WAL 重放的结果，
 * 任何状态流转都采用“在同一把锁内做条件判断并把结果以单批次 fsync 追加”的方式，
 * 因此在并发与重启下不会出现状态被两个消费者同时推进的情况。</p>
 *
 * <p>WAL 条目类型：</p>
 * <ul>
 *   <li>{@code SUBMIT}: 业务实体写入 + 新事件（同批原子）；</li>
 *   <li>{@code SUCCEED}: 处理成功 + 消费副作用（同批原子）；</li>
 *   <li>{@code RETRY}: 瞬时故障后排定下次重试；</li>
 *   <li>{@code DEAD}: 进入死信（携带区分原因）；</li>
 *   <li>{@code REPLAY}: 死信重放回到 PENDING；</li>
 *   <li>{@code HISTORY}: 流转历史，与对应状态流转同批落盘（重启重放后完整重建）。</li>
 * </ul>
 */
public class DurableWALEventStore implements EventStore, BusinessStateStore, EventHistoryStore {

    private static final Logger log = LoggerFactory.getLogger(DurableWALEventStore.class);

    private static final String KIND_SUBMIT = "SUBMIT";
    private static final String KIND_SUCCEED = "SUCCEED";
    private static final String KIND_RETRY = "RETRY";
    private static final String KIND_DEAD = "DEAD";
    private static final String KIND_REPLAY = "REPLAY";
    private static final String KIND_HISTORY = "HISTORY";

    private final DurableFileLog wal;
    private final RetryPolicy retryPolicy;
    private final long leaseMillis;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();

    // eventId -> 事件最新状态
    private final Map<String, EventRecord> byEventId = new LinkedHashMap<>();
    // idempotencyKey -> eventId
    private final Map<String, String> idempotencyIndex = new LinkedHashMap<>();
    // 业务副作用 KV
    private final Map<String, String> businessState = new LinkedHashMap<>();
    // 流转历史：全局有序列表 + 按事件索引（全部由 WAL 重放得到）
    private final List<EventTransition> history = new ArrayList<>();
    private final Map<String, List<EventTransition>> historyByEvent = new LinkedHashMap<>();

    private final AtomicLong leaseCounter = new AtomicLong();

    public DurableWALEventStore(DurableLog log, RetryPolicy retryPolicy, long leaseMillis) {
        if (!(log instanceof DurableFileLog dfl)) {
            throw new IllegalArgumentException("当前实现需要 DurableFileLog");
        }
        this.wal = dfl;
        this.retryPolicy = retryPolicy;
        this.leaseMillis = leaseMillis;
        recover();
    }

    // ------------------------------------------------------------------ 恢复

    private void recover() {
        List<LogEntry> entries = wal.replayAll();
        for (LogEntry e : entries) {
            switch (e.kind()) {
                case KIND_SUBMIT -> applySubmit(e.body(), false);
                case KIND_SUCCEED -> applySucceed(e.body(), false);
                case KIND_RETRY -> applyRetry(e.body(), false);
                case KIND_DEAD -> applyDead(e.body(), false);
                case KIND_REPLAY -> applyReplay(e.body(), false);
                case KIND_HISTORY -> applyHistory(e.body());
                default -> log.warn("恢复时忽略未知 WAL 条目类型: {}", e.kind());
            }
        }
        long activeLeases = byEventId.values().stream()
                .filter(ev -> ev.status() == EventStatus.PENDING && ev.leaseToken() != 0).count();
        log.info("事件存储恢复完成: 事件总数={}, 死信={}, 业务键={}, 未释放租约(等待超时回收)={}",
                byEventId.size(),
                byEventId.values().stream().filter(ev -> ev.status() == EventStatus.DEAD_LETTER).count(),
                businessState.size(), activeLeases);
    }

    // ------------------------------------------------------------------ 提交

    @Override
    public SubmissionResult commit(String idempotencyKey, String type, String payload,
                                   Map<String, String> entitySideEffects, long nowMillis) {
        validateCommitInput(idempotencyKey, type, payload);
        synchronized (lock) {
            String existingId = idempotencyIndex.get(idempotencyKey);
            if (existingId != null) {
                EventRecord existing = byEventId.get(existingId);
                log.info("提交命中幂等键[{}] -> 返回既有事件 eventId={} status={}（不写入新事件、不重复业务副作用）",
                        idempotencyKey, existing.eventId(), existing.status());
                return SubmissionResult.duplicate(existing);
            }
            EventRecord ev = new EventRecord(
                    UUID.randomUUID().toString(), idempotencyKey, type, payload,
                    EventStatus.PENDING, 0, retryPolicy.maxAttempts(), nowMillis,
                    nowMillis, 0L, 0L, null, null, 0L);

            List<DurableFileLog.Entry> batch = new ArrayList<>();
            if (entitySideEffects != null) {
                for (Map.Entry<String, String> kv : entitySideEffects.entrySet()) {
                    batch.add(new DurableFileLog.Entry(KIND_SUBMIT, json(Map.of(
                            "op", "ENTITY", "key", kv.getKey(), "value", kv.getValue()))));
                }
            }
            batch.add(new DurableFileLog.Entry(KIND_SUBMIT, json(
                    Map.of("op", "EVENT",
                            "eventId", ev.eventId(),
                            "idempotencyKey", ev.idempotencyKey(),
                            "type", ev.type(),
                            "payload", ev.payload(),
                            "maxAttempts", ev.maxAttempts(),
                            "createdAtMillis", ev.createdAtMillis()))));
            // 历史与状态流转同批落盘：提交（null -> PENDING）
            batch.add(new DurableFileLog.Entry(KIND_HISTORY, historyJson(
                    ev.eventId(), ev.type(), null, EventStatus.PENDING, TransitionTrigger.SUBMIT,
                    "提交 idempotencyKey=" + idempotencyKey, 0, nowMillis)));
            wal.appendBatch(batch);
            for (DurableFileLog.Entry e : batch) {
                if (KIND_HISTORY.equals(e.kind())) {
                    applyHistory(e.jsonBody());
                } else {
                    applySubmit(e.jsonBody(), true);
                }
            }
            log.info("新事件已原子提交(业务写入 {} 项 + 事件 1 项, 单批 fsync): eventId={} key={} type={} payload={}",
                    entitySideEffects == null ? 0 : entitySideEffects.size(),
                    ev.eventId(), idempotencyKey, type, payload);
            return SubmissionResult.created(ev);
        }
    }

    private void validateCommitInput(String idempotencyKey, String type, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new EventValidationException("idempotencyKey 不能为空（提交期校验失败：拒绝写入）");
        }
        if (type == null || type.isBlank()) {
            throw new EventValidationException("type 不能为空（提交期校验失败：拒绝写入）");
        }
        if (payload == null) {
            throw new EventValidationException("payload 不能为 null（提交期校验失败：拒绝写入）");
        }
    }

    // ------------------------------------------------------------------ 领取

    @Override
    public Optional<ClaimedEvent> claimNext(long nowMillis) {
        synchronized (lock) {
            Optional<EventRecord> candidate = byEventId.values().stream()
                    .filter(ev -> ev.status() == EventStatus.PENDING || ev.status() == EventStatus.RETRYING)
                    .filter(ev -> ev.leaseToken() == 0L || ev.lastAttemptAtMillis() + leaseMillis <= nowMillis)
                    .filter(ev -> ev.nextAttemptAtMillis() <= nowMillis)
                    .min(Comparator.comparingLong(EventRecord::nextAttemptAtMillis)
                            .thenComparing(EventRecord::eventId));
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            EventRecord ev = candidate.get();
            boolean staleLease = ev.leaseToken() != 0L;
            long token = leaseCounter.incrementAndGet();
            int newAttemptCount = ev.attemptCount() + 1;
            String claimDetail = staleLease
                    ? "第 " + newAttemptCount + " 次尝试, lease=" + token + "（旧租约超时回收）"
                    : "第 " + newAttemptCount + " 次尝试, lease=" + token;
            String claimHistory = historyJson(
                    ev.eventId(), ev.type(), ev.status(), EventStatus.PENDING,
                    TransitionTrigger.CLAIM, claimDetail, newAttemptCount, nowMillis);
            wal.appendBatch(List.of(
                    new DurableFileLog.Entry(KIND_RETRY, json(Map.of(
                            "op", "CLAIM", "eventId", ev.eventId(), "leaseToken", token,
                            "attemptCount", newAttemptCount, "at", nowMillis,
                            "staleLease", staleLease))),
                    new DurableFileLog.Entry(KIND_HISTORY, claimHistory)));
            EventRecord claimed = new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                    EventStatus.PENDING, newAttemptCount, ev.maxAttempts(), ev.createdAtMillis(),
                    ev.nextAttemptAtMillis(), nowMillis, token, ev.lastError(),
                    ev.deadLetterReason(), ev.deadLetterAtMillis());
            byEventId.put(ev.eventId(), claimed);
            applyHistory(claimHistory);
            if (staleLease) {
                log.warn("事件 {} 旧租约 {} 已超时(lease={}ms)被回收，新租约={}，第 {} 次尝试（判定依据：上次领取后 {}ms 未终态落盘）",
                        ev.eventId(), ev.leaseToken(), leaseMillis, token,
                        newAttemptCount, nowMillis - ev.lastAttemptAtMillis());
            } else {
                log.info("事件已领取: eventId={} lease={} 第 {}/{} 次尝试 (原状态={})",
                        ev.eventId(), token, newAttemptCount, ev.maxAttempts(), ev.status());
            }
            return Optional.of(new ClaimedEvent(claimed, token));
        }
    }

    // ------------------------------------------------------------------ 成功

    @Override
    public void markSucceeded(String eventId, long leaseToken, Map<String, String> sideEffects, long nowMillis) {
        synchronized (lock) {
            EventRecord ev = requireEvent(eventId);
            if (ev.leaseToken() != leaseToken) {
                throw new OptimisticLockingFailureException(
                        "markSucceeded 租约不匹配: eventId=" + eventId + " expected=" + ev.leaseToken()
                                + " actual=" + leaseToken + "（当前 status=" + ev.status() + "，该结果被丢弃）");
            }
            List<DurableFileLog.Entry> batch = new ArrayList<>();
            if (sideEffects != null) {
                for (Map.Entry<String, String> kv : sideEffects.entrySet()) {
                    batch.add(new DurableFileLog.Entry(KIND_SUCCEED, json(Map.of(
                            "op", "ENTITY", "key", kv.getKey(), "value", kv.getValue()))));
                }
            }
            batch.add(new DurableFileLog.Entry(KIND_SUCCEED, json(Map.of(
                    "op", "SUCCEEDED", "eventId", eventId, "at", nowMillis))));
            batch.add(new DurableFileLog.Entry(KIND_HISTORY, historyJson(
                    eventId, ev.type(), EventStatus.PENDING, EventStatus.SUCCEEDED,
                    TransitionTrigger.SUCCEED, "处理成功（第 " + ev.attemptCount() + " 次尝试）",
                    ev.attemptCount(), nowMillis)));
            wal.appendBatch(batch);
            for (DurableFileLog.Entry e : batch) {
                if (KIND_HISTORY.equals(e.kind())) {
                    applyHistory(e.jsonBody());
                } else {
                    applySucceed(e.jsonBody(), true);
                }
            }
            log.info("事件处理成功并原子落盘(消费副作用 {} 项 + SUCCEEDED 同批 fsync): eventId={} 尝试次数={}",
                    sideEffects == null ? 0 : sideEffects.size(), eventId, ev.attemptCount());
        }
    }

    // ------------------------------------------------------------------ 重试/死信

    @Override
    public void scheduleRetry(String eventId, long leaseToken, String errorMessage,
                              long nextAttemptAtMillis, long nowMillis) {
        synchronized (lock) {
            EventRecord ev = requireEvent(eventId);
            if (ev.leaseToken() != leaseToken) {
                throw new OptimisticLockingFailureException(
                        "scheduleRetry 租约不匹配: eventId=" + eventId + "（该结果被丢弃）");
            }
            int attempts = ev.attemptCount();
            if (attempts >= ev.maxAttempts()) {
                appendDead(ev, DeadLetterReason.RETRIES_EXHAUSTED,
                        "瞬时故障重试次数已耗尽(" + attempts + "/" + ev.maxAttempts() + "): " + errorMessage,
                        nowMillis);
            } else {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("op", "WAIT");
                m.put("eventId", eventId);
                m.put("status", EventStatus.RETRYING.name());
                m.put("nextAttemptAtMillis", nextAttemptAtMillis);
                m.put("error", truncate(errorMessage));
                m.put("at", nowMillis);
                String op = json(m);
                String history = historyJson(eventId, ev.type(), EventStatus.PENDING, EventStatus.RETRYING,
                        TransitionTrigger.RETRY_SCHEDULED,
                        "瞬时故障(" + attempts + "/" + ev.maxAttempts() + "): " + truncate(errorMessage)
                                + ", nextAttemptAt=+" + (nextAttemptAtMillis - nowMillis) + "ms",
                        attempts, nowMillis);
                wal.appendBatch(List.of(
                        new DurableFileLog.Entry(KIND_RETRY, op),
                        new DurableFileLog.Entry(KIND_HISTORY, history)));
                applyRetry(op, true);
                applyHistory(history);
                log.info("瞬时故障，进入重试等待: eventId={} 已尝试 {}/{} nextAttemptAt=+{}ms 原因={}",
                        eventId, attempts, ev.maxAttempts(),
                        nextAttemptAtMillis - nowMillis, truncate(errorMessage));
            }
        }
    }

    @Override
    public void markDeadLetter(String eventId, long leaseToken, DeadLetterReason reason,
                               String errorMessage, long nowMillis) {
        synchronized (lock) {
            EventRecord ev = requireEvent(eventId);
            if (ev.leaseToken() != leaseToken) {
                throw new OptimisticLockingFailureException(
                        "markDeadLetter 租约不匹配: eventId=" + eventId + "（该结果被丢弃）");
            }
            appendDead(ev, reason, errorMessage, nowMillis);
        }
    }

    private void appendDead(EventRecord ev, DeadLetterReason reason, String errorMessage, long nowMillis) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", ev.eventId());
        m.put("reason", reason.name());
        m.put("error", truncate(errorMessage));
        m.put("at", nowMillis);
        String op = json(m);
        String history = historyJson(ev.eventId(), ev.type(), EventStatus.PENDING, EventStatus.DEAD_LETTER,
                TransitionTrigger.DEAD_LETTER,
                reason + ": " + truncate(errorMessage), ev.attemptCount(), nowMillis);
        wal.appendBatch(List.of(
                new DurableFileLog.Entry(KIND_DEAD, op),
                new DurableFileLog.Entry(KIND_HISTORY, history)));
        applyDead(op, true);
        applyHistory(history);
        log.warn("事件进入死信[{}]: eventId={} 尝试次数={} 判定依据={}",
                reason, ev.eventId(), ev.attemptCount(), truncate(errorMessage));
    }

    // ------------------------------------------------------------------ 重放

    @Override
    public EventRecord replay(String eventId, String source, long nowMillis) {
        synchronized (lock) {
            EventRecord ev = requireEvent(eventId);
            if (ev.status() != EventStatus.DEAD_LETTER) {
                throw new EventException("仅死信事件可重放: eventId=" + eventId + " 当前状态=" + ev.status());
            }
            String src = source == null || source.isBlank() ? "MANUAL" : source;
            String op = json(Map.of("eventId", eventId, "at", nowMillis));
            String history = historyJson(eventId, ev.type(), EventStatus.DEAD_LETTER, EventStatus.PENDING,
                    TransitionTrigger.REPLAY, "重放来源=" + src + "（原死信原因=" + ev.deadLetterReason() + "）",
                    0, nowMillis);
            wal.appendBatch(List.of(
                    new DurableFileLog.Entry(KIND_REPLAY, op),
                    new DurableFileLog.Entry(KIND_HISTORY, history)));
            applyReplay(op, true);
            applyHistory(history);
            EventRecord after = byEventId.get(eventId);
            log.info("死信重放: eventId={} 原死信原因={} 来源={} -> PENDING（attemptCount 已重置，不产生重复副作用，成功副作用以 KV 覆盖保证幂等）",
                    eventId, ev.deadLetterReason(), src);
            return after;
        }
    }

    // ------------------------------------------------------------------ 查询

    @Override
    public Optional<EventRecord> findByEventId(String eventId) {
        synchronized (lock) {
            return Optional.ofNullable(byEventId.get(eventId));
        }
    }

    @Override
    public Optional<EventRecord> findByIdempotencyKey(String idempotencyKey) {
        synchronized (lock) {
            String id = idempotencyIndex.get(idempotencyKey);
            return Optional.ofNullable(id == null ? null : byEventId.get(id));
        }
    }

    @Override
    public List<EventRecord> findByStatus(EventStatus status) {
        synchronized (lock) {
            return byEventId.values().stream().filter(ev -> ev.status() == status).toList();
        }
    }

    @Override
    public List<EventRecord> findDeadLetters() {
        synchronized (lock) {
            return byEventId.values().stream()
                    .filter(ev -> ev.status() == EventStatus.DEAD_LETTER)
                    .sorted(Comparator.comparingLong(EventRecord::deadLetterAtMillis))
                    .toList();
        }
    }

    // ------------------------------------------------------------------ 历史查询

    @Override
    public List<EventTransition> historyOf(String eventId) {
        synchronized (lock) {
            return List.copyOf(historyByEvent.getOrDefault(eventId, List.of()));
        }
    }

    @Override
    public List<EventTransition> query(HistoryQuery q) {
        synchronized (lock) {
            return history.stream()
                    .filter(t -> matches(t, q))
                    .skip(q.offset())
                    .limit(q.limit())
                    .toList();
        }
    }

    @Override
    public long count(HistoryQuery q) {
        synchronized (lock) {
            return history.stream().filter(t -> matches(t, q)).count();
        }
    }

    private static boolean matches(EventTransition t, HistoryQuery q) {
        if (q.eventType() != null && !q.eventType().equals(t.eventType())) {
            return false;
        }
        if (q.toStatus() != null && q.toStatus() != t.toStatus()) {
            return false;
        }
        if (q.trigger() != null && q.trigger() != t.trigger()) {
            return false;
        }
        if (q.fromMillis() != null && t.occurredAtMillis() < q.fromMillis()) {
            return false;
        }
        return q.toMillis() == null || t.occurredAtMillis() <= q.toMillis();
    }

    @Override
    public Optional<String> get(String key) {
        synchronized (lock) {
            return Optional.ofNullable(businessState.get(key));
        }
    }

    @Override
    public void flush() {
        wal.flush();
    }

    private EventRecord requireEvent(String eventId) {
        EventRecord ev = byEventId.get(eventId);
        if (ev == null) {
            throw new EventException("事件不存在: " + eventId);
        }
        return ev;
    }

    // ------------------------------------------------------------- WAL 应用

    private void applySubmit(String body, boolean alreadyJson) {
        JsonNode n = parse(body);
        if ("ENTITY".equals(n.path("op").asText())) {
            businessState.put(n.get("key").asText(), n.get("value").asText());
            return;
        }
        long now = n.get("createdAtMillis").asLong();
        EventRecord ev = new EventRecord(
                n.get("eventId").asText(), n.get("idempotencyKey").asText(),
                n.get("type").asText(), n.get("payload").asText(),
                EventStatus.PENDING, 0, n.get("maxAttempts").asInt(),
                now, now, 0L, 0L, null, null, 0L);
        byEventId.put(ev.eventId(), ev);
        idempotencyIndex.put(ev.idempotencyKey(), ev.eventId());
    }

    private void applySucceed(String body, boolean ignored) {
        JsonNode n = parse(body);
        if ("ENTITY".equals(n.path("op").asText())) {
            businessState.put(n.get("key").asText(), n.get("value").asText());
            return;
        }
        String eventId = n.get("eventId").asText();
        EventRecord ev = requireEvent(eventId);
        byEventId.put(eventId, new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                EventStatus.SUCCEEDED, ev.attemptCount(), ev.maxAttempts(), ev.createdAtMillis(),
                ev.nextAttemptAtMillis(), ev.lastAttemptAtMillis(), 0L,
                ev.lastError(), ev.deadLetterReason(), ev.deadLetterAtMillis()));
    }

    private void applyRetry(String body, boolean ignored) {
        JsonNode n = parse(body);
        String op = n.path("op").asText();
        String eventId = n.get("eventId").asText();
        EventRecord ev = requireEvent(eventId);
        if ("CLAIM".equals(op)) {
            long token = n.get("leaseToken").asLong();
            int count = n.get("attemptCount").asInt();
            long at = n.get("at").asLong();
            byEventId.put(eventId, new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                    EventStatus.PENDING, count, ev.maxAttempts(), ev.createdAtMillis(),
                    ev.nextAttemptAtMillis(), at, token, ev.lastError(),
                    ev.deadLetterReason(), ev.deadLetterAtMillis()));
        } else {
            long nextAt = n.get("nextAttemptAtMillis").asLong();
            String error = n.has("error") && !n.get("error").isNull() ? n.get("error").asText() : null;
            byEventId.put(eventId, new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                    EventStatus.RETRYING, ev.attemptCount(), ev.maxAttempts(), ev.createdAtMillis(),
                    nextAt, ev.lastAttemptAtMillis(), 0L, error,
                    ev.deadLetterReason(), ev.deadLetterAtMillis()));
        }
    }

    private void applyDead(String body, boolean ignored) {
        JsonNode n = parse(body);
        String eventId = n.get("eventId").asText();
        EventRecord ev = requireEvent(eventId);
        DeadLetterReason reason = DeadLetterReason.valueOf(n.get("reason").asText());
        String error = n.has("error") && !n.get("error").isNull() ? n.get("error").asText() : null;
        long at = n.get("at").asLong();
        byEventId.put(eventId, new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                EventStatus.DEAD_LETTER, ev.attemptCount(), ev.maxAttempts(), ev.createdAtMillis(),
                ev.nextAttemptAtMillis(), ev.lastAttemptAtMillis(), 0L, error, reason, at));
    }

    private void applyReplay(String body, boolean ignored) {
        JsonNode n = parse(body);
        String eventId = n.get("eventId").asText();
        long at = n.get("at").asLong();
        EventRecord ev = requireEvent(eventId);
        byEventId.put(eventId, new EventRecord(ev.eventId(), ev.idempotencyKey(), ev.type(), ev.payload(),
                EventStatus.PENDING, 0, ev.maxAttempts(), ev.createdAtMillis(),
                at, at, 0L, null, null, 0L));
    }

    // ------------------------------------------------------------- 历史 WAL

    private String historyJson(String eventId, String eventType, EventStatus from, EventStatus to,
                               TransitionTrigger trigger, String detail, int attemptCount, long at) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", eventId);
        m.put("eventType", eventType);
        m.put("from", from == null ? null : from.name());
        m.put("to", to.name());
        m.put("trigger", trigger.name());
        m.put("detail", detail == null ? null : truncate(detail));
        m.put("attemptCount", attemptCount);
        m.put("at", at);
        return json(m);
    }

    private void applyHistory(String body) {
        JsonNode n = parse(body);
        EventStatus from = n.has("from") && !n.get("from").isNull()
                ? EventStatus.valueOf(n.get("from").asText()) : null;
        String detail = n.has("detail") && !n.get("detail").isNull() ? n.get("detail").asText() : null;
        EventTransition t = new EventTransition(
                history.size(),
                n.get("eventId").asText(),
                n.get("eventType").asText(),
                from,
                EventStatus.valueOf(n.get("to").asText()),
                TransitionTrigger.valueOf(n.get("trigger").asText()),
                detail,
                n.get("attemptCount").asInt(),
                n.get("at").asLong());
        history.add(t);
        historyByEvent.computeIfAbsent(t.eventId(), k -> new ArrayList<>()).add(t);
    }

    // --------------------------------------------------------------- helpers

    private JsonNode parse(String s) {
        try {
            return mapper.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException("WAL JSON 解析失败: " + s, e);
        }
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("WAL JSON 序列化失败", e);
        }
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 500 ? s : s.substring(0, 500) + "...(truncated)";
    }
}

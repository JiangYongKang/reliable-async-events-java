package com.github.highcumontoa.reliableasynceventsjava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.core.*;
import com.github.highcumontoa.reliableasynceventsjava.storage.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 面向业务的可靠事件门面：
 * <ul>
 *   <li>{@link #submitWithinTransaction}：在业务事务内登记 outbox，业务与事件同提交同回滚；</li>
 *   <li>同幂等键并发/重复提交只产生一条事件（数据库唯一约束兜底，冲突后返回已有事件）；</li>
 *   <li>{@link #submitStandalone}：独立事务提交纯事件；输入校验失败记录 VALIDATION_FAILED 死信；</li>
 *   <li>查询状态/流转、列出死信、重放死信。</li>
 * </ul>
 */
public class ReliableEventService {

    private static final Logger log = LoggerFactory.getLogger(ReliableEventService.class);

    private final EventRepository repository;
    private final TransactionTemplate newTx;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile int maxAttempts = 5;

    public ReliableEventService(EventRepository repository, PlatformTransactionManager txManager) {
        this.repository = repository;
        this.newTx = new TransactionTemplate(txManager);
        this.newTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * 必须在已开启的业务事务中调用：写事件与业务数据共用同一事务，保证
     * "业务已提交则事件必在；事件不存在则业务必未提交"。
     * 幂等键冲突时底层抛 {@link DuplicateKeyException}，由外层捕获后改查已有记录。
     */
    public SubmitResult submitWithinTransaction(EventEnvelope envelope) {
        validateEnvelope(envelope);
        long id = repository.insert(envelope, maxAttempts);
        log.info("[submit] 事务内登记事件 id={} type={} key={}", id, envelope.type(), envelope.idempotencyKey());
        return new SubmitResult(id, EventStatus.PENDING, true, false);
    }

    /**
     * 独立事务提交纯事件。
     * <p>非法输入不静默丢弃：以 DEAD_LETTER/VALIDATION_FAILED 落库，状态与原因可查询；
     * 合法输入则进入 PENDING。同键重复提交（含重复的非法提交）返回已有事件。</p>
     */
    public SubmitResult submitStandalone(String idempotencyKey, String type, String payload) {
        String key = (idempotencyKey == null || idempotencyKey.isBlank())
                ? "invalid:" + UUID.randomUUID()
                : idempotencyKey.trim();
        String error = validate(type, payload);
        EventEnvelope envelope = new EventEnvelope(key,
                type == null || type.isBlank() ? "UNKNOWN" : type.trim(),
                payload == null ? "" : payload);
        try {
            Long id = newTx.execute(status -> {
                long eventId = repository.insert(envelope, maxAttempts);
                if (error != null) {
                    repository.markDead(eventId, EventStatus.PENDING, DeadReason.VALIDATION_FAILED,
                            error, 0, null);
                    repository.recordTransition(eventId, EventStatus.PENDING, EventStatus.DEAD_LETTER,
                            TransitionReason.VALIDATION_REJECTED, error, 0);
                }
                return eventId;
            });
            EventStatus resultStatus = error == null ? EventStatus.PENDING : EventStatus.DEAD_LETTER;
            log.info("[submit] standalone 事件 id={} key={} status={}{}",
                    id, key, resultStatus, error == null ? "" : " 原因=VALIDATION_FAILED:" + error);
            return new SubmitResult(id == null ? -1 : id, resultStatus, true, false);
        } catch (DuplicateKeyException dup) {
            EventRecord existing = repository.findByIdempotencyKey(key)
                    .orElseThrow(() -> new EventException("幂等键冲突但无法查到已有事件: " + key, dup));
            log.info("[submit] 命中幂等键 key={} 返回已有事件 id={} status={}",
                    key, existing.id(), existing.status());
            return new SubmitResult(existing.id(), existing.status(), false, true);
        }
    }

    /** 记录一条 VALIDATION_FAILED 死信（业务侧输入非法时使用）。若键已存在则返回已有记录。 */
    public SubmitResult recordValidationFailure(String idempotencyKey, String type, String detail) {
        String key = (idempotencyKey == null || idempotencyKey.isBlank())
                ? "invalid:" + UUID.randomUUID()
                : idempotencyKey.trim();
        EventEnvelope envelope = new EventEnvelope(key,
                type == null || type.isBlank() ? "UNKNOWN" : type.trim(), "");
        try {
            Long id = newTx.execute(status -> {
                long eventId = repository.insert(envelope, maxAttempts);
                repository.markDead(eventId, EventStatus.PENDING, DeadReason.VALIDATION_FAILED,
                        detail, 0, null);
                repository.recordTransition(eventId, EventStatus.PENDING, EventStatus.DEAD_LETTER,
                        TransitionReason.VALIDATION_REJECTED, detail, 0);
                return eventId;
            });
            log.info("[submit] 输入校验失败，事件直接死信 id={} key={} detail={}", id, key, detail);
            return new SubmitResult(id == null ? -1 : id, EventStatus.DEAD_LETTER, true, false);
        } catch (DuplicateKeyException dup) {
            EventRecord existing = repository.findByIdempotencyKey(key).orElseThrow();
            return new SubmitResult(existing.id(), existing.status(), false, true);
        }
    }

    public Optional<EventRecord> getEvent(long eventId) {
        return repository.findById(eventId);
    }

    public Optional<EventRecord> findByIdempotencyKey(String key) {
        return repository.findByIdempotencyKey(key);
    }

    public List<EventTransition> getTransitions(long eventId) {
        return repository.findTransitions(eventId);
    }

    public List<EventRecord> listDeadLetters() {
        return repository.findDeadLetters();
    }

    /**
     * 修复死信原因后重放：DEAD_LETTER -> PENDING，清空 attempts/错误/租约。
     * 条件更新保证并发重放只有一个生效；重放不产生重复副作用（消费端有去重表）。
     *
     * @return true=本次重放成功；false=事件不存在或当前不是死信状态
     */
    public boolean replayDeadLetter(long eventId, String note) {
        Boolean ok = newTx.execute(status -> {
            if (repository.replay(eventId, Instant.now())) {
                repository.recordTransition(eventId, EventStatus.DEAD_LETTER, EventStatus.PENDING,
                        TransitionReason.REPLAYED, note == null ? "" : note, 0);
                return true;
            }
            return false;
        });
        log.info("[replay] 死信重放 event={} 结果={} note={}", eventId, ok, note);
        return Boolean.TRUE.equals(ok);
    }

    // ---------- 校验 ----------

    private void validateEnvelope(EventEnvelope envelope) {
        String error = envelope == null ? "事件为空" : validate(envelope.type(), envelope.payload());
        if (error != null) {
            throw new InvalidEventException(error);
        }
        if (envelope.idempotencyKey() == null || envelope.idempotencyKey().isBlank()) {
            throw new InvalidEventException("idempotencyKey 不能为空");
        }
    }

    /** 返回 null 表示通过；否则返回可读的失败原因。 */
    private String validate(String type, String payload) {
        if (type == null || type.isBlank()) {
            return "事件类型 type 为空";
        }
        if (payload == null || payload.isBlank()) {
            return "事件负载 payload 为空";
        }
        try {
            objectMapper.readTree(payload);
        } catch (Exception e) {
            return "payload 不是合法 JSON: " + e.getMessage();
        }
        return null;
    }
}

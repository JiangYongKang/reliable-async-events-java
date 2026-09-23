package com.github.highcumontoa.reliableasynceventsjava.storage;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 事件存储：基于关系数据库的 transactional outbox。
 *
 * <p>所有写方法都假定运行在调用方事务中（与业务写入同提交/同回滚）。
 * 并发安全依赖两点：</p>
 * <ul>
 *   <li>idempotency_key 唯一约束 —— 同键重复提交只会有一条事件；</li>
 *   <li>claim_token + version 乐观条件更新 —— 并发领取/更新只有一个成功。</li>
 * </ul>
 */
public class EventRepository {

    private static final Logger log = LoggerFactory.getLogger(EventRepository.class);

    private static final RowMapper<EventRecord> EVENT_MAPPER = (rs, n) -> new EventRecord(
            rs.getLong("id"),
            rs.getString("idempotency_key"),
            rs.getString("type"),
            rs.getString("payload"),
            EventStatus.valueOf(rs.getString("status")),
            rs.getString("dead_reason") == null ? null : DeadReason.valueOf(rs.getString("dead_reason")),
            rs.getString("error_detail"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getTimestamp("next_attempt_at").toInstant(),
            rs.getString("claim_token"),
            rs.getTimestamp("claimed_at") == null ? null : rs.getTimestamp("claimed_at").toInstant(),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    private static final RowMapper<EventTransition> TRANSITION_MAPPER = (rs, n) -> new EventTransition(
            rs.getLong("id"),
            rs.getLong("event_id"),
            rs.getString("from_status") == null ? null : EventStatus.valueOf(rs.getString("from_status")),
            EventStatus.valueOf(rs.getString("to_status")),
            TransitionReason.valueOf(rs.getString("reason")),
            rs.getString("detail"),
            rs.getInt("attempts"),
            rs.getTimestamp("created_at").toInstant()
    );

    private final JdbcTemplate jdbc;

    public EventRepository(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    // ---------- 写入 ----------

    /**
     * 在当前事务内插入事件（初始一律为 PENDING，再由调用方推进状态）；
     * 幂等键冲突时抛出 {@link DuplicateKeyException}。
     */
    public long insert(EventEnvelope envelope, int maxAttempts) {
        Instant now = Instant.now();
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO outbox_event
                      (idempotency_key, type, payload, status, attempts, max_attempts,
                       next_attempt_at, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, envelope.idempotencyKey());
            ps.setString(2, envelope.type());
            ps.setString(3, envelope.payload());
            ps.setString(4, EventStatus.PENDING.name());
            ps.setInt(5, 0);
            ps.setInt(6, maxAttempts);
            ps.setTimestamp(7, Timestamp.from(now));
            ps.setTimestamp(8, Timestamp.from(now));
            ps.setTimestamp(9, Timestamp.from(now));
            return ps;
        }, kh);
        long id = Objects.requireNonNull(kh.getKey()).longValue();
        recordTransition(id, null, EventStatus.PENDING, TransitionReason.ENQUEUED,
                "type=" + envelope.type() + ", key=" + envelope.idempotencyKey(), 0);
        return id;
    }

    /** 记录一条状态流转流水（当前事务）。 */
    public void recordTransition(long eventId, EventStatus from, EventStatus to,
                                 TransitionReason reason, String detail, int attempts) {
        jdbc.update("""
                        INSERT INTO outbox_event_transition
                          (event_id, from_status, to_status, reason, detail, attempts, created_at)
                        VALUES (?,?,?,?,?,?,?)
                        """,
                eventId,
                from == null ? null : from.name(),
                to.name(),
                reason.name(),
                detail,
                attempts,
                Timestamp.from(Instant.now()));
        log.info("[transition] event={} {} -> {} reason={} attempts={} detail={}",
                eventId, from, to, reason, attempts, detail);
    }

    // ---------- 查询 ----------

    public Optional<EventRecord> findById(long eventId) {
        List<EventRecord> rows = jdbc.query("SELECT * FROM outbox_event WHERE id = ?", EVENT_MAPPER, eventId);
        return rows.stream().findFirst();
    }

    public Optional<EventRecord> findByIdempotencyKey(String key) {
        List<EventRecord> rows = jdbc.query("SELECT * FROM outbox_event WHERE idempotency_key = ?", EVENT_MAPPER, key);
        return rows.stream().findFirst();
    }

    public List<EventRecord> findByStatus(EventStatus status, int limit) {
        return jdbc.query("SELECT * FROM outbox_event WHERE status = ? ORDER BY id LIMIT ?",
                EVENT_MAPPER, status.name(), limit);
    }

    /** 到期可领取的 PENDING 事件。 */
    public List<EventRecord> findDuePending(Instant now, int limit) {
        return jdbc.query("""
                        SELECT * FROM outbox_event
                        WHERE status = 'PENDING' AND next_attempt_at <= ?
                        ORDER BY id LIMIT ?
                        """,
                EVENT_MAPPER, Timestamp.from(now), limit);
    }

    public List<EventRecord> findDeadLetters() {
        return jdbc.query("SELECT * FROM outbox_event WHERE status = 'DEAD_LETTER' ORDER BY id", EVENT_MAPPER);
    }

    public List<EventTransition> findTransitions(long eventId) {
        return jdbc.query("SELECT * FROM outbox_event_transition WHERE event_id = ? ORDER BY id",
                TRANSITION_MAPPER, eventId);
    }

    // ---------- 并发安全的状态推进 ----------

    /**
     * 乐观领取：仅当事件仍为 PENDING 且到期时更新成功。
     * 返回 true 表示当前调用方拿到处理权（并发下只有一个线程成功）。
     */
    public boolean claim(long eventId, String token, Instant now) {
        int rows = jdbc.update("""
                        UPDATE outbox_event
                        SET status='PROCESSING', claim_token=?, claimed_at=?, updated_at=?, version=version+1
                        WHERE id=? AND status='PENDING' AND next_attempt_at <= ?
                        """,
                token, Timestamp.from(now), Timestamp.from(now), eventId, Timestamp.from(now));
        return rows == 1;
    }

    /**
     * 回收崩溃消费者遗留的过期租约：PROCESSING 且 claimed_at 早于 deadline -> PENDING。
     * 返回回收条数。
     */
    public int reclaimStaleLeases(Instant now, Instant leaseDeadline) {
        List<EventRecord> stale = jdbc.query("""
                        SELECT * FROM outbox_event
                        WHERE status='PROCESSING' AND claimed_at < ?
                        """,
                EVENT_MAPPER, Timestamp.from(leaseDeadline));
        int reclaimed = 0;
        for (EventRecord e : stale) {
            int rows = jdbc.update("""
                            UPDATE outbox_event
                            SET status='PENDING', claim_token=NULL, claimed_at=NULL,
                                next_attempt_at=?, updated_at=?, version=version+1
                            WHERE id=? AND status='PROCESSING' AND claim_token=?
                            """,
                    Timestamp.from(now), Timestamp.from(now), e.id(), e.claimToken());
            if (rows == 1) {
                recordTransition(e.id(), EventStatus.PROCESSING, EventStatus.PENDING,
                        TransitionReason.LEASE_STALE_RECLAIMED,
                        "token=" + e.claimToken() + " 租约过期被回收", e.attempts());
                reclaimed++;
            }
        }
        return reclaimed;
    }

    /** 瞬时故障：PROCESSING -> PENDING，安排退避；仅当持有 token 时生效。 */
    public boolean markPendingForRetry(long eventId, int attempts, Instant nextAttemptAt,
                                       String token, String errorDetail) {
        int rows = jdbc.update("""
                        UPDATE outbox_event
                        SET status='PENDING', attempts=?, next_attempt_at=?, error_detail=?,
                            claim_token=NULL, claimed_at=NULL, updated_at=?, version=version+1
                        WHERE id=? AND status='PROCESSING' AND claim_token=?
                        """,
                attempts, Timestamp.from(nextAttemptAt), errorDetail,
                Timestamp.from(Instant.now()), eventId, token);
        return rows == 1;
    }

    /** 处理成功：PROCESSING -> COMPLETED；仅当持有 token 时生效。 */
    public boolean markCompleted(long eventId, String token) {
        int rows = jdbc.update("""
                        UPDATE outbox_event
                        SET status='COMPLETED', claim_token=NULL, claimed_at=NULL,
                            updated_at=?, version=version+1
                        WHERE id=? AND status='PROCESSING' AND claim_token=?
                        """,
                Timestamp.from(Instant.now()), eventId, token);
        return rows == 1;
    }

    /** 进入死信；仅当持有 token（或从 PENDING 直接判死）时生效。 */
    public boolean markDead(long eventId, EventStatus expectedFrom, DeadReason reason,
                            String errorDetail, int attempts, String token) {
        int rows;
        if (token != null) {
            rows = jdbc.update("""
                            UPDATE outbox_event
                            SET status='DEAD_LETTER', dead_reason=?, error_detail=?, attempts=?,
                                claim_token=NULL, claimed_at=NULL, updated_at=?, version=version+1
                            WHERE id=? AND status=? AND claim_token=?
                            """,
                    reason.name(), errorDetail, attempts,
                    Timestamp.from(Instant.now()), eventId, expectedFrom.name(), token);
        } else {
            rows = jdbc.update("""
                            UPDATE outbox_event
                            SET status='DEAD_LETTER', dead_reason=?, error_detail=?, attempts=?,
                                claim_token=NULL, claimed_at=NULL, updated_at=?, version=version+1
                            WHERE id=? AND status=?
                            """,
                    reason.name(), errorDetail, attempts,
                    Timestamp.from(Instant.now()), eventId, expectedFrom.name());
        }
        return rows == 1;
    }

    /** 死信重放：DEAD_LETTER -> PENDING，清空 attempts/错误/租约。仅当当前为 DEAD_LETTER 时生效。 */
    public boolean replay(long eventId, Instant now) {
        int rows = jdbc.update("""
                        UPDATE outbox_event
                        SET status='PENDING', dead_reason=NULL, error_detail=NULL, attempts=0,
                            next_attempt_at=?, claim_token=NULL, claimed_at=NULL,
                            updated_at=?, version=version+1
                        WHERE id=? AND status='DEAD_LETTER'
                        """,
                Timestamp.from(now), Timestamp.from(now), eventId);
        return rows == 1;
    }

    // ---------- 消费端幂等去重 ----------

    /** 若事件未处理过则插入去重标记。返回 true=首次处理（调用方应执行业务副作用）。 */
    public boolean markProcessingIfFirst(long eventId) {
        try {
            jdbc.update("INSERT INTO processed_event (event_id, processed_at) VALUES (?,?)",
                    eventId, Timestamp.from(Instant.now()));
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    /** 处理失败时清除去重标记（与业务副作用同事务回滚时通常无需调用）。 */
    public void clearProcessingMark(long eventId) {
        jdbc.update("DELETE FROM processed_event WHERE event_id = ?", eventId);
    }
}

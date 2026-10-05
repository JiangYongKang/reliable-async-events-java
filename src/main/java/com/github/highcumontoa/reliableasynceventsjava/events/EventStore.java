package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 可靠事件存储（基于预写日志，线程安全，重启可恢复）。
 */
public interface EventStore {

    /**
     * 原子提交：同一磁盘批次内写入业务实体副作用与新事件。
     * 若 idempotencyKey 已存在，返回已有事件且不写入、不产生副作用。
     */
    SubmissionResult commit(String idempotencyKey, String type, String payload,
                            Map<String, String> entitySideEffects, long nowMillis);

    Optional<EventRecord> findByEventId(String eventId);

    Optional<EventRecord> findByIdempotencyKey(String idempotencyKey);

    /**
     * 原子领取一条到期待处理事件（PENDING，或 RETRYING 且退避到期，
     * 或 MID-FLIGHT 但租约已过期）。条件比较 + 状态流转在同一临界区内落盘。
     */
    Optional<ClaimedEvent> claimNext(long nowMillis);

    /** 标记成功：业务副作用与 SUCCEEDED 同批原子落盘。lease 不匹配则失败。 */
    void markSucceeded(String eventId, long leaseToken, Map<String, String> sideEffects, long nowMillis);

    /** 安排重试：次数未耗尽则 RETRYING + 下次尝试时间；耗尽则 DEAD_LETTER(RETRIES_EXHAUSTED)。 */
    void scheduleRetry(String eventId, long leaseToken, String errorMessage, long nextAttemptAtMillis, long nowMillis);

    /** 直接进入死信（NON_RETRYABLE / VALIDATION_FAILED）。 */
    void markDeadLetter(String eventId, long leaseToken, DeadLetterReason reason,
                        String errorMessage, long nowMillis);

    /** 死信重放：DEAD_LETTER -&gt; PENDING 并清除租约与错误，等待正常领取。 */
    default EventRecord replay(String eventId, long nowMillis) {
        return replay(eventId, "MANUAL", nowMillis);
    }

    /** 死信重放（带触发来源，写入流转历史）。 */
    EventRecord replay(String eventId, String source, long nowMillis);

    List<EventRecord> findByStatus(EventStatus status);

    List<EventRecord> findDeadLetters();

    /** 强制等待此前所有追加记录完成磁盘持久化（测试可观测）。 */
    void flush();

    /** 已领取的事件及其租约令牌。 */
    record ClaimedEvent(EventRecord event, long leaseToken) {
    }
}

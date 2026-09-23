package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 事件的不可变快照。状态流转通过追加新的日志记录完成，不做原地更新。
 */
public record EventRecord(
        String eventId,
        String idempotencyKey,
        String type,
        String payload,
        EventStatus status,
        int attemptCount,
        int maxAttempts,
        long createdAtMillis,
        long nextAttemptAtMillis,
        long lastAttemptAtMillis,
        long leaseToken,
        String lastError,
        DeadLetterReason deadLetterReason,
        long deadLetterAtMillis
) {
}

package com.github.highcumontoa.reliableasynceventsjava.core;

import java.time.Instant;

/**
 * outbox_event 的一行快照。
 */
public record EventRecord(
        long id,
        String idempotencyKey,
        String type,
        String payload,
        EventStatus status,
        DeadReason deadReason,
        String errorDetail,
        int attempts,
        int maxAttempts,
        Instant nextAttemptAt,
        String claimToken,
        Instant claimedAt,
        Instant createdAt,
        Instant updatedAt
) {
}

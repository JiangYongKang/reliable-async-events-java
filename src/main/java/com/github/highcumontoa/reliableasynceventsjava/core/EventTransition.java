package com.github.highcumontoa.reliableasynceventsjava.core;

import java.time.Instant;

/**
 * 事件状态流转流水，使每一次流转都可查询、原因可解释。
 */
public record EventTransition(
        long id,
        long eventId,
        EventStatus fromStatus,
        EventStatus toStatus,
        TransitionReason reason,
        String detail,
        int attempts,
        Instant createdAt
) {
}

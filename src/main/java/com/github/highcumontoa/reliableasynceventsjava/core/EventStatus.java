package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 事件状态机。
 *
 * <pre>
 * PENDING ──┬─> PROCESSING ──> COMPLETED
 *           │     │  (失败可重试)
 *           │     └─> PENDING（退避到期后再领取）
 *           ├─> DEAD_LETTER（重试耗尽 / 不可重试 / 输入校验失败）
 *           └─> PROCESSING ──> DEAD_LETTER
 * DEAD_LETTER ──replay──> PENDING
 * </pre>
 */
public enum EventStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    DEAD_LETTER
}

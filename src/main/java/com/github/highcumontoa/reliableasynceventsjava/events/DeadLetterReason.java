package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 进入死信的互相区分的原因（可查询、可解释）。
 */
public enum DeadLetterReason {
    /** 瞬时故障自动重试次数耗尽。 */
    RETRIES_EXHAUSTED,
    /** 处理器明确判定为永久性（不可重试）故障。 */
    NON_RETRYABLE,
    /** 消费期间输入校验失败（如载荷缺少必填字段）。 */
    VALIDATION_FAILED
}

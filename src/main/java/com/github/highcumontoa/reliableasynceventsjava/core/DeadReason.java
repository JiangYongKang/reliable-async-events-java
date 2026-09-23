package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 死信原因，三类互相区分、可查询、可解释。
 */
public enum DeadReason {
    /** 瞬时故障重试次数耗尽。 */
    RETRY_EXHAUSTED,
    /** 永久性故障（处理器显式声明不可重试）。 */
    NON_RETRYABLE,
    /** 输入校验失败（提交时或处理时）。 */
    VALIDATION_FAILED
}

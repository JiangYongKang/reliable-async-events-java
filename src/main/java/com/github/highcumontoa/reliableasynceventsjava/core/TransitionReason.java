package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 状态流转判定依据，写入 outbox_event_transition 流水。
 */
public enum TransitionReason {
    ENQUEUED,                 // 与业务写入同一事务落库
    VALIDATION_REJECTED,      // 提交前输入校验失败，直接死信
    CLAIMED,                  // 调度器领取
    RETRY_BACKOFF,            // 瞬时故障，安排退避后重试
    RETRY_EXHAUSTED,          // 重试耗尽 -> 死信
    NON_RETRYABLE,            // 永久故障 -> 死信
    HANDLER_VALIDATION,       // 处理器发现输入非法 -> 死信
    COMPLETED,                // 处理成功
    LEASE_STALE_RECLAIMED,    // 崩溃遗留租约被回收
    REPLAYED                  // 死信重放 -> PENDING
}

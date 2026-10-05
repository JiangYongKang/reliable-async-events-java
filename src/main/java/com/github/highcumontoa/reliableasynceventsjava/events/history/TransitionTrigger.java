package com.github.highcumontoa.reliableasynceventsjava.events.history;

/**
 * 触发一次事件状态流转的来源。
 */
public enum TransitionTrigger {
    /** 业务提交（含同批业务副作用落盘）。 */
    SUBMIT,
    /** 被分发器领取（获得租约，含租约超时回收后的重新领取）。 */
    CLAIM,
    /** 瞬时故障后排定重试（进入退避等待）。 */
    RETRY_SCHEDULED,
    /** 处理成功（消费副作用与成功标记同批落盘）。 */
    SUCCEED,
    /** 进入死信（不可重试 / 校验失败 / 重试耗尽）。 */
    DEAD_LETTER,
    /** 死信被重放回 PENDING（人工单条或批量恢复）。 */
    REPLAY
}

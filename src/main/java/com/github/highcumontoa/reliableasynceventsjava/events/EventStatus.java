package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 事件生命周期状态。
 *
 * <pre>
 *   PENDING ──领取成功──▶ SUCCEEDED(终态)
 *      │ 瞬时故障
 *      ▼
 *   RETRYING ──退避到期再次领取──▶ ... ──成功──▶ SUCCEEDED
 *      │ 不可重试 / 校验失败 / 次数耗尽
 *      ▼
 *   DEAD_LETTER(终态) ──replay 修复后──▶ PENDING
 * </pre>
 */
public enum EventStatus {
    /** 已提交落盘，尚未开始处理（或死信重放后重新等待）。 */
    PENDING,
    /** 至少处理过一次且遇到瞬时故障，等待退避窗口结束后重试。 */
    RETRYING,
    /** 处理成功，业务副作用与成功标记已同批原子落盘（终态）。 */
    SUCCEEDED,
    /** 死信：重试耗尽 / 不可重试 / 消费期输入校验失败（终态，可查询、可重放）。 */
    DEAD_LETTER
}

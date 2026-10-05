package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 事件流转历史中的动作类型（每一次状态推进都会留下一条对应记录）。
 */
public enum TransitionAction {
    /** 事件首次提交落盘（-&gt; PENDING）。 */
    SUBMITTED,
    /** 被消费者领取，获得租约令牌（PENDING 在途）。 */
    CLAIMED,
    /** 瞬时故障后排定下次重试（-&gt; RETRYING）。 */
    RETRY_SCHEDULED,
    /** 处理成功，副作用与成功标记同批落盘（-&gt; SUCCEEDED）。 */
    SUCCEEDED,
    /** 进入死信（-&gt; DEAD_LETTER，携带区分原因）。 */
    DEAD_LETTER,
    /** 死信被重放（-&gt; PENDING，尝试次数清零）。 */
    REPLAYED
}

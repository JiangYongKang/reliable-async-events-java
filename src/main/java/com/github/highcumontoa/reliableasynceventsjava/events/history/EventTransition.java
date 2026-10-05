package com.github.highcumontoa.reliableasynceventsjava.events.history;

import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;

/**
 * 一条不可变的事件流转历史记录。
 *
 * <p>每一条记录都与对应的状态流转在<strong>同一个 WAL 提交批次</strong>内落盘，
 * 因此历史与事件状态永远一致：要么两者都可见，要么都不可见；
 * 进程重启后通过重放 WAL 完整重建，事件终态之后历史依然保留。</p>
 *
 * @param seq              全局单调递增序号（WAL 追加顺序，重启重放后保持原有顺序）
 * @param eventId          事件 ID
 * @param eventType        事件类型（冗余存储，便于跨事件检索）
 * @param fromStatus       流转前状态；提交（首次出现）时为 null
 * @param toStatus         流转后状态
 * @param trigger          触发来源
 * @param detail           原因或触发细节（错误信息、死信原因、重放来源、尝试次数等）
 * @param attemptCount     该次流转发生时的已尝试次数
 * @param occurredAtMillis 发生时间（毫秒）
 */
public record EventTransition(
        long seq,
        String eventId,
        String eventType,
        EventStatus fromStatus,
        EventStatus toStatus,
        TransitionTrigger trigger,
        String detail,
        int attemptCount,
        long occurredAtMillis
) {
}

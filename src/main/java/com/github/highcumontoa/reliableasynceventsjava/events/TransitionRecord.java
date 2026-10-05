package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 一条不可变的事件流转历史记录。
 *
 * <p>每条记录与其对应的状态流转在<strong>同一个 WAL 批次</strong>（同一次 fsync）落盘，
 * 因此历史与事件状态要么同时可见、要么同时不可见，重启重放后依然一一对应。</p>
 *
 * @param seq          全局单调递增序号（按 WAL 顺序分配，重启后延续）
 * @param eventId      事件 ID
 * @param eventType    事件类型（冗余存储，便于跨事件检索）
 * @param action       流转动作
 * @param fromStatus   流转前状态
 * @param toStatus     流转后状态
 * @param attemptCount 该次流转发生时的尝试次数
 * @param atMillis     发生时间（epoch 毫秒）
 * @param reason       原因 / 判定依据（如错误信息、死信原因）
 * @param trigger      触发来源（如 dispatcher、manual-replay、batch-recovery）
 */
public record TransitionRecord(
        long seq,
        String eventId,
        String eventType,
        TransitionAction action,
        EventStatus fromStatus,
        EventStatus toStatus,
        int attemptCount,
        long atMillis,
        String reason,
        String trigger
) {
}

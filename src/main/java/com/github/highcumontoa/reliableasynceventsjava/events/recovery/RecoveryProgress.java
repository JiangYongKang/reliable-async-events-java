package com.github.highcumontoa.reliableasynceventsjava.events.recovery;

/**
 * 批量恢复任务的进度快照。
 *
 * @param recoveryId   恢复任务 ID
 * @param status       运行状态
 * @param total        本批目标事件总数（启动时快照）
 * @param replayed     已被重放回 PENDING 的数量
 * @param succeeded    已到达 SUCCEEDED 的数量
 * @param deadAgain    重放后再次进入死信的数量（根因未修复时会出现）
 * @param inFlight     已重放但尚未终态的数量
 * @param remaining    尚未重放的数量
 * @param startedAtMillis 启动时间
 */
public record RecoveryProgress(
        String recoveryId,
        RecoveryStatus status,
        int total,
        int replayed,
        int succeeded,
        int deadAgain,
        int inFlight,
        int remaining,
        long startedAtMillis
) {
}

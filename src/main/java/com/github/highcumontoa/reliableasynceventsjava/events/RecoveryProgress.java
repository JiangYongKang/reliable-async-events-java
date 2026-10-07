package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 批量恢复任务的进度快照（不可变，可随时查询）。
 */
public record RecoveryProgress(
        String jobId,
        Status status,
        int total,
        int replayed,
        int skipped,
        int failed,
        int inFlight,
        long startedAtMillis,
        Long finishedAtMillis
) {
    /** 任务状态。 */
    public enum Status {
        /** 正在恢复。 */
        RUNNING,
        /** 收到停止请求，等待在途重放收尾。 */
        STOPPING,
        /** 被中途停止（未处理的死信保持原状，可再次发起恢复）。 */
        STOPPED,
        /** 全部目标事件处理完毕（replayed+skipped+failed == total，且在途事件均已到达终态）。 */
        COMPLETED,
        /** 任务内部异常终止。 */
        FAILED,
        /** 没有任何目标事件（未给筛选条件且当前无死信，或筛选条件一条都没命中），实际恢复 0 条。 */
        NO_TARGETS
    }

    /** 已处理（replayed+skipped+failed）数量。 */
    public int processed() {
        return replayed + skipped + failed;
    }
}

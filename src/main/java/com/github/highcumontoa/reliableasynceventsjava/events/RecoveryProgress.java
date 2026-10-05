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
        /** 全部目标事件处理完毕。 */
        COMPLETED,
        /** 任务内部异常终止。 */
        FAILED
    }

    /** 已处理（replayed+skipped+failed）数量。 */
    public int processed() {
        return replayed + skipped + failed;
    }
}

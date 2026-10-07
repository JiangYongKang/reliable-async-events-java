package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 批量恢复任务的进度快照（不可变，可随时查询）。
 *
 * @param inFlight 已重放回 PENDING、但尚未跑到终态（SUCCEEDED/DEAD_LETTER）的事件数，
 *                 即此刻仍占用并发额度、正在处理链路上流转的恢复事件数
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
        /** 全部目标事件处理完毕（有目标且每条都已重放/跳过/失败入账）。 */
        COMPLETED,
        /** 本次调用没有匹配到任何死信目标：一条都没恢复（与 COMPLETED 明确区分，避免误判恢复已生效）。 */
        COMPLETED_EMPTY,
        /** 任务内部异常终止。 */
        FAILED
    }

    /** 已处理（replayed+skipped+failed）数量。 */
    public int processed() {
        return replayed + skipped + failed;
    }
}

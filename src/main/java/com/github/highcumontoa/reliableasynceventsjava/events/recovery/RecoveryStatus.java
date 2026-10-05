package com.github.highcumontoa.reliableasynceventsjava.events.recovery;

/**
 * 批量恢复任务的运行状态。
 */
public enum RecoveryStatus {
    /** 正在恢复。 */
    RUNNING,
    /** 被中途停止；已重放的事件继续正常处理，未重放的仍是死信，可再次发起恢复（幂等）。 */
    STOPPED,
    /** 本批全部事件都已重放且到达终态。 */
    COMPLETED
}

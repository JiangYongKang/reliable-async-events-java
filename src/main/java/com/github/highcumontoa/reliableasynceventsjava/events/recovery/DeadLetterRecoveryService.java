package com.github.highcumontoa.reliableasynceventsjava.events.recovery;

/**
 * 死信批量恢复：按条件或显式列表把一批死信以受控的并发与速率重放回 PENDING，
 * 由正常分发链路重新处理。可查询进度、可中途停止；重复发起与停止后重发均幂等。
 */
public interface DeadLetterRecoveryService {

    /** 发起一次批量恢复，返回任务 ID。 */
    String startRecovery(RecoveryRequest request);

    /** 查询进度。任务不存在返回 null。 */
    RecoveryProgress progress(String recoveryId);

    /** 请求停止（已重放的事件继续处理完，未重放的保持死信）。 */
    void stop(String recoveryId);
}

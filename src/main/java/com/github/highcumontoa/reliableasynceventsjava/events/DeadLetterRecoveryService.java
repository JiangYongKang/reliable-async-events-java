package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.Optional;

/**
 * 批量死信恢复：按显式列表 / 条件筛选 / 无条件（全部死信）把一批死信以受控速率重放回 PENDING。
 *
 * <p>语义保证：</p>
 * <ul>
 *   <li>每条事件的重放仍是原子的单条 WAL 追加（DEAD_LETTER -&gt; PENDING），崩溃不留半截；</li>
 *   <li>幂等：目标事件若已不在死信态（被其它路径重放/处理）则计为 skipped，不产生重复副作用；</li>
 *   <li>同一批重复提交恢复任务：已重放的事件不再匹配死信筛选，自然去重；</li>
 *   <li>停止后再发起新任务即可续跑，最终所有死信一致地跑完；</li>
 *   <li>重放仅把事件置回 PENDING，正常新事件按 nextAttemptAt 公平竞争领取，不会被饿死；</li>
 *   <li>零目标（无条件且当前无死信，或筛选未命中）时任务以 NO_TARGETS 结束，
 *       明确区别于“有目标且全部完成”的 COMPLETED。</li>
 * </ul>
 */
public interface DeadLetterRecoveryService {

    /** 发起一个批量恢复任务，立即返回任务 ID（后台异步执行）。 */
    String startRecovery(RecoveryRequest request);

    /** 查询任务进度。 */
    Optional<RecoveryProgress> progress(String jobId);

    /** 请求中途停止任务（幂等；已重放的事件不回滚）。 */
    Optional<RecoveryProgress> stop(String jobId);
}

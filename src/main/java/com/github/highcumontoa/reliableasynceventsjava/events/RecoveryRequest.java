package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;

/**
 * 批量死信恢复请求。
 *
 * <p>选事件方式（可组合，取并集去重）：</p>
 * <ul>
 *   <li>{@code eventIds}：直接指定一批事件 ID；</li>
 *   <li>条件筛选：{@code eventType} / {@code reason} / {@code deadLetteredBeforeMillis}；</li>
 *   <li><b>两者都不给（全空请求）：全量恢复当前所有死信</b>（启动时快照，
 *       任务期间新产生的死信不纳入）。</li>
 * </ul>
 *
 * <p>若快照目标为空（如给了筛选条件但一条都没命中），任务以
 * {@link RecoveryProgress.Status#COMPLETED_EMPTY} 结束，明确区别于恢复成功的
 * {@link RecoveryProgress.Status#COMPLETED}。</p>
 *
 * <p>流量控制（两个参数各管一段）：{@code ratePerSecond} 约束重放注入速率（每秒最多
 * 重放多少条）；{@code concurrency} 约束“已重放、尚未到达终态”的在途恢复事件数，
 * 额度占满时注入暂停，直到有事件到达终态释放额度。</p>
 *
 * @param eventIds                 显式指定的事件 ID 列表（可空）
 * @param eventType                按事件类型筛选死信（可空）
 * @param reason                   按死信原因筛选（可空）
 * @param deadLetteredBeforeMillis 仅恢复在此时间之前进入死信的事件（可空）
 * @param ratePerSecond            每秒最多重放条数（&gt;0）
 * @param concurrency              同时在处理链路上流转的恢复事件上限（&gt;0）
 */
public record RecoveryRequest(
        List<String> eventIds,
        String eventType,
        DeadLetterReason reason,
        Long deadLetteredBeforeMillis,
        double ratePerSecond,
        int concurrency
) {
    public RecoveryRequest {
        if (ratePerSecond <= 0) {
            throw new EventValidationException("批量恢复被拒绝：ratePerSecond 必须为正: " + ratePerSecond);
        }
        if (concurrency <= 0) {
            throw new EventValidationException("批量恢复被拒绝：concurrency 必须为正: " + concurrency);
        }
    }
}

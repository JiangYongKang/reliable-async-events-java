package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;

/**
 * 批量死信恢复请求。
 *
 * <p>选事件方式（可组合，取并集去重）：</p>
 * <ul>
 *   <li>{@code eventIds}：直接指定一批事件 ID；</li>
 *   <li>条件筛选：{@code eventType} / {@code reason} / {@code deadLetteredBeforeMillis}；</li>
 *   <li><b>两者都不给：恢复当前全部死信</b>（用于一次性清空线上积压）。</li>
 * </ul>
 *
 * <p>流量控制（两个参数各管一件事）：</p>
 * <ul>
 *   <li>{@code ratePerSecond}：每秒最多重放多少条（注入速率）；</li>
 *   <li>{@code concurrency}：同时在处理链路上流转（已重放、未到终态）的恢复事件上限，
 *       名额占满时暂停注入新的重放，直到有事件到达终态。</li>
 * </ul>
 *
 * @param eventIds                 显式指定的事件 ID 列表（可空）
 * @param eventType                按事件类型筛选死信（可空）
 * @param reason                   按死信原因筛选（可空）
 * @param deadLetteredBeforeMillis 仅恢复在此时间之前进入死信的事件（可空）
 * @param ratePerSecond            每秒最多重放条数（&gt;0）
 * @param concurrency              在途恢复事件上限（&gt;0）
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

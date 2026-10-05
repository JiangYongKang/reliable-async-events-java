package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;

/**
 * 批量死信恢复请求。
 *
 * <p>两种选事件方式（可组合，取并集去重）：</p>
 * <ul>
 *   <li>{@code eventIds}：直接指定一批事件 ID；</li>
 *   <li>条件筛选：{@code eventType} / {@code reason} / {@code deadLetteredBeforeMillis}。</li>
 * </ul>
 *
 * <p>恢复速率由 {@code ratePerSecond} 控制（每秒最多重放多少条，避免打满下游），
 * {@code concurrency} 控制并发执行的重放线程数。</p>
 *
 * @param eventIds                 显式指定的事件 ID 列表（可空）
 * @param eventType                按事件类型筛选死信（可空）
 * @param reason                   按死信原因筛选（可空）
 * @param deadLetteredBeforeMillis 仅恢复在此时间之前进入死信的事件（可空）
 * @param ratePerSecond            每秒最多重放条数（&gt;0）
 * @param concurrency              并发重放线程数（&gt;0）
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

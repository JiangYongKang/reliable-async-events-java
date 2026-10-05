package com.github.highcumontoa.reliableasynceventsjava.events.recovery;

import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterReason;

import java.util.List;

/**
 * 批量恢复死信的请求。
 *
 * <p>目标事件两种指定方式（可组合，取并集）：</p>
 * <ul>
 *   <li>{@code eventIds}：显式给出的一批事件 ID；</li>
 *   <li>条件筛选：{@code type} / {@code reason} / {@code deadLetteredBeforeMillis}，
 *       均为空时匹配全部死信。</li>
 * </ul>
 *
 * <p>同一事件在请求中出现多次只会被恢复一次；已不在死信状态的事件会被跳过，
 * 因此同一批重复提交、或停止后再次发起，都不会产生重复副作用。</p>
 *
 * @param eventIds                 显式事件 ID 列表（可空）
 * @param type                     只恢复该事件类型（可空）
 * @param reason                   只恢复该死信原因（可空）
 * @param deadLetteredBeforeMillis 只恢复进入死信时间早于此时刻的事件（可空）
 * @param concurrency              同时在途（已重放未终态）的事件数上限，&lt;=0 取默认
 * @param ratePerSecond            每秒最多重放多少条，&lt;=0 取默认
 */
public record RecoveryRequest(
        List<String> eventIds,
        String type,
        DeadLetterReason reason,
        Long deadLetteredBeforeMillis,
        int concurrency,
        double ratePerSecond
) {
    public static final int DEFAULT_CONCURRENCY = 4;
    public static final double DEFAULT_RATE_PER_SECOND = 20.0;

    public RecoveryRequest {
        if (concurrency <= 0) {
            concurrency = DEFAULT_CONCURRENCY;
        }
        if (ratePerSecond <= 0) {
            ratePerSecond = DEFAULT_RATE_PER_SECOND;
        }
    }
}

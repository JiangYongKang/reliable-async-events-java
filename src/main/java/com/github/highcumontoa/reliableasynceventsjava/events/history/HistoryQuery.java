package com.github.highcumontoa.reliableasynceventsjava.events.history;

import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;

/**
 * 跨事件历史检索条件。所有过滤条件可空（空 = 不过滤），结果按 seq 升序。
 *
 * <p>为防止一次性倒出全量历史，{@link #limit} 有上限
 * （{@link #MAX_LIMIT}），未指定时使用 {@link #DEFAULT_LIMIT}；
 * 配合 {@link #offset} 分页。</p>
 */
public record HistoryQuery(
        String eventType,
        EventStatus toStatus,
        TransitionTrigger trigger,
        Long fromMillis,
        Long toMillis,
        int offset,
        int limit
) {
    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_LIMIT = 1000;

    public HistoryQuery {
        if (offset < 0) {
            offset = 0;
        }
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
        if (limit > MAX_LIMIT) {
            limit = MAX_LIMIT;
        }
    }

    public static HistoryQuery all() {
        return new HistoryQuery(null, null, null, null, null, 0, DEFAULT_LIMIT);
    }
}

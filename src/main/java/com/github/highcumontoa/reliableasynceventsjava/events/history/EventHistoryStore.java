package com.github.highcumontoa.reliableasynceventsjava.events.history;

import java.util.List;

/**
 * 事件流转历史的只读查询入口。写入由事件存储在每次状态流转时同批完成。
 */
public interface EventHistoryStore {

    /** 单个事件的完整时间线（按发生顺序）。 */
    List<EventTransition> historyOf(String eventId);

    /** 跨事件检索：按类型 / 结果状态 / 触发来源 / 时间窗口过滤，offset+limit 分页。 */
    List<EventTransition> query(HistoryQuery query);

    /** 满足条件的记录总数（用于分页）。 */
    long count(HistoryQuery query);
}

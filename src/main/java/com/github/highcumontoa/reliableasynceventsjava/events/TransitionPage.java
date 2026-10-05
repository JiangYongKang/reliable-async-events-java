package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;

/**
 * 一页流转历史检索结果。
 *
 * @param records    本页记录（按发生时间升序）
 * @param offset     本页偏移
 * @param limit      本页上限
 * @param totalCount 满足过滤条件的总条数（用于分页导航）
 */
public record TransitionPage(
        List<TransitionRecord> records,
        int offset,
        int limit,
        long totalCount
) {
}

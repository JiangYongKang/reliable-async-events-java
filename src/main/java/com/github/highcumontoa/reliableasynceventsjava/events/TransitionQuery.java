package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 跨事件流转历史检索条件。所有字段可空（空 = 不过滤），
 * 通过 {@code offset}/{@code limit} 分页，避免一次倒出全量。
 *
 * @param eventType 按事件类型过滤（可空）
 * @param action    按流转动作过滤（可空）
 * @param toStatus  按目标状态过滤（可空）
 * @param fromMillis 时间窗口下界（含，可空）
 * @param toMillis   时间窗口上界（含，可空）
 * @param offset     分页偏移（&ge;0）
 * @param limit      每页上限（&gt;0，有硬上限保护）
 */
public record TransitionQuery(
        String eventType,
        TransitionAction action,
        EventStatus toStatus,
        Long fromMillis,
        Long toMillis,
        int offset,
        int limit
) {
    /** 单次查询允许返回的最大条数（防止全量倾倒）。 */
    public static final int MAX_LIMIT = 500;

    public TransitionQuery {
        if (offset < 0) {
            throw new EventValidationException("历史检索被拒绝：offset 不能为负: " + offset);
        }
        if (limit <= 0) {
            throw new EventValidationException("历史检索被拒绝：limit 必须为正: " + limit);
        }
        if (limit > MAX_LIMIT) {
            throw new EventValidationException("历史检索被拒绝：limit 超过硬上限 " + MAX_LIMIT + ": " + limit);
        }
    }

    /** 便捷构造：仅按类型 + 分页。 */
    public static TransitionQuery of(String eventType, int offset, int limit) {
        return new TransitionQuery(eventType, null, null, null, null, offset, limit);
    }
}

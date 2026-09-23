package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 事件提交结果。
 *
 * @param created    true=本次新建；false=命中幂等键返回已有事件
 * @param eventId    事件 ID
 * @param status     提交后状态
 * @param duplicate  是否重复提交
 */
public record SubmitResult(long eventId, EventStatus status, boolean created, boolean duplicate) {
}

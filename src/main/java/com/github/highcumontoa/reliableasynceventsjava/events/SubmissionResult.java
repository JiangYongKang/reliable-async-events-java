package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 提交结果：{@code duplicate=true} 表示命中同一业务幂等键，
 * 返回的是先前已落盘的同一条事件，不会产生第二条可独立处理的事件。
 */
public record SubmissionResult(EventRecord event, boolean duplicate) {

    public static SubmissionResult created(EventRecord event) {
        return new SubmissionResult(event, false);
    }

    public static SubmissionResult duplicate(EventRecord existing) {
        return new SubmissionResult(existing, true);
    }
}

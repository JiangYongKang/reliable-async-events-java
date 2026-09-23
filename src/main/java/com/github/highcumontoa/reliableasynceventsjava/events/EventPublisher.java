package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.Map;

/** 业务侧提交入口：业务写入与事件投递原子完成。 */
public interface EventPublisher {

    /**
     * @param businessSideEffects 与事件同批原子落盘的业务写入（可为 null）
     */
    SubmissionResult publish(String idempotencyKey, String type, String payload,
                             Map<String, String> businessSideEffects);
}

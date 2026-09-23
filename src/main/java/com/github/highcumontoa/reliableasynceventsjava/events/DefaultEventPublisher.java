package com.github.highcumontoa.reliableasynceventsjava.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 业务侧提交入口。
 *
 * <p>一致性语义：业务写入（{@code businessSideEffects}）与事件记录由
 * {@link EventStore#commit} 在同一磁盘批次（一次 fsync）原子落盘，
 * 因此“业务已提交但事件丢失”与“事件已存在而业务回滚”都不可能发生；
 * 提交方法在写入返回前不会让任何副作用对外可见。</p>
 */
public class DefaultEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventPublisher.class);

    private final EventStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    public DefaultEventPublisher(EventStore store) {
        this.store = store;
    }

    @Override
    public SubmissionResult publish(String idempotencyKey, String type, String payload,
                                    Map<String, String> businessSideEffects) {
        // 提交期输入校验：失败直接拒绝，不产生任何事件记录
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new EventValidationException("提交被拒绝：idempotencyKey 不能为空");
        }
        if (type == null || type.isBlank()) {
            throw new EventValidationException("提交被拒绝：type 不能为空");
        }
        if (payload == null || payload.isBlank()) {
            throw new EventValidationException("提交被拒绝：payload 不能为空");
        }
        try {
            mapper.readTree(payload);
        } catch (Exception e) {
            throw new EventValidationException("提交被拒绝：payload 不是合法 JSON: " + e.getMessage());
        }
        log.info("收到事件提交输入: key={} type={} payload={} businessKeys={}",
                idempotencyKey, type, payload,
                businessSideEffects == null ? 0 : businessSideEffects.size());
        return store.commit(idempotencyKey, type, payload, businessSideEffects, System.currentTimeMillis());
    }
}

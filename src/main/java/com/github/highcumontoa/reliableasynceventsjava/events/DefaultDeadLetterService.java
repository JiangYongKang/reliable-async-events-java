package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 死信查询与恢复入口。重放仅把 DEAD_LETTER 事件原子地置回 PENDING，
 * 之后由正常分发链路重新领取，是否成功取决于修复后的处理器判定。
 */
public class DefaultDeadLetterService implements DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DefaultDeadLetterService.class);

    private final EventStore store;

    public DefaultDeadLetterService(EventStore store) {
        this.store = store;
    }

    @Override
    public List<EventRecord> list() {
        return store.findDeadLetters();
    }

    @Override
    public EventRecord replay(String eventId) {
        EventRecord before = store.findByEventId(eventId)
                .orElseThrow(() -> new EventException("死信重放失败：事件不存在 " + eventId));
        if (before.status() != EventStatus.DEAD_LETTER) {
            throw new EventException("死信重放被拒绝：eventId=" + eventId
                    + " 当前状态=" + before.status() + "，仅 DEAD_LETTER 可重放");
        }
        log.info("请求重放死信: eventId={} 原因={} 末次错误={}",
                eventId, before.deadLetterReason(), before.lastError());
        return store.replay(eventId, System.currentTimeMillis());
    }
}

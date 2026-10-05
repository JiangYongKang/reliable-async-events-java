package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.DeadLetterReason;
import com.github.highcumontoa.reliableasynceventsjava.events.DurableWALEventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStatus;
import com.github.highcumontoa.reliableasynceventsjava.events.EventStore;
import com.github.highcumontoa.reliableasynceventsjava.events.ExponentialBackoffRetryPolicy;
import com.github.highcumontoa.reliableasynceventsjava.events.history.EventTransition;
import com.github.highcumontoa.reliableasynceventsjava.events.history.HistoryQuery;
import com.github.highcumontoa.reliableasynceventsjava.events.history.TransitionTrigger;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 流转历史：单事件完整时间线、跨事件检索与分页、重启后历史保留、
 * 历史与事件状态始终一致。
 */
class EventHistoryTests {

    private static final Logger log = LoggerFactory.getLogger(EventHistoryTests.class);

    private DurableWALEventStore newStore(Path dir) {
        return new DurableWALEventStore(new DurableFileLog(dir.toString()),
                new ExponentialBackoffRetryPolicy(3, 100, 1000, 0), 30_000);
    }

    @Test
    void singleEventTimelineIsCompleteAndOrdered(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        String eventId = store.commit("k-h1", "ORDER", "{\"a\":1}", Map.of("order:1", "CREATED"), 1000)
                .event().eventId();
        log.info("【输入】提交事件 k-h1 后驱动完整生命周期：领取 -> 瞬时故障排定重试 -> 再次领取 -> 成功");

        EventStore.ClaimedEvent c1 = store.claimNext(1010).orElseThrow();
        store.scheduleRetry(eventId, c1.leaseToken(), "模拟瞬时故障: 连接超时", 1200, 1100);
        EventStore.ClaimedEvent c2 = store.claimNext(1200).orElseThrow();
        store.markSucceeded(eventId, c2.leaseToken(), Map.of("effect:1", "done"), 1300);

        List<EventTransition> timeline = store.historyOf(eventId);
        for (EventTransition t : timeline) {
            log.info("【状态变化】seq={} {} -> {} trigger={} attempt={} at={} detail={}",
                    t.seq(), t.fromStatus(), t.toStatus(), t.trigger(), t.attemptCount(),
                    t.occurredAtMillis(), t.detail());
        }
        assertEquals(5, timeline.size(), "提交/领取/重试/再领取/成功 共 5 条流转");
        assertTransition(timeline.get(0), null, EventStatus.PENDING, TransitionTrigger.SUBMIT, 1000);
        assertTransition(timeline.get(1), EventStatus.PENDING, EventStatus.PENDING, TransitionTrigger.CLAIM, 1010);
        assertTransition(timeline.get(2), EventStatus.PENDING, EventStatus.RETRYING,
                TransitionTrigger.RETRY_SCHEDULED, 1100);
        assertTransition(timeline.get(3), EventStatus.RETRYING, EventStatus.PENDING, TransitionTrigger.CLAIM, 1200);
        assertTransition(timeline.get(4), EventStatus.PENDING, EventStatus.SUCCEEDED,
                TransitionTrigger.SUCCEED, 1300);
        for (int i = 0; i < timeline.size(); i++) {
            assertEquals(i, timeline.get(i).seq(), "seq 必须按发生顺序单调递增");
            assertNotNull(timeline.get(i).detail(), "每条流转都必须携带原因/触发来源");
            assertEquals("ORDER", timeline.get(i).eventType());
        }
        assertTrue(timeline.get(2).detail().contains("连接超时"), "重试历史必须记录失败原因");
        assertEquals(2, timeline.get(3).attemptCount(), "第二次领取时 attemptCount=2");
        log.info("【判定依据】5 条流转 from/to 首尾相接、seq=0..4、时间与尝试次数与驱动输入一致 => 时间线完整有序");
    }

    @Test
    void deadLetterAndReplayAreRecordedWithReasonAndSource(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        String eventId = store.commit("k-h2", "ORDER", "{}", null, 0).event().eventId();
        EventStore.ClaimedEvent c = store.claimNext(0).orElseThrow();
        store.markDeadLetter(eventId, c.leaseToken(), DeadLetterReason.NON_RETRYABLE, "资源不存在", 100);
        store.replay(eventId, "MANUAL", 200);
        log.info("【输入】事件领取后标记 NON_RETRYABLE 死信，再以来源 MANUAL 重放");

        List<EventTransition> timeline = store.historyOf(eventId);
        EventTransition dead = timeline.get(2);
        EventTransition replay = timeline.get(3);
        assertEquals(TransitionTrigger.DEAD_LETTER, dead.trigger());
        assertEquals(EventStatus.DEAD_LETTER, dead.toStatus());
        assertTrue(dead.detail().contains("NON_RETRYABLE") && dead.detail().contains("资源不存在"),
                "死信历史必须记录区分原因与错误信息");
        assertEquals(TransitionTrigger.REPLAY, replay.trigger());
        assertEquals(EventStatus.DEAD_LETTER, replay.fromStatus());
        assertEquals(EventStatus.PENDING, replay.toStatus());
        assertTrue(replay.detail().contains("MANUAL"), "重放历史必须记录触发来源");
        log.info("【状态变化】{} -> {} ({}); {} -> {} ({})",
                dead.fromStatus(), dead.toStatus(), dead.detail(),
                replay.fromStatus(), replay.toStatus(), replay.detail());
        log.info("【判定依据】死信/重放均入历史且携带 reason 与来源 => 审计可解释");
    }

    @Test
    void historySurvivesRestartAndStaysConsistentWithState(@TempDir Path dir) {
        DurableWALEventStore before = newStore(dir);
        // 顺序提交并逐条驱动，避免依赖领取顺序
        String okId = before.commit("k-ok", "A", "{}", null, 0).event().eventId();
        EventStore.ClaimedEvent c1 = before.claimNext(0).orElseThrow();
        before.markSucceeded(c1.event().eventId(), c1.leaseToken(), null, 10);
        String deadId = before.commit("k-dead", "B", "{}", null, 20).event().eventId();
        EventStore.ClaimedEvent c2 = before.claimNext(20).orElseThrow();
        before.markDeadLetter(c2.event().eventId(), c2.leaseToken(),
                DeadLetterReason.VALIDATION_FAILED, "缺字段", 30);
        before.flush();
        int historySizeBefore = before.historyOf(okId).size() + before.historyOf(deadId).size();
        log.info("【输入】1 条成功 + 1 条死信后进程退出（WAL 已 fsync），用新实例从同一 WAL 恢复");

        DurableWALEventStore after = newStore(dir);
        List<EventTransition> okTimeline = after.historyOf(okId);
        List<EventTransition> deadTimeline = after.historyOf(deadId);
        assertEquals(before.historyOf(okId).size(), 0 + okTimeline.size(), "重启后成功事件历史完整保留");
        assertEquals(3, okTimeline.size(), "提交/领取/成功 3 条");
        assertEquals(3, deadTimeline.size(), "提交/领取/死信 3 条");
        assertEquals(historySizeBefore, okTimeline.size() + deadTimeline.size());
        assertEquals(TransitionTrigger.SUCCEED, okTimeline.get(2).trigger());
        assertEquals(TransitionTrigger.DEAD_LETTER, deadTimeline.get(2).trigger());
        log.info("【状态变化】重启后历史完整重建: ok={} 条末态={}, dead={} 条末态={}",
                okTimeline.size(), okTimeline.get(2).toStatus(),
                deadTimeline.size(), deadTimeline.get(2).toStatus());

        // 历史与状态一致性：每条事件历史末条 toStatus 必须等于当前状态
        assertEquals(after.findByEventId(okId).orElseThrow().status(),
                okTimeline.get(okTimeline.size() - 1).toStatus());
        assertEquals(after.findByEventId(deadId).orElseThrow().status(),
                deadTimeline.get(deadTimeline.size() - 1).toStatus());
        // 全量一致性：所有事件历史链重放结果 == 当前状态
        for (var status : EventStatus.values()) {
            for (var ev : after.findByStatus(status)) {
                List<EventTransition> tl = after.historyOf(ev.eventId());
                assertFalse(tl.isEmpty(), "任何事件都必须有历史");
                assertEquals(ev.status(), tl.get(tl.size() - 1).toStatus(),
                        "历史末条流转目标状态必须等于事件当前状态: " + ev.eventId());
                for (int i = 1; i < tl.size(); i++) {
                    assertEquals(tl.get(i - 1).toStatus(), tl.get(i).fromStatus(),
                            "历史链必须首尾相接: " + ev.eventId());
                }
            }
        }
        log.info("【判定依据】重启后历史条数不变；全部事件历史末态==当前状态且链式相接 => 历史与状态始终一致");
    }

    @Test
    void crossEventQueryFiltersAndPaginates(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        // 3 条 TYPE_A：2 成功 1 死信；2 条 TYPE_B：1 成功 1 待处理
        for (int i = 0; i < 3; i++) {
            store.commit("ka-" + i, "TYPE_A", "{}", null, 1000 + i);
        }
        for (int i = 0; i < 2; i++) {
            store.commit("kb-" + i, "TYPE_B", "{}", null, 2000 + i);
        }
        log.info("【输入】3 条 TYPE_A + 2 条 TYPE_B；驱动 3 条成功、1 条死信、1 条保持 PENDING");
        for (int i = 0; i < 4; i++) {
            EventStore.ClaimedEvent c = store.claimNext(3000 + i * 10L).orElseThrow();
            if (i == 3) {
                store.markDeadLetter(c.event().eventId(), c.leaseToken(),
                        DeadLetterReason.NON_RETRYABLE, "永久故障", 4000);
            } else {
                store.markSucceeded(c.event().eventId(), c.leaseToken(), null, 3000 + i * 10L + 5);
            }
        }

        // 按类型过滤
        List<EventTransition> typeA = store.query(new HistoryQuery(
                "TYPE_A", null, null, null, null, 0, 100));
        assertTrue(typeA.size() >= 3);
        assertTrue(typeA.stream().allMatch(t -> t.eventType().equals("TYPE_A")));
        // 按结果状态过滤
        List<EventTransition> deadTransitions = store.query(new HistoryQuery(
                null, EventStatus.DEAD_LETTER, null, null, null, 0, 100));
        assertEquals(1, deadTransitions.size());
        assertEquals(TransitionTrigger.DEAD_LETTER, deadTransitions.get(0).trigger());
        // 按触发来源过滤
        List<EventTransition> submits = store.query(new HistoryQuery(
                null, null, TransitionTrigger.SUBMIT, null, null, 0, 100));
        assertEquals(5, submits.size(), "5 次提交各一条 SUBMIT 历史");
        // 按时间窗口过滤
        List<EventTransition> window = store.query(new HistoryQuery(
                null, null, TransitionTrigger.SUBMIT, 1500L, 2500L, 0, 100));
        assertEquals(2, window.size(), "提交时间落在 [1500,2500] 的只有 ka-2/kb-0 两条");
        assertTrue(window.stream().allMatch(t -> t.occurredAtMillis() >= 1500
                && t.occurredAtMillis() <= 2500));
        log.info("【状态变化】按类型={} 条、按结果 DEAD_LETTER={} 条、按触发 SUBMIT={} 条、时间窗={} 条",
                typeA.size(), deadTransitions.size(), submits.size(), window.size());

        // 分页：limit 截断 + offset 翻页 + count 总数
        HistoryQuery page1 = new HistoryQuery(null, null, null, null, null, 0, 4);
        HistoryQuery page2 = new HistoryQuery(null, null, null, null, null, 4, 4);
        HistoryQuery page3 = new HistoryQuery(null, null, null, null, null, 8, 100);
        List<EventTransition> p1 = store.query(page1);
        List<EventTransition> p2 = store.query(page2);
        List<EventTransition> p3 = store.query(page3);
        long total = store.count(new HistoryQuery(null, null, null, null, null, 0, 1000));
        assertEquals(4, p1.size());
        assertEquals(4, p2.size());
        assertEquals(total - 8, p3.size(), "第三页为剩余全部记录");
        assertEquals(p1.get(3).seq() + 1, p2.get(0).seq(), "翻页必须按 seq 连续衔接");
        assertEquals(p2.get(3).seq() + 1, p3.get(0).seq(), "翻页必须按 seq 连续衔接");
        // limit 上限保护：超过 MAX_LIMIT 被截断
        HistoryQuery capped = new HistoryQuery(null, null, null, null, null, 0, 999_999);
        assertEquals(HistoryQuery.MAX_LIMIT, capped.limit(), "limit 必须被限制在上限内，防止全量倒出");
        log.info("【判定依据】过滤结果与输入构造完全吻合；分页 seq 连续、count={} 与全量一致；limit 上限={} 生效",
                total, HistoryQuery.MAX_LIMIT);
    }

    private static void assertTransition(EventTransition t, EventStatus from, EventStatus to,
                                         TransitionTrigger trigger, long at) {
        assertEquals(from, t.fromStatus(), "fromStatus 不符: " + t);
        assertEquals(to, t.toStatus(), "toStatus 不符: " + t);
        assertEquals(trigger, t.trigger(), "trigger 不符: " + t);
        assertEquals(at, t.occurredAtMillis(), "发生时间不符: " + t);
    }
}

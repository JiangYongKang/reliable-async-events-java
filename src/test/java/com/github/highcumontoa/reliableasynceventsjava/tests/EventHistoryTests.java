package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
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
 * 流转历史：完整时间线记录、与状态的一致性、重启后保留、跨事件检索与分页。
 */
class EventHistoryTests {

    private static final Logger log = LoggerFactory.getLogger(EventHistoryTests.class);

    private ExponentialBackoffRetryPolicy retryPolicy() {
        return new ExponentialBackoffRetryPolicy(3, 100, 1000, 0);
    }

    private DurableWALEventStore newStore(Path dir) {
        return new DurableWALEventStore(new DurableFileLog(dir.toString()), retryPolicy(), 30_000);
    }

    @Test
    void fullLifecycleTimelineIsRecorded(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        store.commit("h-1", "ORDER", "{\"amount\":1}", Map.of("order:1", "NEW"), 1_000);
        String eventId = store.findByIdempotencyKey("h-1").orElseThrow().eventId();
        log.info("【输入】提交事件 h-1(type=ORDER)，随后 领取->瞬时故障排定重试->再领取->成功");

        EventStore.ClaimedEvent c1 = store.claimNext(1_000).orElseThrow();
        store.scheduleRetry(eventId, c1.leaseToken(), "模拟瞬时故障: 连接超时", 1_100, 1_010);
        EventStore.ClaimedEvent c2 = store.claimNext(1_100).orElseThrow();
        store.markSucceeded(eventId, c2.leaseToken(), Map.of("done:1", "yes"), 1_120);

        List<TransitionRecord> timeline = store.historyOf(eventId);
        assertEquals(5, timeline.size(), "提交/领取/重试/再领取/成功 共 5 条历史");
        assertEquals(TransitionAction.SUBMITTED, timeline.get(0).action());
        assertNull(timeline.get(0).fromStatus());
        assertEquals(EventStatus.PENDING, timeline.get(0).toStatus());
        assertEquals("submit", timeline.get(0).trigger());

        assertEquals(TransitionAction.CLAIMED, timeline.get(1).action());
        assertEquals(1, timeline.get(1).attemptCount());

        assertEquals(TransitionAction.RETRY_SCHEDULED, timeline.get(2).action());
        assertEquals(EventStatus.PENDING, timeline.get(2).fromStatus());
        assertEquals(EventStatus.RETRYING, timeline.get(2).toStatus());
        assertTrue(timeline.get(2).reason().contains("连接超时"), "重试历史必须携带失败原因");

        assertEquals(TransitionAction.CLAIMED, timeline.get(3).action());
        assertEquals(EventStatus.RETRYING, timeline.get(3).fromStatus());
        assertEquals(2, timeline.get(3).attemptCount());

        assertEquals(TransitionAction.SUCCEEDED, timeline.get(4).action());
        assertEquals(EventStatus.SUCCEEDED, timeline.get(4).toStatus());

        // seq 单调递增且连续；时间戳非递减
        for (int i = 0; i < timeline.size(); i++) {
            assertEquals(i, timeline.get(i).seq(), "seq 必须按发生顺序连续分配");
            if (i > 0) {
                assertTrue(timeline.get(i).atMillis() >= timeline.get(i - 1).atMillis(),
                        "历史时间戳必须非递减");
            }
        }
        // 一致性：最后一条历史的目标状态 == 事件当前状态
        EventRecord current = store.findByEventId(eventId).orElseThrow();
        assertEquals(timeline.get(timeline.size() - 1).toStatus(), current.status(),
                "历史末笔与事件当前状态必须一致");
        log.info("【状态变化】{}", timeline);
        log.info("【判定依据】5 条历史动作/来源/目标状态/尝试次数逐项匹配；末笔 toStatus={} == 当前 status={}",
                timeline.get(4).toStatus(), current.status());
    }

    @Test
    void deadLetterAndReplayAreRecordedWithReasonAndTrigger(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        store.commit("h-2", "ORDER", "{}", null, 0);
        String eventId = store.findByIdempotencyKey("h-2").orElseThrow().eventId();
        log.info("【输入】事件 h-2 领取后判定永久故障进死信，再手动重放");

        EventStore.ClaimedEvent c = store.claimNext(0).orElseThrow();
        store.markDeadLetter(eventId, c.leaseToken(), DeadLetterReason.NON_RETRYABLE,
                "下游账户不存在", 10);
        store.replay(eventId, 20);

        List<TransitionRecord> timeline = store.historyOf(eventId);
        assertEquals(4, timeline.size());
        TransitionRecord dead = timeline.get(2);
        assertEquals(TransitionAction.DEAD_LETTER, dead.action());
        assertEquals(EventStatus.DEAD_LETTER, dead.toStatus());
        assertTrue(dead.reason().contains("NON_RETRYABLE"), "死信历史必须携带区分原因");
        assertTrue(dead.reason().contains("下游账户不存在"), "死信历史必须携带判定依据");

        TransitionRecord replay = timeline.get(3);
        assertEquals(TransitionAction.REPLAYED, replay.action());
        assertEquals(EventStatus.DEAD_LETTER, replay.fromStatus());
        assertEquals(EventStatus.PENDING, replay.toStatus());
        assertEquals("manual-replay", replay.trigger());
        assertEquals(EventStatus.PENDING, store.findByEventId(eventId).orElseThrow().status());
        log.info("【状态变化】DEAD_LETTER 历史 reason={}；REPLAYED 历史 trigger={}",
                dead.reason(), replay.trigger());
        log.info("【判定依据】死信原因/触发来源落历史，末笔 toStatus=PENDING == 当前状态");
    }

    @Test
    void historySurvivesRestartAndStaysConsistentWithState(@TempDir Path dir) {
        DurableWALEventStore before = newStore(dir);
        before.commit("h-3a", "A", "{}", null, 0);
        before.commit("h-3b", "B", "{}", null, 0);
        String idA = before.findByIdempotencyKey("h-3a").orElseThrow().eventId();
        String idB = before.findByIdempotencyKey("h-3b").orElseThrow().eventId();
        EventStore.ClaimedEvent c = before.claimNext(0).orElseThrow();
        before.markSucceeded(c.event().eventId(), c.leaseToken(), null, 5);
        before.flush();
        List<TransitionRecord> timelineABefore = before.historyOf(idA);
        List<TransitionRecord> timelineBBefore = before.historyOf(idB);
        long totalBefore = before.searchTransitions(new TransitionQuery(null, null, null, null, null, 0, 500))
                .totalCount();
        log.info("【输入】两条事件（A 成功、B 仅提交）后进程强杀重启，从同一 WAL 重建");

        DurableWALEventStore after = newStore(dir);
        assertEquals(timelineABefore, after.historyOf(idA), "重启后单事件时间线必须完整保留");
        assertEquals(timelineBBefore, after.historyOf(idB));
        assertEquals(totalBefore,
                after.searchTransitions(new TransitionQuery(null, null, null, null, null, 0, 500)).totalCount(),
                "重启后跨事件历史总数必须一致");
        log.info("【状态变化】重启后 A 历史={} 条, B 历史={} 条, 总数={}",
                after.historyOf(idA).size(), after.historyOf(idB).size(), totalBefore);

        // 重启后新流转的 seq 必须延续，不与旧记录冲突
        EventStore.ClaimedEvent c2 = after.claimNext(10).orElseThrow();
        List<TransitionRecord> all = after.searchTransitions(
                new TransitionQuery(null, null, null, null, null, 0, 500)).records();
        for (int i = 0; i < all.size(); i++) {
            assertEquals(i, all.get(i).seq(), "重启后新增历史的 seq 必须全局连续");
        }
        assertEquals(c2.event().eventId(), all.get(all.size() - 1).eventId());
        log.info("【判定依据】重启前后历史逐条相等；新领取产生第 {} 条历史且 seq 连续 => 历史与 WAL 同批落盘、可恢复",
                all.size());
    }

    @Test
    void crossEventSearchFiltersAndPaginates(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        // 3 条 ORDER（1 条成功、1 条死信、1 条待处理）+ 2 条 PAYMENT（1 条成功、1 条死信）
        store.commit("s-1", "ORDER", "{}", null, 100);
        store.commit("s-2", "ORDER", "{}", null, 200);
        store.commit("s-3", "ORDER", "{}", null, 300);
        store.commit("s-4", "PAYMENT", "{}", null, 400);
        store.commit("s-5", "PAYMENT", "{}", null, 500);
        succeedFirst(store, 100);
        deadLetterFirst(store, 200, 210);
        succeedFirst(store, 400);
        deadLetterFirst(store, 500, 510);
        log.info("【输入】5 条事件两种类型，结果覆盖 SUCCEEDED/DEAD_LETTER/PENDING");

        // 按类型过滤
        TransitionPage orders = store.searchTransitions(
                new TransitionQuery("ORDER", null, null, null, null, 0, 100));
        assertTrue(orders.totalCount() > 0);
        assertTrue(orders.records().stream().allMatch(r -> r.eventType().equals("ORDER")));
        // 按动作过滤（按结果检索）
        TransitionPage deads = store.searchTransitions(
                new TransitionQuery(null, TransitionAction.DEAD_LETTER, null, null, null, 0, 100));
        assertEquals(2, deads.totalCount(), "两条死信流转");
        assertTrue(deads.records().stream().allMatch(r -> r.toStatus() == EventStatus.DEAD_LETTER));
        // 组合：类型 + 动作 + 时间窗口
        TransitionPage combo = store.searchTransitions(new TransitionQuery(
                "PAYMENT", TransitionAction.SUBMITTED, null, 350L, 450L, 0, 100));
        assertEquals(1, combo.totalCount(), "PAYMENT 在 [350,450] 窗口内只有 1 次提交");
        // 分页：全量按页取，拼起来等于一次取全量
        TransitionPage all = store.searchTransitions(
                new TransitionQuery(null, null, null, null, null, 0, 500));
        TransitionPage page1 = store.searchTransitions(
                new TransitionQuery(null, null, null, null, null, 0, 3));
        TransitionPage page2 = store.searchTransitions(
                new TransitionQuery(null, null, null, null, null, 3, 3));
        assertEquals(3, page1.records().size());
        assertEquals(all.totalCount(), page1.totalCount());
        assertEquals(all.records().subList(0, 3), page1.records());
        assertEquals(all.records().subList(3, Math.min(6, all.records().size())), page2.records());
        // 限量保护：limit 超硬上限 / 非正数直接拒绝
        assertThrows(EventValidationException.class,
                () -> new TransitionQuery(null, null, null, null, null, 0, TransitionQuery.MAX_LIMIT + 1));
        assertThrows(EventValidationException.class,
                () -> new TransitionQuery(null, null, null, null, null, 0, 0));
        log.info("【状态变化】类型过滤={} 条, 死信动作过滤={} 条, 组合窗口={} 条, 全量={} 条(分页 3+{} 一致)",
                orders.totalCount(), deads.totalCount(), combo.totalCount(),
                all.totalCount(), page2.records().size());
        log.info("【判定依据】过滤条件逐项生效；offset/limit 分页切片与全量一致；limit 超上限被校验拒绝");
    }

    @Test
    void historyMatchesStateAfterEveryTransition(@TempDir Path dir) {
        DurableWALEventStore store = newStore(dir);
        store.commit("h-5", "T", "{}", null, 0);
        String eventId = store.findByIdempotencyKey("h-5").orElseThrow().eventId();
        log.info("【输入】对同一事件逐步推进 提交->领取->重试->领取->死信->重放->领取->成功，每步校验历史与状态一致");

        assertConsistent(store, eventId, TransitionAction.SUBMITTED);
        EventStore.ClaimedEvent c1 = store.claimNext(0).orElseThrow();
        assertConsistent(store, eventId, TransitionAction.CLAIMED);
        store.scheduleRetry(eventId, c1.leaseToken(), "瞬时故障", 100, 10);
        assertConsistent(store, eventId, TransitionAction.RETRY_SCHEDULED);
        EventStore.ClaimedEvent c2 = store.claimNext(100).orElseThrow();
        assertConsistent(store, eventId, TransitionAction.CLAIMED);
        store.markDeadLetter(eventId, c2.leaseToken(), DeadLetterReason.NON_RETRYABLE, "永久故障", 110);
        assertConsistent(store, eventId, TransitionAction.DEAD_LETTER);
        store.replay(eventId, 120);
        assertConsistent(store, eventId, TransitionAction.REPLAYED);
        EventStore.ClaimedEvent c3 = store.claimNext(120).orElseThrow();
        assertConsistent(store, eventId, TransitionAction.CLAIMED);
        store.markSucceeded(eventId, c3.leaseToken(), null, 130);
        assertConsistent(store, eventId, TransitionAction.SUCCEEDED);

        assertEquals(8, store.historyOf(eventId).size(), "每次流转恰好一条历史，不多不少");
        log.info("【判定依据】8 次流转后历史=8 条；每一步末笔 toStatus == 当前 status => 无漏记、无多记");
    }

    private void assertConsistent(DurableWALEventStore store, String eventId, TransitionAction expectedLast) {
        List<TransitionRecord> timeline = store.historyOf(eventId);
        assertFalse(timeline.isEmpty());
        TransitionRecord last = timeline.get(timeline.size() - 1);
        EventStatus current = store.findByEventId(eventId).orElseThrow().status();
        assertEquals(expectedLast, last.action(), "末笔历史动作");
        assertEquals(last.toStatus(), current,
                "历史末笔目标状态必须与事件当前状态一致: action=" + last.action());
    }

    private void succeedFirst(DurableWALEventStore store, long at) {
        EventStore.ClaimedEvent c = store.claimNext(at).orElseThrow();
        store.markSucceeded(c.event().eventId(), c.leaseToken(), null, at + 5);
    }

    private void deadLetterFirst(DurableWALEventStore store, long claimAt, long deadAt) {
        EventStore.ClaimedEvent c = store.claimNext(claimAt).orElseThrow();
        store.markDeadLetter(c.event().eventId(), c.leaseToken(),
                DeadLetterReason.NON_RETRYABLE, "永久故障", deadAt);
    }
}

package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.core.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重启持久化：使用 H2 文件库，丢弃旧 DataSource / 引擎实例后用全新实例重连，
 * 模拟进程重启。事件与状态必须不丢、不重复处理。
 */
class RestartPersistenceTest {

    private static final Logger log = LoggerFactory.getLogger(RestartPersistenceTest.class);

    private Path newDbDir() throws Exception {
        Path dir = Files.createTempDirectory("reliable-events-restart-");
        dir.toFile().deleteOnExit();
        return dir;
    }

    private void deleteRecursively(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private TestSupport.Harness newEngine(Path dir, int maxAttempts) {
        JdbcDataSource ds = TestSupport.fileDataSource(dir.resolve("eventdb").toString());
        return TestSupport.harnessFor(ds, maxAttempts, Duration.ofMillis(10),
                Duration.ofMinutes(2), 2);
    }

    @Test
    void pendingAndDeadLetterEventsSurviveRestartAndComplete() throws Exception {
        Path dir = newDbDir();
        long eventId;
        long deadEventId;

        // ---- 第一个进程：提交 2 条事件，1 条失败退避中，1 条永久死信 ----
        {
            TestSupport.Harness first = newEngine(dir, 5);
            AtomicInteger calls = new AtomicInteger();
            first.register("DURABLE", payload -> {
                int n = calls.incrementAndGet();
                log.info("[进程1] 处理器第 {} 次调用", n);
                if (n == 1) {
                    throw new RetryableEventException("进程重启前的瞬时故障");
                }
            });
            first.register("FATAL", payload -> {
                throw new NonRetryableEventException("永久故障，等待重启后人工重放");
            });

            SubmitResult s1 = first.eventService.submitStandalone("k-1", "DURABLE", "{}");
            SubmitResult s2 = first.eventService.submitStandalone("k-2", "FATAL", "{}");
            eventId = s1.eventId();
            deadEventId = s2.eventId();

            String r1 = first.dispatcher.claimAndProcess(eventId);
            String r2 = first.dispatcher.claimAndProcess(deadEventId);
            log.info("[进程1] 事件 {} -> {}（退避中）；事件 {} -> {}（死信）",
                    eventId, r1, deadEventId, r2);
            assertEquals("PENDING", r1);
            assertEquals("DEAD_LETTER", r2);
            // 模拟进程退出：连接池与内存状态全部丢弃
        }

        // ---- 第二个进程：全新引擎实例重连同一文件库 ----
        {
            TestSupport.Harness second = newEngine(dir, 5);
            AtomicInteger calls = new AtomicInteger();
            second.register("DURABLE", payload -> {
                calls.incrementAndGet();
                log.info("[进程2] 执行副作用（不依赖进程1的任何内存状态）");
            });
            second.register("FATAL", payload -> {
                throw new NonRetryableEventException("仍未修复");
            });

            EventRecord pending = second.eventService.getEvent(eventId).orElseThrow();
            EventRecord dead = second.eventService.getEvent(deadEventId).orElseThrow();
            log.info("[进程2] 重启后读取: event={} status={} attempts={}; event={} status={} reason={}",
                    pending.id(), pending.status(), pending.attempts(),
                    dead.id(), dead.status(), dead.deadReason());
            assertEquals(EventStatus.PENDING, pending.status(), "退避中的事件必须存活");
            assertEquals(1, pending.attempts());
            assertEquals(DeadReason.NON_RETRYABLE, dead.deadReason(), "死信必须存活");

            EventRecord done = TransientFailureRecoveryTest.waitUntilTerminal(
                    second, eventId, Duration.ofSeconds(2), 10);
            assertEquals(EventStatus.COMPLETED, done.status());
            assertEquals(1, calls.get(), "重启后副作用恰好执行一次（attempts 计数已持久化）");
        }

        deleteRecursively(dir);
    }

    @Test
    void processingLeaseFromDeadProcessIsRecoveredAfterRestart() throws Exception {
        Path dir = newDbDir();
        long eventId;
        {
            TestSupport.Harness first = newEngine(dir, 5);
            SubmitResult s = first.eventService.submitStandalone("k-lease", "DURABLE", "{}");
            eventId = s.eventId();
            // 领取后立即"进程死亡"
            first.repository.claim(eventId, "lost-token", java.time.Instant.now());
            log.info("[进程1] event={} 领取后崩溃，留在 PROCESSING", eventId);
        }

        // 新进程使用很短的租约 TTL，验证对遗留租约的回收
        {
            JdbcDataSource ds = TestSupport.fileDataSource(dir.resolve("eventdb").toString());
            TestSupport.Harness second = TestSupport.harnessFor(ds, 5, Duration.ofMillis(10),
                    Duration.ofMillis(50), 2);
            AtomicInteger calls = new AtomicInteger();
            second.register("DURABLE", payload -> calls.incrementAndGet());

            // 人为把 claimed_at 回拨到 TTL 之前
            second.jdbc.update("UPDATE outbox_event SET claimed_at=? WHERE id=?",
                    java.sql.Timestamp.from(java.time.Instant.now().minusMillis(100)), eventId);

            int processed = second.dispatcher.runOnce();
            EventRecord done = second.eventService.getEvent(eventId).orElseThrow();
            log.info("[进程2] 租约回收后 runOnce={} status={} 副作用={}",
                    processed, done.status(), calls.get());
            assertEquals(EventStatus.COMPLETED, done.status());
            assertEquals(1, calls.get());
            List<EventTransition> ts = second.eventService.getTransitions(eventId);
            assertTrue(ts.stream().anyMatch(t -> t.reason() == TransitionReason.LEASE_STALE_RECLAIMED));
        }
        deleteRecursively(dir);
    }
}

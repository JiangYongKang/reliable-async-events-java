package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.engine.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 退避策略单测：打印每次输入与判定依据（base 退避、抖动边界）。
 */
class RetryPolicyTest {

    private static final Logger log = LoggerFactory.getLogger(RetryPolicyTest.class);

    @Test
    void exponentialBackoffIsBoundedAndExplainable() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0,
                Duration.ofSeconds(10), 0.0);
        Instant t0 = Instant.parse("2026-09-23T00:00:00Z");

        long[] expected = {100, 200, 400, 800}; // 第1~4次失败后的基础退避
        for (int attempt = 1; attempt <= 4; attempt++) {
            Duration base = policy.backoffFor(attempt);
            Instant next = policy.nextAttemptAfter(attempt, t0);
            log.info("输入: attempt={} 判定: base={} nextAttemptAt={}", attempt, base, next);
            assertEquals(expected[attempt - 1], base.toMillis());
            assertEquals(t0.plus(base), next, "无抖动时下一次尝试时间 = now + base");
        }

        // 基础退避 100*2^(n-1)：到第 8 次（12.8s）超过 10s 上限，必须截断（退避有界）
        Duration capped = policy.backoffFor(8);
        log.info("输入: attempt=8 判定: base={}（上限 10s 截断，未截断应为 12.8s）", capped);
        assertEquals(Duration.ofSeconds(10), capped);
    }

    @Test
    void jitterStaysWithinFactor() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0,
                Duration.ofSeconds(10), 0.2);
        Instant t0 = Instant.parse("2026-09-23T00:00:00Z");
        for (int attempt = 1; attempt <= 3; attempt++) {
            Instant next = policy.nextAttemptAfter(attempt, t0);
            long base = policy.backoffFor(attempt).toMillis();
            long actual = Duration.between(t0, next).toMillis();
            log.info("输入: attempt={} base={}ms 判定: 抖动后={}ms（允许 [base, base*1.2]）",
                    attempt, base, actual);
            assertTrue(actual >= base && actual <= base * 1.2 + 1);
        }
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(0, Duration.ofMillis(10), 2.0, Duration.ofSeconds(1), 0.1));
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(3, Duration.ZERO, 2.0, Duration.ofSeconds(1), 0.1));
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(3, Duration.ofMillis(10), 0.5, Duration.ofSeconds(1), 0.1));
    }
}

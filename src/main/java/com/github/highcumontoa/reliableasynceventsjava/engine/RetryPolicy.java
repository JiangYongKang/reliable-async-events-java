package com.github.highcumontoa.reliableasynceventsjava.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 可配置的指数退避重试策略：
 * <pre>
 * base(n)   = min(initialDelay * multiplier^(n-1), maxDelay)
 * jitter    = base * random(0 .. jitterFactor)
 * delay(n)  = base + jitter
 * </pre>
 * 抖动用于避免多个消费者在同一时刻形成惊群。
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;
    private final double jitterFactor;

    public RetryPolicy(int maxAttempts, Duration initialDelay, double multiplier,
                       Duration maxDelay, double jitterFactor) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1");
        }
        if (initialDelay == null || initialDelay.isNegative() || initialDelay.isZero()) {
            throw new IllegalArgumentException("initialDelay 必须为正");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier 必须 >= 1");
        }
        if (jitterFactor < 0 || jitterFactor >= 1) {
            throw new IllegalArgumentException("jitterFactor 必须位于 [0,1)");
        }
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
        this.jitterFactor = jitterFactor;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Duration getInitialDelay() {
        return initialDelay;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    public double getJitterFactor() {
        return jitterFactor;
    }

    /**
     * 第 attempt 次尝试（从 1 开始）失败后，下一次可被领取的最早时间。
     */
    public Instant nextAttemptAfter(int attempt, Instant now) {
        return now.plus(backoffFor(attempt)).plusMillis(jitterMillis(attempt));
    }

    /** 第 attempt 次尝试失败后的基础退避时长（不含抖动），便于日志解释。 */
    public Duration backoffFor(int attempt) {
        double factor = Math.pow(multiplier, Math.max(0, attempt - 1));
        long millis = (long) Math.min(initialDelay.toMillis() * factor, (double) maxDelay.toMillis());
        return Duration.ofMillis(millis);
    }

    private long jitterMillis(int attempt) {
        if (jitterFactor == 0.0) {
            return 0L;
        }
        long base = backoffFor(attempt).toMillis();
        return (long) (base * jitterFactor * ThreadLocalRandom.current().nextDouble());
    }
}

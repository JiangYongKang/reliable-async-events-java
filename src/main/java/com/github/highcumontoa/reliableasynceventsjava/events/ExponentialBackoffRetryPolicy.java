package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 指数退避重试策略：
 *
 * <pre>
 *   delay(n) = min(base * 2^(n-1), maxBackoff) + uniform(0, jitter)
 * </pre>
 *
 * 其中 n 为已完成的尝试次数（第一次失败后 n=1）。
 * 最大尝试次数包含首次处理；第 maxAttempts 次仍失败即不再重试。
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(ExponentialBackoffRetryPolicy.class);

    private final int maxAttempts;
    private final long baseBackoffMillis;
    private final long maxBackoffMillis;
    private final long jitterMillis;
    private final Random random;

    public ExponentialBackoffRetryPolicy(int maxAttempts, long baseBackoffMillis,
                                         long maxBackoffMillis, long jitterMillis) {
        this(maxAttempts, baseBackoffMillis, maxBackoffMillis, jitterMillis, new Random());
    }

    public ExponentialBackoffRetryPolicy(int maxAttempts, long baseBackoffMillis,
                                  long maxBackoffMillis, long jitterMillis, Random random) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.jitterMillis = jitterMillis;
        this.random = random;
        log.info("重试策略初始化: maxAttempts={}, baseBackoff={}ms, maxBackoff={}ms, jitter={}ms",
                maxAttempts, baseBackoffMillis, maxBackoffMillis, jitterMillis);
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public long backoffMillis(int completedAttempts, long nowMillis) {
        long exp = baseBackoffMillis;
        for (int i = 1; i < completedAttempts; i++) {
            exp = Math.min(exp * 2, maxBackoffMillis);
        }
        long jitter = jitterMillis <= 0 ? 0 : Math.abs(random.nextLong()) % (jitterMillis + 1);
        long delay = Math.min(exp, maxBackoffMillis) + jitter;
        log.debug("退避计算: 已完成尝试={} -> delay={}ms (指数项={}, jitter={})",
                completedAttempts, delay, exp, jitter);
        return delay;
    }
}

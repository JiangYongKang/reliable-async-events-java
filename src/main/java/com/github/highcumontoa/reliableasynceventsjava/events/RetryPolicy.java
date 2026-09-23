package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 重试策略：最大尝试次数与指数退避。
 */
public interface RetryPolicy {

    int maxAttempts();

    /**
     * 第 {@code completedAttempts} 次尝试失败后，下一次尝试前需要等待的毫秒数。
     */
    long backoffMillis(int completedAttempts, long nowMillis);
}

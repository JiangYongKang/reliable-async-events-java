package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 瞬时故障：按退避策略自动重试；耗尽后进入死信（RETRY_EXHAUSTED）。
 */
public class RetryableEventException extends EventException {
    public RetryableEventException(String message) {
        super(message);
    }

    public RetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

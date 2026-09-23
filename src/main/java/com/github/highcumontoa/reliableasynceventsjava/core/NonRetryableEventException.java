package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 处理器显式声明的永久故障：不重试，事件进入死信（NON_RETRYABLE）。
 */
public class NonRetryableEventException extends EventException {
    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

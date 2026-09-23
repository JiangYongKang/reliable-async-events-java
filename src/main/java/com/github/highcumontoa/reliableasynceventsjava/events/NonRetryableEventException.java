package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 永久性（不可重试）故障：处理器主动声明该错误重试无意义。
 */
public class NonRetryableEventException extends EventException {
    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

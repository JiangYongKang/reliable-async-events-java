package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 事件模块统一异常基类。
 */
public class EventException extends RuntimeException {
    public EventException(String message) {
        super(message);
    }

    public EventException(String message, Throwable cause) {
        super(message, cause);
    }
}

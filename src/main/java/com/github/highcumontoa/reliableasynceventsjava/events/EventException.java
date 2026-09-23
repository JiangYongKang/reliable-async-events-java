package com.github.highcumontoa.reliableasynceventsjava.events;

/** 事件系统异常基类。 */
public class EventException extends RuntimeException {
    public EventException(String message) {
        super(message);
    }

    public EventException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 输入校验失败：永久失败，不进入重试，事件直接进死信（VALIDATION_FAILED）。
 */
public class InvalidEventException extends EventException {
    public InvalidEventException(String message) {
        super(message);
    }
}

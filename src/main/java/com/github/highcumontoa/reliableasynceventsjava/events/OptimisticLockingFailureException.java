package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 条件更新失败（lease 不匹配 / 状态已被其它消费者推进）。
 */
public class OptimisticLockingFailureException extends EventException {
    public OptimisticLockingFailureException(String message) {
        super(message);
    }
}

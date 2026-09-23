package com.github.highcumontoa.reliableasynceventsjava.events;

/**
 * 输入校验失败。
 * <ul>
 *   <li>提交阶段抛出：拒绝写入，不产生事件（接口直接 400）；</li>
 *   <li>消费阶段抛出：事件进入死信，原因为 VALIDATION_FAILED。</li>
 * </ul>
 */
public class EventValidationException extends EventException {
    public EventValidationException(String message) {
        super(message);
    }
}

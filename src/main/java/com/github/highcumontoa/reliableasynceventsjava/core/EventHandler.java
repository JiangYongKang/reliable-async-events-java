package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 事件处理器。每个事件类型注册一个实现。
 *
 * <p>异常分类约定：</p>
 * <ul>
 *   <li>{@link InvalidEventException} / {@link NonRetryableEventException}：永久失败，直接死信；</li>
 *   <li>{@link RetryableEventException}：瞬时故障，退避重试；</li>
 *   <li>其它异常：默认按瞬时故障处理（保守重试，避免静默丢弃）。</li>
 * </ul>
 *
 * <p>实现必须是幂等的：投递为 at-least-once，重复领取/重放时副作用只允许生效一次。</p>
 */
public interface EventHandler {

    /** 处理事件负载。 */
    void handle(String payload) throws Exception;

    /**
     * 该处理器对应的事件类型。默认把 {@code XxxHandler} 类名转为 {@code XXX}
     * （如 OrderCreatedHandler -> ORDER_CREATED），可按需要覆盖。
     */
    default String eventType() {
        String name = getClass().getSimpleName();
        if (name.endsWith("Handler")) {
            name = name.substring(0, name.length() - "Handler".length());
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                sb.append('_');
            }
            sb.append(Character.toUpperCase(c));
        }
        return sb.toString();
    }
}

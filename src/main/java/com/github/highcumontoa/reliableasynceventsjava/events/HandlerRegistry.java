package com.github.highcumontoa.reliableasynceventsjava.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按事件类型查找处理器。同一类型重复注册将被拒绝（避免消费行为歧义）。
 */
public class HandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(HandlerRegistry.class);

    private final Map<String, EventHandler> handlers = new ConcurrentHashMap<>();

    public void register(EventHandler handler) {
        EventHandler previous = handlers.putIfAbsent(handler.type(), handler);
        if (previous != null) {
            throw new IllegalStateException("事件类型 " + handler.type() + " 已注册处理器: "
                    + previous.getClass().getName());
        }
        log.info("事件处理器已注册: type={} -> {}", handler.type(), handler.getClass().getSimpleName());
    }

    public Optional<EventHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}

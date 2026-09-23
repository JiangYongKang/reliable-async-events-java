package com.github.highcumontoa.reliableasynceventsjava.engine;

import com.github.highcumontoa.reliableasynceventsjava.core.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 事件类型 -> 处理器 的注册表。
 */
public class EventHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventHandlerRegistry.class);

    private final Map<String, EventHandler> handlers = new LinkedHashMap<>();

    public EventHandlerRegistry() {
    }

    public synchronized void register(String type, EventHandler handler) {
        if (handlers.putIfAbsent(type, handler) != null) {
            throw new IllegalStateException("事件类型 " + type + " 已注册处理器");
        }
        log.info("[registry] 注册处理器 type={} handler={}", type, handler.getClass().getSimpleName());
    }

    public synchronized Optional<EventHandler> lookup(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public synchronized Map<String, EventHandler> all() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(handlers));
    }
}

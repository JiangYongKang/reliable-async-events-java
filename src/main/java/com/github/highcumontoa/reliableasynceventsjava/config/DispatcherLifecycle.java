package com.github.highcumontoa.reliableasynceventsjava.config;

import com.github.highcumontoa.reliableasynceventsjava.engine.EventDispatcher;
import org.springframework.context.SmartLifecycle;

/**
 * 调度器随 Spring 容器启动/停止（可通过 reliable.events.dispatcher-enabled=false 关闭）。
 */
public class DispatcherLifecycle implements SmartLifecycle {

    private final EventDispatcher dispatcher;
    private final EventProperties properties;
    private volatile boolean started;

    public DispatcherLifecycle(EventDispatcher dispatcher, EventProperties properties) {
        this.dispatcher = dispatcher;
        this.properties = properties;
    }

    @Override
    public void start() {
        if (properties.isDispatcherEnabled()) {
            dispatcher.start();
            started = true;
        }
    }

    @Override
    public void stop() {
        if (started) {
            dispatcher.stop();
            started = false;
        }
    }

    @Override
    public boolean isRunning() {
        return started && dispatcher.isRunning();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}

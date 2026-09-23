package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.NonRetryableEventException;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.Map;

/**
 * 集成测试用事件处理器（作为 Bean 被 HandlerRegistry 自动收集）。
 */
@TestConfiguration
public class TestEventHandlers {

    @Bean
    EventHandler webOkHandler() {
        return new EventHandler() {
            @Override
            public String type() {
                return "WEB_OK";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                return Map.of("consumed:" + event.idempotencyKey(), "yes");
            }
        };
    }

    @Bean
    EventHandler webFatalHandler() {
        return new EventHandler() {
            @Override
            public String type() {
                return "WEB_FATAL";
            }

            @Override
            public Map<String, String> handle(EventRecord event) {
                throw new NonRetryableEventException("下游账户不存在（集成测试永久故障）");
            }
        };
    }
}

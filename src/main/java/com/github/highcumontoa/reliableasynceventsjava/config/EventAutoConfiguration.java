package com.github.highcumontoa.reliableasynceventsjava.config;

import com.github.highcumontoa.reliableasynceventsjava.core.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.engine.EventHandlerRegistry;
import com.github.highcumontoa.reliableasynceventsjava.engine.EventDispatcher;
import com.github.highcumontoa.reliableasynceventsjava.engine.ReliableEventService;
import com.github.highcumontoa.reliableasynceventsjava.engine.RetryPolicy;
import com.github.highcumontoa.reliableasynceventsjava.storage.EventRepository;
import com.github.highcumontoa.reliableasynceventsjava.storage.SchemaInitializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;

/**
 * 组装事件模块：Schema、Repository、Handler 注册表、RetryPolicy、
 * 可靠事件门面、进程内调度器及其生命周期。
 */
@Configuration
@EnableConfigurationProperties(EventProperties.class)
public class EventAutoConfiguration {

    @Bean
    public SchemaInitializer schemaInitializer(DataSource dataSource) {
        return new SchemaInitializer(dataSource);
    }

    @Bean
    public EventRepository eventRepository(DataSource dataSource) {
        return new EventRepository(dataSource);
    }

    @Bean
    public EventHandlerRegistry eventHandlerRegistry(List<EventHandler> handlers) {
        EventHandlerRegistry registry = new EventHandlerRegistry();
        for (EventHandler handler : handlers) {
            registry.register(handler.eventType(), handler);
        }
        return registry;
    }

    @Bean
    public RetryPolicy retryPolicy(EventProperties properties) {
        return new RetryPolicy(
                properties.getMaxAttempts(),
                properties.getInitialBackoff(),
                properties.getBackoffMultiplier(),
                properties.getMaxBackoff(),
                properties.getJitterFactor());
    }

    @Bean
    public ReliableEventService reliableEventService(EventRepository repository,
                                                     PlatformTransactionManager txManager,
                                                     EventProperties properties) {
        ReliableEventService service = new ReliableEventService(repository, txManager);
        service.setMaxAttempts(properties.getMaxAttempts());
        return service;
    }

    @Bean
    public EventDispatcher eventDispatcher(EventRepository repository,
                                           EventHandlerRegistry registry,
                                           RetryPolicy retryPolicy,
                                           PlatformTransactionManager txManager,
                                           EventProperties properties) {
        return new EventDispatcher(repository, registry, retryPolicy, txManager, properties);
    }

    @Bean
    public DispatcherLifecycle dispatcherLifecycle(EventDispatcher dispatcher,
                                                   EventProperties properties) {
        return new DispatcherLifecycle(dispatcher, properties);
    }
}

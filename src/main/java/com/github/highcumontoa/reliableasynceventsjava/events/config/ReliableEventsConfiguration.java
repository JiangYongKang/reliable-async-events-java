package com.github.highcumontoa.reliableasynceventsjava.events.config;

import com.github.highcumontoa.reliableasynceventsjava.events.*;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableLog;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 进程内可靠事件组件装配：WAL 日志 -&gt; 事件存储 -&gt; 发布/死信服务/分发器。
 * 所有组件都在当前进程内运行，不依赖任何外部中间件或网络服务。
 */
@Configuration
@EnableConfigurationProperties(ReliableEventsProperties.class)
public class ReliableEventsConfiguration {

    @Bean
    public DurableLog reliableEventLog(ReliableEventsProperties props) {
        return new DurableFileLog(props.getStoreDir());
    }

    @Bean
    public RetryPolicy retryPolicy(ReliableEventsProperties props) {
        return new ExponentialBackoffRetryPolicy(props.getMaxAttempts(),
                props.getBaseBackoffMillis(), props.getMaxBackoffMillis(), props.getJitterMillis());
    }

    @Bean
    public DurableWALEventStore eventStore(DurableLog log, RetryPolicy retryPolicy,
                                           ReliableEventsProperties props) {
        return new DurableWALEventStore(log, retryPolicy, props.getLeaseMillis());
    }

    @Bean
    @org.springframework.context.annotation.Primary
    public BusinessStateStore businessStateStore(DurableWALEventStore store) {
        return store;
    }

    @Bean
    public HandlerRegistry handlerRegistry(List<EventHandler> handlers) {
        HandlerRegistry registry = new HandlerRegistry();
        handlers.forEach(registry::register);
        return registry;
    }

    @Bean
    public EventPublisher eventPublisher(DurableWALEventStore store) {
        return new DefaultEventPublisher(store);
    }

    @Bean
    public DeadLetterService deadLetterService(DurableWALEventStore store) {
        return new DefaultDeadLetterService(store);
    }

    @Bean(destroyMethod = "shutdown")
    public com.github.highcumontoa.reliableasynceventsjava.events.recovery.DefaultDeadLetterRecoveryService
            deadLetterRecoveryService(DurableWALEventStore store) {
        return new com.github.highcumontoa.reliableasynceventsjava.events.recovery
                .DefaultDeadLetterRecoveryService(store);
    }

    @Bean
    public EventDispatcher eventDispatcher(DurableWALEventStore store, HandlerRegistry registry,
                                           RetryPolicy retryPolicy, ReliableEventsProperties props) {
        return new EventDispatcher(store, registry, retryPolicy,
                props.getPollIntervalMillis(), props.getWorkerCount());
    }
}

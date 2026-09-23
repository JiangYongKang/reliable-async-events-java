package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.config.EventProperties;
import com.github.highcumontoa.reliableasynceventsjava.core.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.engine.EventHandlerRegistry;
import com.github.highcumontoa.reliableasynceventsjava.engine.EventDispatcher;
import com.github.highcumontoa.reliableasynceventsjava.engine.ReliableEventService;
import com.github.highcumontoa.reliableasynceventsjava.engine.RetryPolicy;
import com.github.highcumontoa.reliableasynceventsjava.storage.EventRepository;
import com.github.highcumontoa.reliableasynceventsjava.storage.SchemaInitializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.UUID;

/**
 * 测试基础设施：纯进程内组装（嵌入式 H2 内存库 + 引擎各组件），
 * 不启动 Spring、不依赖任何外部服务。
 */
public final class TestSupport {

    private TestSupport() {
    }

    /** 一整套引擎组件。 */
    public static class Harness {
        public final DataSource dataSource;
        public final JdbcTemplate jdbc;
        public final PlatformTransactionManager txManager;
        public final EventRepository repository;
        public final EventHandlerRegistry registry;
        public final ReliableEventService eventService;
        public final RetryPolicy retryPolicy;
        public final EventDispatcher dispatcher;

        Harness(DataSource dataSource, int maxAttempts, Duration backoff,
                Duration leaseTtl, int workerCount) {
            this.dataSource = dataSource;
            this.jdbc = new JdbcTemplate(dataSource);
            this.txManager = new DataSourceTransactionManager(dataSource);
            new SchemaInitializer(dataSource).initialize();
            this.repository = new EventRepository(dataSource);
            this.registry = new EventHandlerRegistry();
            EventProperties props = new EventProperties();
            props.setMaxAttempts(maxAttempts);
            props.setLeaseTtl(leaseTtl == null ? Duration.ofMinutes(2) : leaseTtl);
            props.setPollInterval(Duration.ofMillis(20));
            props.setWorkerCount(workerCount);
            this.retryPolicy = new RetryPolicy(maxAttempts, backoff, 2.0,
                    Duration.ofSeconds(30), 0.1);
            this.eventService = new ReliableEventService(repository, txManager);
            this.eventService.setMaxAttempts(maxAttempts);
            this.dispatcher = new EventDispatcher(repository, registry, retryPolicy,
                    txManager, props);
        }

        public void register(String type, EventHandler handler) {
            registry.register(type, handler);
        }
    }

    /** 默认：内存 H2、最大 5 次、初始退避 10ms、租约 2 分钟。 */
    public static Harness newHarness() {
        return newHarness(5, Duration.ofMillis(10), Duration.ofMinutes(2), 4);
    }

    public static Harness newHarness(int maxAttempts, Duration backoff,
                                     Duration leaseTtl, int workers) {
        DataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ((org.h2.jdbcx.JdbcDataSource) ds).setUrl(
                "jdbc:h2:mem:ev_" + UUID.randomUUID().toString().replace("-", "")
                        + ";DB_CLOSE_DELAY=-1");
        return new Harness(ds, maxAttempts, backoff, leaseTtl, workers);
    }

    /** 连接到文件库（用于跨连接/重启持久化场景）。 */
    public static Harness harnessFor(DataSource ds, int maxAttempts, Duration backoff,
                                     Duration leaseTtl, int workers) {
        return new Harness(ds, maxAttempts, backoff, leaseTtl, workers);
    }

    public static org.h2.jdbcx.JdbcDataSource fileDataSource(String path) {
        org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setUrl("jdbc:h2:file:" + path);
        return ds;
    }
}

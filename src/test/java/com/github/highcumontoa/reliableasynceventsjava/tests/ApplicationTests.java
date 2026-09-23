package com.github.highcumontoa.reliableasynceventsjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 仅验证 Spring 上下文可正常装配（所有 Bean、配置绑定、调度器生命周期）。
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:ctxdb;DB_CLOSE_DELAY=-1",
        "reliable.events.dispatcher-enabled=false"
})
class ApplicationTests {

    @Test
    void contextLoads() {
    }
}

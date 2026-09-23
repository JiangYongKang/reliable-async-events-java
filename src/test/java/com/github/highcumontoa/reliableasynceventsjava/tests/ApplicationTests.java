package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.Application;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 上下文装配冒烟测试：验证可靠事件组件（WAL、存储、发布器、分发器、控制器）均可启动。
 */
@SpringBootTest(classes = Application.class)
class ApplicationTests {

    @Test
    void contextLoads() {
    }
}

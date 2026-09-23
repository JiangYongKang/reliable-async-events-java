package com.github.highcumontoa.reliableasynceventsjava.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.core.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.core.InvalidEventException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * 处理 ORDER_CREATED 事件：写 order_projection 读模型（演示副作用）。
 *
 * <p>幂等保证（双层）：</p>
 * <ul>
 *   <li>调度器 processed_event 去重表：重复领取/重放时本处理器不会再执行；</li>
 *   <li>副作用本身用 MERGE（主键 order_id）：即便被直接调用也不会产生重复投影。</li>
 * </ul>
 *
 * <p>故障注入：按 orderId 从 {@link FaultSimulator} 读取瞬时/永久故障，
 * 由调度器统一分类为重试或死信。</p>
 */
@Component
public class OrderCreatedHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedHandler.class);

    private final JdbcTemplate jdbc;
    private final FaultSimulator faultSimulator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderCreatedHandler(DataSource dataSource, FaultSimulator faultSimulator) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.faultSimulator = faultSimulator;
    }

    @Override
    public void handle(String payload) {
        JsonNode node;
        try {
            node = objectMapper.readTree(payload);
        } catch (Exception e) {
            // 输入在处理阶段才发现非法：判定永久失败，直接死信（VALIDATION_FAILED）
            throw new InvalidEventException("ORDER_CREATED payload 不是合法 JSON: " + e.getMessage());
        }
        JsonNode orderIdNode = node.get("orderId");
        if (orderIdNode == null || !orderIdNode.canConvertToLong()) {
            throw new InvalidEventException("ORDER_CREATED payload 缺少合法 orderId: " + payload);
        }
        long orderId = orderIdNode.asLong();
        String product = node.path("product").asText("");
        int quantity = node.path("quantity").asInt(0);
        if (product.isBlank() || quantity <= 0) {
            throw new InvalidEventException(
                    "ORDER_CREATED payload 字段非法 product=" + product + " quantity=" + quantity);
        }

        // 故障注入点（瞬时/永久），异常分类由调度器完成
        faultSimulator.failPermanentIfConfigured(String.valueOf(orderId));
        faultSimulator.failTransientIfConfigured(String.valueOf(orderId));

        // 幂等副作用：投影写入与事件处理在同一事务，失败一起回滚，重试无重复副作用
        jdbc.update("MERGE INTO order_projection (order_id, product, quantity, projected_at) KEY (order_id) VALUES (?,?,?,?)",
                orderId, product, quantity, Timestamp.from(Instant.now()));
        log.info("[handler] ORDER_CREATED 投影写入完成 orderId={} product={} quantity={}",
                orderId, product, quantity);
    }
}

package com.github.highcumontoa.reliableasynceventsjava.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.reliableasynceventsjava.core.EventEnvelope;
import com.github.highcumontoa.reliableasynceventsjava.core.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.core.InvalidEventException;
import com.github.highcumontoa.reliableasynceventsjava.core.SubmitResult;
import com.github.highcumontoa.reliableasynceventsjava.engine.ReliableEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 演示业务服务：下单（orders 写入）与 ORDER_CREATED 事件投递在同一事务内完成。
 *
 * <p>一致性语义：</p>
 * <ul>
 *   <li>订单与事件同提交：订单可见 ⇒ 事件必在 outbox；事件不存在 ⇒ 订单必已回滚；</li>
 *   <li>业务幂等：orders.request_id 唯一 + outbox idempotency_key=requestId，
 *       重复/并发提交只会有一个订单、一条事件；</li>
 *   <li>输入非法：不创建订单，同时落一条 VALIDATION_FAILED 死信，原因可查。</li>
 * </ul>
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    public static final String EVENT_TYPE = "ORDER_CREATED";

    private final ReliableEventService eventService;
    private final TransactionTemplate tx;
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderService(ReliableEventService eventService,
                        PlatformTransactionManager txManager,
                        DataSource dataSource) {
        this.eventService = eventService;
        this.tx = new TransactionTemplate(txManager);
        this.tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public OrderResult placeOrder(OrderCommand command) {
        log.info("[order] 收到下单请求 requestId={} product={} quantity={}",
                command == null ? null : command.requestId(),
                command == null ? null : command.product(),
                command == null ? null : command.quantity());

        String error = validate(command);
        if (error != null) {
            log.warn("[order] 输入校验失败: {}", error);
            eventService.recordValidationFailure(
                    command == null ? null : command.requestId(), EVENT_TYPE, error);
            throw new InvalidEventException(error);
        }

        try {
            return tx.execute(status -> doPlace(command));
        } catch (DuplicateKeyException dup) {
            // 并发/重复提交：当前事务已回滚，在新事务中返回已存在的订单与事件
            return loadExisting(command.requestId(), dup);
        }
    }

    private OrderResult doPlace(OrderCommand command) {
        Instant now = Instant.now();
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO orders (request_id, product, quantity, created_at)
                    VALUES (?,?,?,?)
                    """, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, command.requestId());
            ps.setString(2, command.product());
            ps.setInt(3, command.quantity());
            ps.setTimestamp(4, Timestamp.from(now));
            return ps;
        }, kh);
        long orderId = kh.getKey().longValue();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderId", orderId);
        payload.put("requestId", command.requestId());
        payload.put("product", command.product());
        payload.put("quantity", command.quantity());
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("构造事件 payload 失败", e);
        }

        // 与订单同一事务登记 outbox：任一步失败则两者一起回滚
        SubmitResult result = eventService.submitWithinTransaction(
                new EventEnvelope(command.requestId(), EVENT_TYPE, json));
        log.info("[order] 下单成功 orderId={} requestId={} 事件 id={}（同一事务提交）",
                orderId, command.requestId(), result.eventId());
        return new OrderResult(orderId, command.requestId(), command.product(),
                command.quantity(), result.eventId(), false);
    }

    private OrderResult loadExisting(String requestId, DuplicateKeyException dup) {
        return tx.execute(status -> {
            var existingOrder = jdbc.queryForMap(
                    "SELECT id, product, quantity FROM orders WHERE request_id = ?", requestId);
            EventRecord event = eventService.findByIdempotencyKey(requestId)
                    .orElseThrow(() -> new IllegalStateException(
                            "命中订单唯一约束但缺少对应事件: " + requestId, dup));
            log.info("[order] 重复/并发提交 requestId={} 命中已有订单 orderId={} eventId={}",
                    requestId, existingOrder.get("ID"), event.id());
            return new OrderResult(((Number) existingOrder.get("ID")).longValue(),
                    requestId,
                    (String) existingOrder.get("PRODUCT"),
                    ((Number) existingOrder.get("QUANTITY")).intValue(),
                    event.id(), true);
        });
    }

    private String validate(OrderCommand c) {
        if (c == null) {
            return "下单命令为空";
        }
        if (c.requestId() == null || c.requestId().isBlank()) {
            return "requestId（业务幂等键）不能为空";
        }
        if (c.product() == null || c.product().isBlank()) {
            return "product 不能为空";
        }
        if (c.quantity() <= 0) {
            return "quantity 必须为正整数，实际: " + c.quantity();
        }
        return null;
    }
}

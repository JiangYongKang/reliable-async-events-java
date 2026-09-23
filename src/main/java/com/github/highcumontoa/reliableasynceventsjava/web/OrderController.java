package com.github.highcumontoa.reliableasynceventsjava.web;

import com.github.highcumontoa.reliableasynceventsjava.core.InvalidEventException;
import com.github.highcumontoa.reliableasynceventsjava.demo.OrderCommand;
import com.github.highcumontoa.reliableasynceventsjava.demo.OrderResult;
import com.github.highcumontoa.reliableasynceventsjava.demo.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 演示下单接口（带业务幂等键 requestId）。
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<?> placeOrder(@RequestBody OrderCommand command) {
        try {
            OrderResult result = orderService.placeOrder(command);
            return ResponseEntity.ok(result);
        } catch (InvalidEventException e) {
            // 输入校验失败：事件已以 VALIDATION_FAILED 死信落库，接口给出可解释响应
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "VALIDATION_FAILED", "message", e.getMessage()));
        }
    }
}

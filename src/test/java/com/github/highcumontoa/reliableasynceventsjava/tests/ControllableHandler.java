package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.EventRecord;
import com.github.highcumontoa.reliableasynceventsjava.events.EventHandler;
import com.github.highcumontoa.reliableasynceventsjava.events.EventValidationException;
import com.github.highcumontoa.reliableasynceventsjava.events.NonRetryableEventException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用可控处理器：按预设模式返回结果或抛出不同类别的异常，并统计实际处理次数。
 * 副作用 key 固定为 effect:&lt;eventId&gt;，重复执行会写相同值，配合 KV 覆盖验证不产生重复副作用。
 */
public class ControllableHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(ControllableHandler.class);

    public enum Mode { TRANSIENT_N_TIMES, PERMANENT, VALIDATION, OK }

    private final String type;
    private final Mode mode;
    private final int transientFailures;
    public final AtomicInteger invocationCount = new AtomicInteger();
    public final Map<String, Integer> perEventInvocations = new ConcurrentHashMap<>();

    public ControllableHandler(String type, Mode mode, int transientFailures) {
        this.type = type;
        this.mode = mode;
        this.transientFailures = transientFailures;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, String> handle(EventRecord event) {
        int n = invocationCount.incrementAndGet();
        perEventInvocations.merge(event.eventId(), 1, Integer::sum);
        log.info("  [处理器 {}] 第 {} 次被调用 eventId={} payload={}", type, n, event.eventId(), event.payload());
        switch (mode) {
            case PERMANENT -> throw new NonRetryableEventException("模拟永久故障：资源不存在（判定依据：不可重试）");
            case VALIDATION -> throw new EventValidationException("模拟消费期校验失败：payload 缺少字段 amount");
            case TRANSIENT_N_TIMES -> {
                if (n <= transientFailures) {
                    throw new RuntimeException("模拟瞬时故障：远程调用超时 connection timeout（第 " + n + " 次，判定为可重试）");
                }
            }
            case OK -> { }
        }
        return Map.of("effect:" + event.eventId(), "processed@" + n);
    }
}

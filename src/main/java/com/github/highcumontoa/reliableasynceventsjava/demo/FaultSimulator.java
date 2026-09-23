package com.github.highcumontoa.reliableasynceventsjava.demo;

import com.github.highcumontoa.reliableasynceventsjava.core.RetryableEventException;
import com.github.highcumontoa.reliableasynceventsjava.core.NonRetryableEventException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 进程内故障注入器（演示与测试共用，不依赖外部组件）：
 * <ul>
 *   <li>armTransientFailures：前 N 次处理抛 {@link RetryableEventException}（瞬时故障），之后自动恢复；</li>
 *   <li>armPermanent：持续抛 {@link NonRetryableEventException}（永久故障）；</li>
 *   <li>clear：清除该键上的注入，模拟"故障已修复"。</li>
 * </ul>
 * key 一般使用 orderId，用于把故障绑定到具体业务对象。
 */
@Component
public class FaultSimulator {

    private static final Logger log = LoggerFactory.getLogger(FaultSimulator.class);

    private final Map<String, AtomicInteger> transientFailures = new ConcurrentHashMap<>();
    private final Set<String> permanent = ConcurrentHashMap.newKeySet();

    public FaultSimulator() {
    }

    /** 瞬时故障：剩余次数 > 0 时抛 {@link RetryableEventException} 并递减，计数耗尽后自动恢复。 */
    public void failTransientIfConfigured(String key) {
        AtomicInteger counter = transientFailures.get(key);
        if (counter == null) {
            return;
        }
        int before = counter.getAndUpdate(v -> v > 0 ? v - 1 : 0);
        if (before > 0) {
            log.warn("[fault] key={} 注入瞬时故障，剩余 {} 次", key, before - 1);
            throw new RetryableEventException("注入的瞬时故障 key=" + key + "，剩余 " + (before - 1) + " 次");
        }
    }

    public int transientFailuresRemaining(String key) {
        AtomicInteger counter = transientFailures.get(key);
        return counter == null ? 0 : counter.get();
    }

    public void armTransientFailures(String key, int times) {
        transientFailures.put(key, new AtomicInteger(times));
        permanent.remove(key);
        log.info("[fault] key={} 注入 {} 次瞬时故障", key, times);
    }

    public void armPermanent(String key) {
        permanent.add(key);
        log.warn("[fault] key={} 注入永久故障（不可重试）", key);
    }

    public void failPermanentIfConfigured(String key) {
        if (permanent.contains(key)) {
            log.warn("[fault] key={} 注入永久故障", key);
            throw new NonRetryableEventException("注入的永久故障 key=" + key);
        }
    }

    public boolean isPermanent(String key) {
        return permanent.contains(key);
    }

    /** 清除注入（含永久故障），模拟运维修复。 */
    public void clear(String key) {
        transientFailures.remove(key);
        permanent.remove(key);
        log.info("[fault] key={} 故障注入已清除（模拟原因修复）", key);
    }
}

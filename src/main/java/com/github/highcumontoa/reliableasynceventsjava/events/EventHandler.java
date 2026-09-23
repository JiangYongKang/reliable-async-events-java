package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.Map;

/**
 * 某一事件类型的处理器（业务消费侧）。
 *
 * <p>失败分类约定：</p>
 * <ul>
 *   <li>抛出 {@link NonRetryableEventException}：永久故障，直接死信（NON_RETRYABLE）；</li>
 *   <li>抛出 {@link EventValidationException}：消费期输入校验失败，直接死信（VALIDATION_FAILED）；</li>
 *   <li>抛出其它任意异常：瞬时故障，按退避策略自动重试。</li>
 * </ul>
 *
 * <p>处理器返回的 KV 是本次处理的业务副作用，将与 SUCCEEDED 状态在同一磁盘批次中
 * 原子落盘；处理过程中不允许产生提交前即对外可见的副作用。</p>
 */
public interface EventHandler {

    /** 该处理器消费的事件类型。 */
    String type();

    /**
     * @return 随成功标记原子落盘的业务副作用（key -&gt; JSON 字符串），可为 null。
     */
    Map<String, String> handle(EventRecord event) throws Exception;
}

package com.github.highcumontoa.reliableasynceventsjava.core;

/**
 * 待提交事件的输入参数。
 *
 * @param idempotencyKey 业务幂等键；同键重复提交只会对应同一条事件
 * @param type           事件类型，对应一个已注册的 {@link EventHandler}
 * @param payload        事件负载（JSON 字符串）
 */
public record EventEnvelope(String idempotencyKey, String type, String payload) {
}

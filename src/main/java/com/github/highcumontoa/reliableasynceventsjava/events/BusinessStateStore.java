package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.Optional;

/**
 * 业务副作用 KV 查询视图（数据由事件存储同批落盘，保证一致）。
 */
public interface BusinessStateStore {

    Optional<String> get(String key);
}

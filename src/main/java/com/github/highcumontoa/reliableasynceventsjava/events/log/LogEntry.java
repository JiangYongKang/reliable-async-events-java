package com.github.highcumontoa.reliableasynceventsjava.events.log;

/**
 * WAL 中的一条已持久化记录：单调递增序号 + 条目类型 + JSON 内容。
 */
public record LogEntry(long sequence, String kind, String body) {
}

package com.github.highcumontoa.reliableasynceventsjava.events.log;

import java.util.List;

/**
 * 追加型预写日志。append 必须在返回前 fsync；重启时 replayAll 重放全部历史。
 */
public interface DurableLog {

    /** 追加并强制持久化（单条记录原子）。 */
    void append(String kind, String jsonBody);

    /** 重放全部已确认记录；文件尾部不完整/损坏的记录将被截断并记录日志。 */
    List<LogEntry> replayAll();

    void flush();
}

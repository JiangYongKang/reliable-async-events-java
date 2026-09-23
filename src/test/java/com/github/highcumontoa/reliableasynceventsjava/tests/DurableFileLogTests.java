package com.github.highcumontoa.reliableasynceventsjava.tests;

import com.github.highcumontoa.reliableasynceventsjava.events.log.DurableFileLog;
import com.github.highcumontoa.reliableasynceventsjava.events.log.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WAL：原子批次、重启重放、尾部撕裂写自动截断。
 */
class DurableFileLogTests {

    private static final Logger log = LoggerFactory.getLogger(DurableFileLogTests.class);

    @Test
    void batchIsAtomicAndReplayableAcrossRestart(@TempDir Path dir) {
        DurableFileLog wal = new DurableFileLog(dir.toString());
        wal.appendBatch(List.of(
                new DurableFileLog.Entry("SUBMIT", "{\"op\":\"EVENT\",\"id\":\"e1\"}"),
                new DurableFileLog.Entry("SUCCEED", "{\"op\":\"SUCCEEDED\",\"eventId\":\"e1\"}")));
        wal.append("RETRY", "{\"op\":\"WAIT\",\"eventId\":\"e2\"}");
        log.info("【输入】WAL 写入一个 2 条记录的原子批次 + 1 条单记录后重新打开文件");

        DurableFileLog wal2 = new DurableFileLog(dir.toString());
        List<LogEntry> entries = wal2.replayAll();
        assertEquals(3, entries.size(), "重启后应完整重放 3 条记录");
        assertEquals("SUBMIT", entries.get(0).kind());
        assertTrue(entries.get(0).body().contains("e1"));
        assertEquals("SUCCEED", entries.get(1).kind());
        assertEquals("RETRY", entries.get(2).kind());
        log.info("【状态变化】重放得到 {} 条记录，顺序与类型: {}", entries.size(),
                entries.stream().map(e -> e.kind() + ":" + e.sequence()).toList());
    }

    @Test
    void tornTailFrameIsDiscardedOnReplay(@TempDir Path dir) throws Exception {
        Path file;
        DurableFileLog wal = new DurableFileLog(dir.toString());
        wal.append("SUBMIT", "{\"op\":\"EVENT\",\"id\":\"good\"}");
        file = Path.of(dir.toString(), "events.wal");
        long goodLength = new java.io.File(file.toString()).length();
        log.info("【输入】已确认 1 条合法记录后，模拟崩溃在帧写一半：文件长度 {} 追加 7 个垃圾字节", goodLength);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{1, 2, 3, 4, 5, 6, 7});
            raf.getFD().sync();
        }

        DurableFileLog wal2 = new DurableFileLog(dir.toString());
        List<LogEntry> entries = wal2.replayAll();
        assertEquals(1, entries.size(), "撕裂尾部帧必须被丢弃，只保留已确认记录");
        assertTrue(entries.get(0).body().contains("good"));
        log.info("【判定依据】CRC/长度校验失败 => 截断尾部；恢复后记录数={}，文件回到长度 {}（全有或全无）",
                entries.size(), goodLength);
        assertEquals(goodLength, new java.io.File(file.toString()).length(),
                "损坏尾部应被物理截断");
    }
}

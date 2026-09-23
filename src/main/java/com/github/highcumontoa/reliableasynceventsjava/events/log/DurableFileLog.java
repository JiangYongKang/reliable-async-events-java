package com.github.highcumontoa.reliableasynceventsjava.events.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 基于单文件的追加型预写日志（WAL）。
 *
 * <p>帧格式（小端）：</p>
 * <pre>
 *   8 bytes 文件魔数 ('R','A','E','W','A','L','V','1')，仅文件起始一次
 *   每个帧：
 *     4 bytes body 长度 N
 *     4 bytes CRC32（长度字节 + body 字节）
 *     N bytes body（JSON: {"kind":..., "op":...}）
 * </pre>
 *
 * <p>写入语义：{@link #appendBatch} 将多条记录在同一把锁内连续写入后只执行一次
 * {@code fsync}；若进程在 fsync 完成前崩溃，尾部帧要么长度不全、要么 CRC 不匹配，
 * 重放时会被整体丢弃并截断文件，因此一个提交批次具备“全有或全无”的原子可见性。</p>
 */
public class DurableFileLog implements DurableLog {

    private static final Logger log = LoggerFactory.getLogger(DurableFileLog.class);

    private static final byte[] MAGIC = "RAEWALV1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_LEN = 4 + 4;

    private final Path file;
    private final ObjectMapper mapper;
    private final Object writeLock = new Object();

    public DurableFileLog(String storeDir) {
        try {
            Path dir = Path.of(storeDir);
            Files.createDirectories(dir);
            this.file = dir.resolve("events.wal");
            this.mapper = new ObjectMapper();
            if (Files.notExists(file) || Files.size(file) == 0) {
                try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
                    raf.write(MAGIC);
                    raf.getFD().sync();
                }
            } else {
                verifyMagic();
            }
        } catch (IOException e) {
            throw new IllegalStateException("初始化 WAL 失败: " + storeDir, e);
        }
    }

    private void verifyMagic() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            byte[] head = new byte[8];
            int read = raf.read(head);
            if (read != 8 || !java.util.Arrays.equals(head, MAGIC)) {
                throw new IllegalStateException("WAL 文件魔数不匹配，文件可能已损坏: " + file);
            }
        }
    }

    @Override
    public void append(String kind, String jsonBody) {
        appendBatch(List.of(new Entry(kind, jsonBody)));
    }

    /** 多条记录作为一个原子批次追加，仅执行一次 fsync。 */
    public void appendBatch(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        synchronized (writeLock) {
            try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
                raf.seek(raf.length());
                for (Entry e : entries) {
                    byte[] body = mapper.writeValueAsBytes(new Frame(e.kind(), mapper.readTree(e.jsonBody())));
                    byte[] len = intLE(body.length);
                    raf.write(len);
                    raf.write(intLE(crc32(len, body)));
                    raf.write(body);
                }
                raf.getFD().sync();
            } catch (IOException e) {
                throw new IllegalStateException("WAL 追加失败: " + file, e);
            }
        }
    }

    @Override
    public List<LogEntry> replayAll() {
        synchronized (writeLock) {
            List<LogEntry> entries = new ArrayList<>();
            long validEnd;
            try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
                long length = raf.length();
                byte[] magic = new byte[8];
                if (raf.read(magic) != 8 || !java.util.Arrays.equals(magic, MAGIC)) {
                    throw new IllegalStateException("WAL 文件魔数不匹配: " + file);
                }
                long pos = 8;
                long seq = 0;
                while (pos < length) {
                    long frameStart = pos;
                    byte[] header = new byte[HEADER_LEN];
                    raf.seek(pos);
                    int got = raf.read(header);
                    if (got < HEADER_LEN) {
                        log.warn("WAL 尾部帧头不完整（{}<{} 字节），截断至 offset={}", got, HEADER_LEN, frameStart);
                        break;
                    }
                    pos += HEADER_LEN;
                    int bodyLen = readIntLE(header, 0);
                    long crc = readIntLE(header, 4) & 0xffffffffL;
                    long afterFrame = pos + bodyLen;
                    if (bodyLen < 0 || afterFrame > length) {
                        log.warn("WAL 帧声明长度 {} 超出文件剩余范围，判定为撕裂写，截断至 offset={}",
                                bodyLen, frameStart);
                        break;
                    }
                    byte[] body = new byte[bodyLen];
                    raf.readFully(body);
                    pos += bodyLen;
                    byte[] lenBytes = new byte[4];
                    System.arraycopy(header, 0, lenBytes, 0, 4);
                    if ((crc32(lenBytes, body) & 0xffffffffL) != crc) {
                        log.warn("WAL 帧 CRC 校验失败（offset={}），判定为撕裂写，截断", frameStart);
                        break;
                    }
                    JsonNode node = mapper.readTree(body);
                    entries.add(new LogEntry(seq++, node.get("kind").asText(),
                            node.get("op").toString()));
                }
                validEnd = pos;
                if (validEnd < length) {
                    truncate(validEnd);
                }
            } catch (IOException e) {
                throw new IllegalStateException("WAL 重放失败: " + file, e);
            }
            return entries;
        }
    }

    private void truncate(long validEnd) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(validEnd);
            raf.getFD().sync();
        }
        log.info("WAL 已截断损坏尾部，新文件长度={}", validEnd);
    }

    @Override
    public void flush() {
        synchronized (writeLock) {
            try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
                raf.getFD().sync();
            } catch (IOException e) {
                throw new IllegalStateException("WAL flush 失败", e);
            }
        }
    }

    private static int crc32(byte[] lenBytes, byte[] body) {
        CRC32 c = new CRC32();
        c.update(lenBytes);
        c.update(body);
        return (int) c.getValue();
    }

    private static byte[] intLE(int v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    private static int readIntLE(byte[] b, int off) {
        return (b[off] & 0xff)
                | ((b[off + 1] & 0xff) << 8)
                | ((b[off + 2] & 0xff) << 16)
                | ((b[off + 3]) << 24);
    }

    /** 待追加的一条记录：kind 为条目类型，jsonBody 为内容 JSON 字符串。 */
    public record Entry(String kind, String jsonBody) {
    }

    private record Frame(String kind, JsonNode op) {
    }
}

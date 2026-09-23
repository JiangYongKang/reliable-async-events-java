package com.github.highcumontoa.reliableasynceventsjava.events.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 可靠事件组件配置（前缀 reliable-events）。
 */
@ConfigurationProperties(prefix = "reliable-events")
public class ReliableEventsProperties {

    /** WAL 文件目录（进程内本地存储，无外部中间件）。 */
    private String storeDir = "data/reliable-events";

    /** 最大尝试次数（含首次），达到后进入死信。 */
    private int maxAttempts = 5;

    /** 首次重试前基础退避（毫秒）。 */
    private long baseBackoffMillis = 200;

    /** 指数退避上限（毫秒）。 */
    private long maxBackoffMillis = 5_000;

    /** 退避随机抖动（毫秒），避免并发重试同时唤醒。 */
    private long jitterMillis = 50;

    /** 消费租约时长（毫秒）：消费者崩溃后事件在此时间后可被重新领取。 */
    private long leaseMillis = 30_000;

    /** 分发器轮询间隔（毫秒）。 */
    private long pollIntervalMillis = 50;

    /** 消费线程数。 */
    private int workerCount = 2;

    public String getStoreDir() { return storeDir; }
    public void setStoreDir(String storeDir) { this.storeDir = storeDir; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBaseBackoffMillis() { return baseBackoffMillis; }
    public void setBaseBackoffMillis(long v) { this.baseBackoffMillis = v; }
    public long getMaxBackoffMillis() { return maxBackoffMillis; }
    public void setMaxBackoffMillis(long v) { this.maxBackoffMillis = v; }
    public long getJitterMillis() { return jitterMillis; }
    public void setJitterMillis(long jitterMillis) { this.jitterMillis = jitterMillis; }
    public long getLeaseMillis() { return leaseMillis; }
    public void setLeaseMillis(long leaseMillis) { this.leaseMillis = leaseMillis; }
    public long getPollIntervalMillis() { return pollIntervalMillis; }
    public void setPollIntervalMillis(long v) { this.pollIntervalMillis = v; }
    public int getWorkerCount() { return workerCount; }
    public void setWorkerCount(int workerCount) { this.workerCount = workerCount; }
}

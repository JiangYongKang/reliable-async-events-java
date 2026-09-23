package com.github.highcumontoa.reliableasynceventsjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * reliable.events.* 配置项。
 */
@ConfigurationProperties(prefix = "reliable.events")
public class EventProperties {

    private boolean dispatcherEnabled = true;
    private int workerCount = 2;
    private Duration pollInterval = Duration.ofMillis(500);
    private int maxAttempts = 5;
    private Duration initialBackoff = Duration.ofMillis(200);
    private Duration maxBackoff = Duration.ofSeconds(30);
    private double backoffMultiplier = 2.0;
    private double jitterFactor = 0.2;
    private Duration leaseTtl = Duration.ofMinutes(2);
    private int batchSize = 32;

    public EventProperties() {
    }

    // getters / setters 占位，实现阶段补齐
    public boolean isDispatcherEnabled() { return dispatcherEnabled; }
    public void setDispatcherEnabled(boolean dispatcherEnabled) { this.dispatcherEnabled = dispatcherEnabled; }
    public int getWorkerCount() { return workerCount; }
    public void setWorkerCount(int workerCount) { this.workerCount = workerCount; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialBackoff() { return initialBackoff; }
    public void setInitialBackoff(Duration initialBackoff) { this.initialBackoff = initialBackoff; }
    public Duration getMaxBackoff() { return maxBackoff; }
    public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public double getJitterFactor() { return jitterFactor; }
    public void setJitterFactor(double jitterFactor) { this.jitterFactor = jitterFactor; }
    public Duration getLeaseTtl() { return leaseTtl; }
    public void setLeaseTtl(Duration leaseTtl) { this.leaseTtl = leaseTtl; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

}

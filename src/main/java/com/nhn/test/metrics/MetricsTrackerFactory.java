package com.nhn.test.metrics;

public interface MetricsTrackerFactory {
    IMetricsTracker create(String poolName, PoolStats poolStats);
}

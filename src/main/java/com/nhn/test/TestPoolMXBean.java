package com.nhn.test;

import javax.sql.DataSource;

public interface TestPoolMXBean {
    int getIdleConnections();
    int getActiveConnections();
    int getTotalConnections();
    int getThreadsAwaitingConnection();
    void softEvictConnections();
    void suspendPool();
    void resumePool();
}

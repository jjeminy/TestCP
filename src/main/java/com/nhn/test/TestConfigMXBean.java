package com.nhn.test;

public interface TestConfigMXBean {
    long getConnectionTimeout();
    void setConnectionTimeout(long connectionTimeoutMs);
    long getValidationTimeout();
    void setValidationTimeout(long validationTimeoutMs);
    long getIdleTimeout();
    void setIdleTimeout(long idleTimeoutMs);
    long getLeakDetectionThreshold();
    void setLeakDetectionThreshold(long leakDetectionThresholdMs);
    long getMaxLifetime();
    void setMaxLifetime(long maxLifetimeMs);
    int getMinimumIdle();
    void setMinimumIdle(int minIdle);
    int getMaximumPoolSize();
    void setMaximumPoolSize(int maxPoolSize);
    void setPassword(String password);
    void setUsername(String username);

    String getPoolName();
    String getCatalog();
    void setCatalog(String catalog);
}

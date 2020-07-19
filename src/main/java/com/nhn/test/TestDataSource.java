package com.nhn.test;

import com.nhn.test.pool.TestPool;
import com.nhn.test.pool.TestPool.PoolInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.nhn.test.pool.TestPool.POOL_NORMAL;

public class TestDataSource extends TestConfig implements DataSource, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestDataSource.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final TestPool fastPathPool;
    private volatile TestPool pool;

    public TestDataSource() {
        super();
        fastPathPool = null;
    }

    public TestDataSource(TestConfig configuration) {
        configuration.validate();
        configuration.copyStateTo(this);

        LOGGER.info("{} - Starting...", configuration.getPoolName());
        pool = fastPathPool = new TestPool(this);
        LOGGER.info("{} - Start completed", configuration.getPoolName());

        this.seal();
    }

    // ***********************************************************************
    //                          DataSource methods
    // ***********************************************************************

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("TestDataSource " + this + " has been closed.");
        }

        if (fastPathPool != null) {
            return fastPathPool.getConnection();
        }

        TestPool result = pool;
        if (result == null) {
            synchronized (this) {
                result = pool;
                if (result == null) {
                    validate();
                    LOGGER.info("{} - Starting...", getPoolName());
                    try {
                        pool = result = new TestPool(this);
                        this.seal();
                    }
                    catch (PoolInitializationException pie) {
                        if (pie.getCause() instanceof SQLException) {
                            throw (SQLException) pie.getCause();
                        }
                        else {
                            throw pie;
                        }
                    }
                    LOGGER.info("{} - Start completed.", getPoolName());
                }
            }
        }

        return result.getConnection();
    }

    /** {@inheritDoc} */
    @Override
    public Connection getConnection(String username, String password) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override
    public PrintWriter getLogWriter() throws SQLException
    {
        TestPool p = pool;
        return (p != null ? p.getUnwrappedDataSource().getLogWriter() : null);
    }

    /** {@inheritDoc} */
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException
    {
        TestPool p = pool;
        if (p != null) {
            p.getUnwrappedDataSource().setLogWriter(out);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException
    {
        TestPool p = pool;
        if (p != null) {
            p.getUnwrappedDataSource().setLoginTimeout(seconds);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getLoginTimeout() throws SQLException
    {
        TestPool p = pool;
        return (p != null ? p.getUnwrappedDataSource().getLoginTimeout() : 0);
    }

    /** {@inheritDoc} */
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.isInstance(this)) {
            return (T) this;
        }

        TestPool p = pool;
        if (p != null) {
            final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
            if (iface.isInstance(unwrappedDataSource)) {
                return (T) unwrappedDataSource;
            }

            if (unwrappedDataSource != null) {
                return unwrappedDataSource.unwrap(iface);
            }
        }

        throw new SQLException("Wrapped DataSource is not an instance of " + iface);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        if (iface.isInstance(this)) {
            return true;
        }

        TestPool p = pool;
        if (p != null) {
            final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
            if (iface.isInstance(unwrappedDataSource)) {
                return true;
            }

            if (unwrappedDataSource != null) {
                return unwrappedDataSource.isWrapperFor(iface);
            }
        }

        return false;
    }

    // ***********************************************************************
    //                        TestConfigMXBean methods
    // ***********************************************************************

//    @Override
//    public void setMetricRegistry(Object metricRegistry) {
//        boolean isAlreadySet = getMetricRegistry() != null;
//        super.setMetricRegistry(metricRegistry);
//
//        TestPool p = pool;
//        if (p != null) {
//            if (isAlreadySet) {
//                throw new IllegalStateException("MetricRegistry can only be set one time");
//            }
//            else {
//                p.setMetricRegistry(super.getMetricRegistry());
//            }
//        }
//    }
//
//    @Override
//    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
//        boolean isAlreadySet = getMetricsTrackerFactory() != null;
//        super.setMetricsTrackerFactory(metricsTrackerFactory);
//
//        TestPool p = pool;
//        if (p != null) {
//            if (isAlreadySet) {
//                throw new IllegalStateException("MetricsTrackerFactory can only be set one time");
//            }
//            else {
//                p.setMetricsTrackerFactory(super.getMetricsTrackerFactory());
//            }
//        }
//    }
//
//    @Override
//    public void setHealthCheckRegistry(Object healthCheckRegistry) {
//        boolean isAlreadySet = getHealthCheckRegistry() != null;
//        super.setHealthCheckRegistry(healthCheckRegistry);
//
//        TestPool p = pool;
//        if (p != null) {
//            if (isAlreadySet) {
//                throw new IllegalStateException("HealthCheckRegistry can only be set one time");
//            }
//            else {
//                p.setHealthCheckRegistry(super.getHealthCheckRegistry());
//            }
//        }
//    }

    // ***********************************************************************
    //                        TestCP-specific methods
    // ***********************************************************************

    public boolean isRunning() {
        return pool != null && pool.poolState == POOL_NORMAL;
    }

    public TestPoolMXBean getTestPoolMXBean() {
        return pool;
    }

    public TestConfigMXBean getTestConfigMXBean() {
        return this;
    }

    public void evictConnection(Connection connection) {
        TestPool p;
        if (!isClosed() && (p = pool) != null && connection.getClass().getName().startsWith("com.nhn.test")) {
            p.evictConnection(connection);
        }
    }

    @Override
    public void close() {
        if (isShutdown.getAndSet(true)) {
            return;
        }

        TestPool p = pool;
        if (p != null) {
            try {
                LOGGER.info("{} - Shutdown initiated...", getPoolName());
                p.shutdown();
                LOGGER.info("{} - Shutdown completed.", getPoolName());
            }
            catch (InterruptedException e) {
                LOGGER.warn("{} - Interrupted during closing", getPoolName(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isClosed() {
        return isShutdown.get();
    }

    @Override
    public String toString() {
        return "TestDataSource (" + pool + ")";
    }
}

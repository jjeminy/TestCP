package com.nhn.test.pool;

import com.nhn.test.util.ConcurrentBag.IConcurrentBagEntry;
import com.nhn.test.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.nhn.test.util.ClockSource.*;

public class PoolEntry implements IConcurrentBagEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolEntry.class);
    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;

    Connection connection;
    long lastAccessed;
    long lastBorrowed;

    private volatile int state = 0;
    private volatile boolean evict;

    private volatile ScheduledFuture<?> endOfLife;

    private final FastList<Statement> openStatements;
    private final TestPool testPool;

    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    static {
        stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");
    }

    PoolEntry(final Connection connection, final PoolBase pool, final boolean isReadOnly, final Boolean isAutoCommit) {
        this.connection = connection;
        this.testPool = (TestPool) pool;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        this.lastAccessed = currentTime();
        this.openStatements = new FastList<>(Statement.class, 16);
    }

    void recycle(final long lastAccessed)
    {
        if (connection != null) {
            this.lastAccessed = lastAccessed;
            testPool.recycle(this);
        }
    }

    void setFutureEol(final ScheduledFuture<?> endOfLife) {
        this.endOfLife = endOfLife;
    }

    Connection createProxyConnection(final ProxyLeakTask leakTask, final long now) {
        return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
        testPool.resetConnectionState(connection, proxyConnection, dirtyBits);
    }

    String getPoolName() {
        return testPool.toString();
    }

    boolean isMarkedEvicted() {
        return evict;
    }

    void markEvicted() {
        this.evict = true;
    }

    void evict(final String closureReason) {
        testPool.closeConnection(this, closureReason);
    }

    long getMillisSinceBorrowed() {
        return elapsedMillis(lastBorrowed);
    }

    PoolBase getPoolBase() {
        return testPool;
    }

    @Override
    public String toString() {
        final long now = currentTime();
        return connection
                + ", accessed " + elapsedDisplayString(lastAccessed, now) + " ago, "
                + stateToString();
    }

    // ***********************************************************************
    //                      IConcurrentBagEntry methods
    // ***********************************************************************

    @Override
    public boolean compareAndSet(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    @Override
    public void setState(int update) {
        stateUpdater.set(this, update);
    }

    @Override
    public int getState() {
        return stateUpdater.get(this);
    }

    Connection close()
    {
        ScheduledFuture<?> eol = endOfLife;
        if (eol != null && !eol.isDone() && !eol.cancel(false)) {
            LOGGER.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
        }

        Connection con = connection;
        connection = null;
        endOfLife = null;
        return con;
    }

    private String stateToString()
    {
        switch (state) {
            case STATE_IN_USE:
                return "IN_USE";
            case STATE_NOT_IN_USE:
                return "NOT_IN_USE";
            case STATE_REMOVED:
                return "REMOVED";
            case STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid";
        }
    }
}

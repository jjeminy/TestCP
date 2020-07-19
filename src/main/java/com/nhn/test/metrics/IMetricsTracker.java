package com.nhn.test.metrics;

public interface IMetricsTracker extends AutoCloseable {
    default void recordConnectionCreatedMillis(long connectionCreatedMillis) {}
    default void recordConnectionAcquiredNanos(final long elapsedAcquiredNanos) {}
    default void recordConnectionUsageMillis(final long elapsedBorrowedMillis) {}
    default void recordConnectionTimeout() {}

    @Override
    default void close() {}
}

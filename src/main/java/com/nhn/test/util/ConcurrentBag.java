package com.nhn.test.util;

import com.nhn.test.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.nhn.test.util.ClockSource.currentTime;
import static com.nhn.test.util.ClockSource.elapsedNanos;
import static com.nhn.test.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

    private final CopyOnWriteArrayList<T> sharedList;
    private final boolean weakThreadLocals;

    private final ThreadLocal<List<Object>> threadList;
    private final IBagStateListener listener;
    private final AtomicInteger waiters;
    private volatile boolean closed;

    private final SynchronousQueue<T> handoffQueue;

    public interface IConcurrentBagEntry {
        int STATE_NOT_IN_USE = 0;
        int STATE_IN_USE = 1;
        int STATE_REMOVED = -1;
        int STATE_RESERVED = -2;

        boolean compareAndSet(int expectState, int newState);
        void setState(int newState);
        int getState();
    }

    public interface IBagStateListener {
        void addBagItem(int waiting);
    }

    public ConcurrentBag(final IBagStateListener listener) {
        this.listener = listener;
        this.weakThreadLocals = useWeakThreadLocals();

        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        if (weakThreadLocals) {
            this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
        } else {
            this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
        }
    }

    public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException {
        // Try the thread-local list first
        final List<Object> list = threadList.get();
        for (int i = list.size() - 1; i >= 0; i--) {
            final Object entry = list.remove(i);
            @SuppressWarnings("unchecked")
            final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
            if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                return bagEntry;
            }
        }

        // Otherwise, scan the shared list ... then poll the handoff queue
        final int waiting = waiters.incrementAndGet();
        try {
            for (T bagEntry : sharedList) {
                if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    // If we may have stolen another waiter's connection, request another bag add.
                    if (waiting > 1) {
                        listener.addBagItem(waiting - 1);
                    }
                    return bagEntry;
                }
            }

            listener.addBagItem(waiting);

            timeout = timeUnit.toNanos(timeout);
            do {
                final long start = currentTime();
                final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
                if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    return bagEntry;
                }

                timeout -= elapsedNanos(start);
            } while (timeout > 10_000);

            return null;
        }
        finally {
            waiters.decrementAndGet();
        }
    }

    public void requite(final T bagEntry) {
        bagEntry.setState(STATE_NOT_IN_USE);

        for (int i = 0; waiters.get() > 0; i++) {
            if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
                return;
            }
            else if ((i & 0xff) == 0xff) {
                parkNanos(MICROSECONDS.toNanos(10));
            }
            else {
                Thread.yield();
            }
        }

        final List<Object> threadLocalList = threadList.get();
        if (threadLocalList.size() < 50) {
            threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
        }
    }

    public void add(final T bagEntry) {
        if (closed) {
            LOGGER.info("ConcurrentBag has been closed, ignoring add()");
            throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
        }

        sharedList.add(bagEntry);

        // spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
        }
    }

    public boolean remove(final T bagEntry) {
        if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
            return false;
        }

        final boolean removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
        }

        threadList.get().remove(bagEntry);

        return removed;
    }

    @Override
    public void close() {
        closed = true;
    }

    public List<T> values(final int state) {
        final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    @SuppressWarnings("unchecked")
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    public boolean reserve(final T bagEntry) {
        return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
    }

    @SuppressWarnings("SpellCheckingInspection")
    public void unreserve(final T bagEntry) {
        if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
            // spin until a thread takes it or none are waiting
            while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
                Thread.yield();
            }
        }
        else {
            LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
        }
    }

    public int getWaitingThreadCount() {
        return waiters.get();
    }

    public int getCount(final int state) {
        int count = 0;
        for (IConcurrentBagEntry e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    public int[] getStateCounts() {
        final int[] states = new int[6];
        for (IConcurrentBagEntry e : sharedList) {
            ++states[e.getState()];
        }
        states[4] = sharedList.size();
        states[5] = waiters.get();

        return states;
    }

    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> LOGGER.info(entry.toString()));
    }

    private boolean useWeakThreadLocals() {
        try {
            if (System.getProperty("com.nhn.test.useWeakReferences") != null) {
                return Boolean.getBoolean("com.nhn.test.useWeakReferences");
            }

            return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
        } catch (SecurityException se) {
            return true;
        }
    }
}

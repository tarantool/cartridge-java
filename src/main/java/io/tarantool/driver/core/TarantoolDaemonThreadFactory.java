package io.tarantool.driver.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom thread factory for scheduled executor service that creates daemon threads. Otherwise,
 * applications that neglect to close the client will not exit.
 *
 * <p>This class is not part of the public API.</p>
 *
 * @author Sergey Volgin
 */
public class TarantoolDaemonThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public TarantoolDaemonThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix + "-" + POOL_NUMBER.incrementAndGet() + "-thread-";
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        Thread thread = new Thread(runnable, namePrefix + threadNumber.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    }
}

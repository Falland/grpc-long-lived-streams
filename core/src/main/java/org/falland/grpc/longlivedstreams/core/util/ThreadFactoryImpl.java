package org.falland.grpc.longlivedstreams.core.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryImpl implements ThreadFactory {

    private final ThreadFactory defaultThreadFactory;
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private final String namePrefix;
    private final boolean isDaemon;

    public ThreadFactoryImpl(ThreadFactory threadFactory, String namePrefix, boolean isDaemon) {
        this.defaultThreadFactory = threadFactory;
        this.namePrefix = namePrefix.endsWith("-") ? namePrefix : namePrefix + "-";
        this.isDaemon = isDaemon;
    }

    public ThreadFactoryImpl(String namePrefix, boolean isDaemon) {
       this(Executors.defaultThreadFactory(), namePrefix, isDaemon);
    }

    public ThreadFactoryImpl(String namePrefix) {
       this(Executors.defaultThreadFactory(), namePrefix, false);
    }

    @Override
    public Thread newThread(@SuppressWarnings("NullableProblems") Runnable runnable) {
        Thread thread = defaultThreadFactory.newThread(runnable);
        thread.setDaemon(isDaemon);
        thread.setName(namePrefix + threadCounter.getAndIncrement());
        return thread;
    }
}

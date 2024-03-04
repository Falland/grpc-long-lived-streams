package org.falland.grpc.longlivedstreams.core.strategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExceptionOnOverflow<V> implements BackpressureStrategy<V> {
    public static final String TOO_SLOW_EXCEPTION_MESSAGE =
            "The sending queue is filled, receiver is not keeping up and should be stopped.";
    private final BlockingQueue<V> updatesQueue;

    public ExceptionOnOverflow(int queueSize) {
        this.updatesQueue = new LinkedBlockingQueue<>(queueSize);
    }

    @Override
    public void offer(V value) {
        if (!updatesQueue.offer(value)) {
            throw new YouAreTooSlowException(TOO_SLOW_EXCEPTION_MESSAGE);
        }
    }

    @Override
    public V poll() {
        return updatesQueue.poll();
    }

    @Override
    public void stop() {
        //ToDo add isActive flag
        updatesQueue.clear();
    }
}

package org.falland.grpc.longlivedstreams.core.strategy;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class BlockProducerOnOverflow<V> implements BackpressureStrategy<V> {
    private final int queueSize;
    private final Semaphore semaphore;
    private final Queue<V> updatesQueue;
    private volatile boolean isActive = true;

    public BlockProducerOnOverflow(int queueSize) {
        this.queueSize = queueSize;
        this.semaphore = new Semaphore(queueSize);
        this.updatesQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void offer(V value) {
        try {
            semaphore.acquire();
            if (isActive) {
                updatesQueue.add(value);
            } else {
                semaphore.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            //Do nothing we just stop if interrupted
        }
    }

    @Override
    public V poll() {
        V message = updatesQueue.poll();
        if (message != null) {
            semaphore.release();
        }
        return message;
    }

    @Override
    public void stop() {
        isActive = false;
        //we don't care of keeping the invariant on permits count here, we want to unblock all waiting threads
        semaphore.release(queueSize);
        updatesQueue.clear();
    }
}

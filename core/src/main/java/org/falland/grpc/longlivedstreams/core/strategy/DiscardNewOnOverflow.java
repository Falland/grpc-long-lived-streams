package org.falland.grpc.longlivedstreams.core.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class DiscardNewOnOverflow<V> implements BackpressureStrategy<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardNewOnOverflow.class);
    private final Queue<V> updatesQueue;
    private final int queueSize;

    public DiscardNewOnOverflow(int queueSize) {
        this.queueSize = queueSize;
        this.updatesQueue = new ArrayBlockingQueue<>(this.queueSize);
    }

    @Override
    public void offer(V value) {
        if (!updatesQueue.offer(value)) {
            LOGGER.debug("Dropping incoming message {} as the queue is full. Queue size {}", value, queueSize);
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
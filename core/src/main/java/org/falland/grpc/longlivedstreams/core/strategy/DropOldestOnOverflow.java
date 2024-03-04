package org.falland.grpc.longlivedstreams.core.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class DropOldestOnOverflow<V> implements BackpressureStrategy<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropOldestOnOverflow.class);
    private final Queue<V> updatesQueue;
    private final int queueSize;

    public DropOldestOnOverflow(int queueSize) {
        this.queueSize = queueSize;
        this.updatesQueue = new ArrayBlockingQueue<>(this.queueSize);
    }

    @Override
    public void offer(V value) {
        //there's a potential race with another producer, hence we need a loop in case we've deleted message and another thread took the free space
        while (!updatesQueue.offer(value)) {
            V toDrop = updatesQueue.poll();
            LOGGER.debug("Dropping oldest message in queue {}, queue is overflown. queue size {}", toDrop, queueSize);
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
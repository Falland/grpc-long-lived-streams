package org.falland.grpc.longlivedstreams.core.strategy;

/**
 * This interface represents the API of the back pressure strategy
 * It can be offered a message and message can be polled from it.
 * It is up to a particular implementation to decide whether to accept the offered message or discard it, or maybe block the producer
 * The {@link #poll} is usually straight-forward - if there's an eligible message then it will be returned by this method
 * Note! Both {@link #offer(V)} and {@link #poll()} methods has to be thread safe as they can be called by different threads concurrently
 * @param <V> - the type of the message handled by the strategy
 */
public interface BackpressureStrategy<V> {

    void offer(V value);

    V poll();

    void stop();
}

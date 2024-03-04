package org.falland.grpc.longlivedstreams.client;

/**
 * The interface that represents the consumer of streaming data
 * @param <U> stream message type
 */
public interface UpdateProcessor<U> {

    void processUpdate(U update);
}

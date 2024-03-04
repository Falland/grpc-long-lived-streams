package org.falland.grpc.longlivedstreams.client;

import io.grpc.internal.GrpcUtil;

import java.util.concurrent.Executor;

/**
 * The configuration class for the gRPC client.
 */
public interface ClientConfiguration {
    String hostName();

    int port();

    Executor executor();

    String clientName();

    default boolean usePlaintext() {
        return true;
    }

    default boolean reconnectOnComplete() {
        return false;
    }

    default int maxInboundMessageSize() {
        return GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;
    }

    /**
     * Max number of reconnection attempts before re-creating the channel.
     * Defaults to MAX_VALUE, meaning channel is re-used.
     */
    default int maxAttemptsBeforeReconnect() {
        return Integer.MAX_VALUE;
    }
}
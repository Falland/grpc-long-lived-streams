package io.grpc.longlivedstreams.client;

import io.grpc.internal.GrpcUtil;

public interface ClientContext {
    String hostName();

    int port();

    String getClientName();

    boolean usePlaintext();

    default int getMaxInboundMessageSize() {
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
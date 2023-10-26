package org.falland.grpc.longlivedstreams.client;

import io.grpc.internal.GrpcUtil;

import java.util.concurrent.Executor;

public interface ClientContext {
    String hostName();

    int port();

    Executor executor();

    String clientName();

    boolean usePlaintext();

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
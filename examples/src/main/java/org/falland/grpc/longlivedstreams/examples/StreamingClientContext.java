package org.falland.grpc.longlivedstreams.examples;

import org.falland.grpc.longlivedstreams.client.ClientContext;

import java.util.concurrent.Executor;

public class StreamingClientContext implements ClientContext {
    private final String hostName;
    private final String clientName;
    private final int port;

    private final Executor executor;

    public StreamingClientContext(String hostName, String clientName, int port, Executor executor) {
        this.hostName = hostName;
        this.clientName = clientName;
        this.port = port;
        this.executor = executor;
    }

    @Override
    public String hostName() {
        return hostName;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public Executor executor() {
        return executor;
    }

    @Override
    public String clientName() {
        return clientName;
    }

    @Override
    public boolean usePlaintext() {
        return true;
    }
}

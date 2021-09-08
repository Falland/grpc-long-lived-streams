package io.grpc.longlivedstreams.examples;

import io.grpc.longlivedstreams.client.ClientContext;

public class StreamingClientContext implements ClientContext {
    private final String hostName;
    private final String clientName;
    private final int port;

    public StreamingClientContext(String hostName, String clientName, int port) {
        this.hostName = hostName;
        this.clientName = clientName;
        this.port = port;
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
    public String getClientName() {
        return clientName;
    }

    @Override
    public boolean usePlaintext() {
        return true;
    }
}

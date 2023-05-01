package org.falland.grpc.longlivedstreams.client;

public interface GrpcClient {

    /**
     * Client should use this method to start
     * */
    @SuppressWarnings("squid:S00112")
    void start() throws Exception;

    void stop();
}
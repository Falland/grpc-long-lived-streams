package com.falland.grpc.longlivedstreams.client;

public interface UpdateProcessor<U> {

    void processUpdate(U update);
}

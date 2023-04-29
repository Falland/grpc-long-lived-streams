package com.falland.grpc.longlivedstreams.client;

import io.grpc.ConnectivityState;

public interface GrpcConnectionListener {

    /**
     * Called when connectivity state is changed
     */
    void onStateChanged(String channelName, ConnectivityState state);
}

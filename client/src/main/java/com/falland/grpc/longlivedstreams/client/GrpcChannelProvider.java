package com.falland.grpc.longlivedstreams.client;

import io.grpc.ManagedChannel;

public interface GrpcChannelProvider {
    ManagedChannel getChannel();
}

package org.falland.grpc.longlivedstreams.core;

import io.grpc.stub.StreamObserver;

public interface SubscriptionObserver<V> extends StreamObserver<V> {

    boolean isReady();

    boolean isOpened();
}

package org.falland.grpc.longlivedstreams.core;

import io.grpc.stub.StreamObserver;

public interface ControlledStreamObserver<V> extends StreamObserver<V> {

    boolean isReady();

    boolean isOpened();
}

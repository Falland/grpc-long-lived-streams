package org.falland.grpc.longlivedstreams.server.streaming;

import org.falland.grpc.longlivedstreams.core.GrpcStream;
import org.falland.grpc.longlivedstreams.core.streams.StreamType;

public class ServerGrpcStream<U> implements GrpcStream<U> {

    private final SubscriptionKey subscriptionKey;
    private final GrpcStream<U> delegate;

    public ServerGrpcStream(SubscriptionKey subscriptionKey, GrpcStream<U> delegate) {
        this.subscriptionKey = subscriptionKey;
        this.delegate = delegate;
    }

    public SubscriptionKey subscriptionKey() {
        return subscriptionKey;
    }

    @Override
    public void onNext(U update) {
        delegate.onNext(update);
    }

    @Override
    public boolean isActive() {
        return delegate.isActive();
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }

    @Override
    public StreamType type() {
        return delegate.type();
    }
}

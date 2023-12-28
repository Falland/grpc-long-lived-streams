package org.falland.grpc.longlivedstreams.server.streaming;

import org.falland.grpc.longlivedstreams.core.subscription.GrpcSubscription;
import org.falland.grpc.longlivedstreams.core.subscription.SubscriptionType;

public class ServerGrpcSubscription<U> implements GrpcSubscription<U> {

    private final SubscriptionKey subscriptionKey;
    private final GrpcSubscription<U> delegate;

    public ServerGrpcSubscription(SubscriptionKey subscriptionKey, GrpcSubscription<U> delegate) {
        this.subscriptionKey = subscriptionKey;
        this.delegate = delegate;
    }

    public SubscriptionKey subscriptionKey() {
        return subscriptionKey;
    }

    @Override
    public void processUpdate(U update) {
        delegate.processUpdate(update);
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
    public SubscriptionType type() {
        return delegate.type();
    }
}

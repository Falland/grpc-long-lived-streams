package io.grpc.longlivedstreams.server.subscription;

import java.util.function.Predicate;

public class FilteringGrpcSubscription<U> implements GrpcSubscription<U> {

    private final GrpcSubscription<U> delegate;
    private final Predicate<U> filter;

    public FilteringGrpcSubscription(GrpcSubscription<U> delegate, Predicate<U> filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public String getAddress() {
        return delegate.getAddress();
    }

    @Override
    public SubscriptionType getType() {
        return delegate.getType();
    }

    @Override
    public String getClientId() {
        return delegate.getClientId();
    }

    @Override
    public void processUpdate(U update) {
        if (filter.test(update)) {
            delegate.processUpdate(update);
        }
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
}

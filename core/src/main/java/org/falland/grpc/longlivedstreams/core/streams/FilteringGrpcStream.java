package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.GrpcStream;

import java.util.function.Predicate;

public class FilteringGrpcStream<U> implements GrpcStream<U> {

    private final GrpcStream<U> delegate;
    private final Predicate<U> filter;

    public FilteringGrpcStream(GrpcStream<U> delegate, Predicate<U> filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public StreamType type() {
        return delegate.type();
    }

    @Override
    public void onNext(U update) {
        if (filter.test(update)) {
            delegate.onNext(update);
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

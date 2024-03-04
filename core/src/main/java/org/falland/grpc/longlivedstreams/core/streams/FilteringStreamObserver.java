package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

/**
 * Returns the stream observer that filters updates according to the predicate provided
 * If the message is filtered the underlying observer would not see it
 * @param <V> type of the observer message
 */
public class FilteringStreamObserver<V> implements ControlledStreamObserver<V> {

    private final ControlledStreamObserver<V> delegate;
    private final Predicate<V> filter;

    public FilteringStreamObserver(@Nonnull ControlledStreamObserver<V> delegate, @Nonnull Predicate<V> filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public void onNext(V message) {
        if (filter.test(message)) {
            delegate.onNext(message);
        }
    }

    @Override
    public boolean isOpened() {
        return delegate.isOpened();
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

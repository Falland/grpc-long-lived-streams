package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Returns the stream observer that transforms the update from one type to another
 * That might be handy when the there's a generic stream of events and a streaming API for a particular type of events.
 * Transformer should always return some value that is not null, otherwise the returned value is going to be ignored and underlying observer is not going to be updated
 * The transformer is going to be called for each update passed to the observer exactly once.
 * @param <V> type of the observer message
 */
public class TransformingStreamObserver<T, V> implements ControlledStreamObserver<V> {

    private final ControlledStreamObserver<T> delegate;
    private final Function<V, T> transformer;

    public TransformingStreamObserver(@Nonnull ControlledStreamObserver<T> delegate, @Nonnull Function<V, T> transformer) {
        this.delegate = delegate;
        this.transformer = transformer;
    }

    @Override
    public void onNext(V value) {
        T transformedUpdate = transformer.apply(value);
        if (transformedUpdate != null) {
            delegate.onNext(transformedUpdate);
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

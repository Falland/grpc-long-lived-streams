package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.GrpcStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class TransformingGrpcStream<U, T> implements GrpcStream<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformingGrpcStream.class);

    private final GrpcStream<U> delegate;
    private final Function<T, U> transformer;

    public TransformingGrpcStream(GrpcStream<U> delegate, Function<T, U> transformer) {
        this.delegate = delegate;
        this.transformer = transformer;
    }

    @Override
    public StreamType type() {
        return delegate.type();
    }

    @Override
    public void onNext(T update) {
        try {
            U transformedUpdate = transformer.apply(update);
            if (transformedUpdate == null) {
                throw new TransformationException("Transformed value can't be null");
            }
            delegate.onNext(transformedUpdate);
        } catch (Exception e) {
            LOGGER.debug("Error during transformation");
            throw new TransformationException("Error during transformation", e);
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

    public static class TransformationException extends RuntimeException {

        public TransformationException(String message) {
            super(message);
        }

        public TransformationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

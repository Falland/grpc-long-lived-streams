package com.falland.grpc.longlivedstreams.server.subscription;

import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformingGrpcSubscription<U, T> implements GrpcSubscription<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformingGrpcSubscription.class);

    private final GrpcSubscription<U> delegate;
    private final Function<T, U> transformer;

    public TransformingGrpcSubscription(GrpcSubscription<U> delegate, Function<T, U> transformer) {
        this.delegate = delegate;
        this.transformer = transformer;
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
    public void processUpdate(T update) {
        try {
            U transformedUpdate = transformer.apply(update);
            if (transformedUpdate == null) {
                throw new TransformationException("Transformed value can't be null");
            }
            delegate.processUpdate(transformedUpdate);
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

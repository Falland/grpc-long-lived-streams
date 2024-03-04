package org.falland.grpc.longlivedstreams.core;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * This is the simplest implementation of the {@link ControlledStreamObserver} that ensures that
 * {@link StreamObserver#onCompleted()} or {@link StreamObserver#onError(Throwable)} methods can be called once and no other methods can be called after
 * This class also exposes the {@link CallStreamObserver} methods for more fine-grained flow control.
 * The most important methods are {@link CallStreamObserver#isReady()} and {@link CallStreamObserver#setOnReadyHandler(Runnable)}
 * The former allows the message producer to check whether underlying stream observer can process new update or not.
 * The latter allows to register a callback that is going to be executed once the observer becomes ready.
 * @param <V>
 */
public class FlowControlledObserver<V> extends CallStreamObserver<V> implements ControlledStreamObserver<V> {

    private final CallStreamObserver<V> delegate;
    private volatile boolean isOpened = true;

    public FlowControlledObserver(CallStreamObserver<V> delegate) {
        this.delegate = delegate;
    }

    public boolean isOpened() {
        return isOpened;
    }

    @Override
    public boolean isReady() {
        return isOpened && delegate.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
        if (isOpened) {
            delegate.setOnReadyHandler(onReadyHandler);
        }
    }

    @Override
    public void disableAutoInboundFlowControl() {
        if (isOpened) {
            delegate.disableAutoInboundFlowControl();
        }
    }

    @Override
    public void request(int count) {
        if (isOpened) {
            delegate.request(count);
        }
    }

    @Override
    public void setMessageCompression(boolean enable) {
        if (isOpened) {
            delegate.setMessageCompression(enable);
        }
    }

    @Override
    public void onNext(V value) {
        if (isOpened) {
            delegate.onNext(value);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (isOpened) {
            isOpened = false;
            delegate.onError(t);
        }
    }

    @Override
    public void onCompleted() {
        if (isOpened) {
            isOpened = false;
            delegate.onCompleted();
        }
    }
}

package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.ClientCallStreamObserver;
import org.falland.grpc.longlivedstreams.core.SubscriptionObserver;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClientSubscriptionObserver<U> implements SubscriptionObserver<U> {

    private final ClientCallStreamObserver<U> observer;
    //This flag is a guard for onError method. As we need to guarantee onError is never called twice
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public ClientSubscriptionObserver(ClientCallStreamObserver<U> observer) {
        this.observer = observer;
    }

    @Override
    public boolean isReady() {
        return observer.isReady();
    }

    @Override
    public void onNext(U value) {
        if (isOpened()) {
            observer.onNext(value);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (isClosed.compareAndSet(false, true)) {
            //We guarantee that onError is called only once
            observer.onError(t);
        }
    }

    @Override
    public void onCompleted() {
        if (isClosed.compareAndSet(false, true)) {
            observer.onCompleted();
        }
    }

    @Override
    public boolean isOpened() {
        return !isClosed.get();
    }
}
package io.grpc.longlivedstreams.server.subscription;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionObserver<U> implements StreamObserver<U> {

    private final String address;
    private final ServerCallStreamObserver<U> observer;
    //This flag is a guard for onError method. As we need to guarantee onError is never called twice
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public SubscriptionObserver(String address, ServerCallStreamObserver<U> observer) {
        this.address = address;
        this.observer = observer;
    }

    public String getAddress() {
        return address;
    }

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

    public boolean isOpened() {
        return !isClosed.get() && !observer.isCancelled();
    }
}
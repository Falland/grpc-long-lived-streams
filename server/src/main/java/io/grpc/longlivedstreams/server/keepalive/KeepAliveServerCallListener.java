package io.grpc.longlivedstreams.server.keepalive;

import io.grpc.longlivedstreams.server.keepalive.CallRegistry.Removable;
import io.grpc.ForwardingServerCallListener;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;

@SuppressWarnings("squid:S119")
class KeepAliveServerCallListener<ReqT> extends ForwardingServerCallListener<ReqT> {

    private final ServerCall.Listener<ReqT> delegate;
    private final Removable removable;

    KeepAliveServerCallListener(Listener<ReqT> delegate, Removable removable) {
        this.delegate = delegate;
        this.removable = removable;
    }

    @Override
    protected Listener<ReqT> delegate() {
        return delegate;
    }

    @Override
    public void onCancel() {
        removable.remove();
        super.onCancel();
    }

    @Override
    public void onComplete() {
        removable.remove();
        super.onComplete();
    }
}

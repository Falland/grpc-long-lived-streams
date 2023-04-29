package com.falland.grpc.longlivedstreams.server.keepalive;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("squid:S119")
class IdleAwareServerCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> implements IdleAware {

    private static final byte[] EMPTY_MESSAGE = new byte[]{};
    private static final Metadata METADATA = new Metadata();

    /*
     * 0  -> message has been sent since last idleness check
     * 1+ -> message has not been sent since last idleness check(s)
     */
    private final AtomicInteger idleCounter = new AtomicInteger();
    private final AtomicBoolean sentHeaders = new AtomicBoolean();

    IdleAwareServerCall(ServerCall<ReqT, RespT> delegate) {
        super(delegate);
    }

    @Override
    public void sendHeaders(Metadata headers) {
        trySendHeaders(headers);
    }

    @Override
    public void sendMessage(RespT message) {
        idleCounter.set(0);
        super.sendMessage(message);
    }

    @Override
    public int idleCount() {
        return idleCounter.incrementAndGet();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void ping() {
        trySendHeaders(METADATA);
        sendMessage((RespT) new BufferedInputStream(new ByteArrayInputStream(EMPTY_MESSAGE)));
    }

    /*
     * The headers have to be sent before the first message and they can only
     * be sent once. The headers may be sent via the regular stream observer
     * before the first message, or when the call is pinged. This method stops
     * the headers from being sent more than once.
     */
    private void trySendHeaders(Metadata headers) {
        if (sentHeaders.compareAndSet(false, true)) {
            super.sendHeaders(headers);
        }
    }
}

package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.CallStreamObserver;
import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;

import java.util.function.Consumer;

/**
 * Wrapper for the {@link OnReadyHandlerForwarder} to mimic the regular {@link FlowControlledObserver}.
 * This is needed to keep {@link org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver} agnostic of the specifics of the onReadyHandler injection in the client streams.
 * See {@link io.grpc.stub.ClientResponseObserver} and {@link ClientReceivingObserver} java docs for more info.
 * @param <V> stream observer message type
 */
public class OnReadyHandlerForwardingStreamObserver<V> extends FlowControlledObserver<V> {
    private final Consumer<Runnable> onReadyHandlerConsumer;

    public OnReadyHandlerForwardingStreamObserver(CallStreamObserver<V> delegate, Consumer<Runnable> onReadyHadlerConsumer) {
        super(delegate);
        this.onReadyHandlerConsumer = onReadyHadlerConsumer;
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
        onReadyHandlerConsumer.accept(onReadyHandler);
    }

    @Override
    public void disableAutoInboundFlowControl() {
        throw new UnsupportedOperationException("This operation is not supported in client call stream");
    }

    @Override
    public void request(int count) {
        throw new UnsupportedOperationException("This operation is not supported in client call stream");
    }

    @Override
    public void setMessageCompression(boolean enable) {
        throw new UnsupportedOperationException("This operation is not supported in client call stream");
    }
}
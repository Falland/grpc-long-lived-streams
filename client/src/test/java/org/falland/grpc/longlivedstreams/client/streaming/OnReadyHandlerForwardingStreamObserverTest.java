package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.CallStreamObserver;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OnReadyHandlerForwardingStreamObserverTest {

    private final CallStreamObserver<Long> delegate = mock(CallStreamObserver.class);

    private final Consumer<Runnable> onReadyHandlerConsumer = mock(Consumer.class);

    private final OnReadyHandlerForwardingStreamObserver<Long> underTest = new OnReadyHandlerForwardingStreamObserver<>(delegate, onReadyHandlerConsumer);

    @Test
    public void testSetOnReadyHandler_shouldPassHandlerToConsumer() {
        Runnable handler = () -> {};
        underTest.setOnReadyHandler(handler);
        verify(onReadyHandlerConsumer).accept(handler);
    }

    @Test
    public void testDisableAutoInboundFlowControl_shouldThrowUnsupportedException() {
        assertThrows(UnsupportedOperationException.class, underTest::disableAutoInboundFlowControl);
    }

    @Test
    public void testRequest_shouldThrowUnsupportedException() {
        assertThrows(UnsupportedOperationException.class, () -> underTest.request(1));
    }

    @Test
    public void testSetMessageCompression_shouldThrowUnsupportedException() {
        assertThrows(UnsupportedOperationException.class, () -> underTest.setMessageCompression(true));
    }
}
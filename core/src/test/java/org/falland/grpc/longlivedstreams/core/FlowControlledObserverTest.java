package org.falland.grpc.longlivedstreams.core;


import io.grpc.stub.CallStreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

class FlowControlledObserverTest {

    private FlowControlledObserver<Long> underTest;
    private CallStreamObserver<Long> delegate;

    @BeforeEach
    public void init() {
        delegate = mock(CallStreamObserver.class);
        underTest = new FlowControlledObserver<>(delegate);
    }

    @Test
    public void testOnCompleted_shouldNotAllowToCallOtherMethods_whenCalled() {
        underTest.onCompleted();

        verify(delegate).onCompleted();

        underTest.onCompleted();
        underTest.onError(new RuntimeException());
        underTest.onNext(1L);
        underTest.isReady();
        underTest.request(1);
        underTest.disableAutoInboundFlowControl();
        underTest.setOnReadyHandler(null);
        assertFalse(underTest.isOpened());
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void testOnError_shouldNotAllowToCallOtherMethods_whenCalled() {
        RuntimeException t = new RuntimeException();
        underTest.onError(t);

        verify(delegate).onError(t);

        underTest.onCompleted();
        underTest.onError(new RuntimeException());
        underTest.onNext(1L);
        underTest.isReady();
        underTest.request(1);
        underTest.disableAutoInboundFlowControl();
        underTest.setOnReadyHandler(null);
        assertFalse(underTest.isOpened());
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void testOnNext_shouldForwardToDelegate_whenCalled() {
        underTest.onNext(1L);

        verify(delegate).onNext(1L);
    }

    @Test
    public void testIsReady_shouldForwardToDelegate_whenCalled() {
        underTest.isReady();
        verify(delegate).isReady();
    }

    @Test
    public void testSetOnReadyHandler_shouldForwardToDelegate_whenCalled() {
        underTest.setOnReadyHandler(null);

        verify(delegate).setOnReadyHandler(null);
    }

    @Test
    public void testDisableAutoInboundFlowControl_shouldForwardToDelegate_whenCalled() {
        underTest.disableAutoInboundFlowControl();

        verify(delegate).disableAutoInboundFlowControl();
    }

    @Test
    public void testRequest_shouldForwardToDelegate_whenCalled() {
        underTest.request(1);

        verify(delegate).request(1);
    }

    @Test
    public void testSetMessageCompression_shouldForwardToDelegate_whenCalled() {
        underTest.setMessageCompression(true);

        verify(delegate).setMessageCompression(true);
    }


}
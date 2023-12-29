package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;

class ClientControlledStreamObserverTest {

    private ClientControlledStreamObserver<Long> underTest;
    private ClientCallStreamObserver<Long> observer;

    @BeforeEach
    public void beforeEach() {
        //noinspection unchecked
        observer = Mockito.mock(ClientCallStreamObserver.class);
        Mockito.doReturn(true).when(observer).isReady();
        underTest = new ClientControlledStreamObserver<>(observer);
    }

    @Test
    public void testIsReady_shouldBeReady_whileObserverIsReady() {
        Mockito.doReturn(true).when(observer).isReady();
        assertTrue(underTest.isReady());
    }

    @Test
    public void testIsReady_shouldBeNotReady_whenObserverIsNotReady() {
        Mockito.doReturn(false).when(observer).isReady();
        assertFalse(underTest.isReady());
    }

    @Test
    public void testOnNext_shouldSend_whenActive() {
        underTest.onNext(1L);
        Mockito.verify(observer).onNext(1L);
    }

    @Test
    public void testOnNext_shouldNotSend_whenIsClosed() {
        underTest.onCompleted();
        underTest.onNext(1L);
        Mockito.verify(observer, Mockito.never()).onNext(anyLong());
    }

    @Test
    public void testOnError_shouldCloseObserverAndSendError_whenFirstTime() {
        RuntimeException exception = new RuntimeException();
        underTest.onError(exception);
        Mockito.verify(observer).onError(exception);
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentCalls_always() {
        RuntimeException exception = new RuntimeException();
        RuntimeException anotherException = new RuntimeException();
        underTest.onError(exception);
        underTest.onError(anotherException);
        Mockito.verify(observer, Mockito.times(1)).onError(ArgumentMatchers.any(Exception.class));
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentOnCompleteCalls_always() {
        RuntimeException exception = new RuntimeException();
        underTest.onError(exception);
        underTest.onCompleted();
        Mockito.verify(observer, Mockito.times(1)).onError(ArgumentMatchers.any(Exception.class));
        Mockito.verify(observer, Mockito.never()).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnCompleted_shouldCloseObserverAndSendComplete_whenFirstTime() {
        underTest.onCompleted();
        Mockito.verify(observer).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnCompleted_shouldIgnoreSubsequentCalls_always() {
        underTest.onCompleted();
        underTest.onCompleted();
        Mockito.verify(observer, Mockito.times(1)).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentOnErrorCalls_always() {
        RuntimeException exception = new RuntimeException();
        underTest.onCompleted();
        underTest.onError(exception);
        Mockito.verify(observer, Mockito.times(1)).onCompleted();
        Mockito.verify(observer, Mockito.never()).onError(exception);
        assertFalse(underTest.isOpened());
    }

}
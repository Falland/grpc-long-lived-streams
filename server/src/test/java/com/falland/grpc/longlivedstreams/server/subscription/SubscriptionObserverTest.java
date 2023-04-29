package com.falland.grpc.longlivedstreams.server.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SubscriptionObserverTest {

    private SubscriptionObserver<Long> underTest;
    private ServerCallStreamObserver<Long> observer;

    @BeforeEach
    public void beforeEach() {
        observer = mock(ServerCallStreamObserver.class);
        doReturn(true).when(observer).isReady();
        underTest = new SubscriptionObserver<>("test", observer);
    }

    @Test
    public void testGetAddress_shouldReturnAddress_always() {
        assertEquals("test", underTest.getAddress());
    }

    @Test
    public void testIsReady_shouldBeReady_whileObserverIsReady() {
        doReturn(true).when(observer).isReady();
        assertTrue(underTest.isReady());
    }

    @Test
    public void testIsReady_shouldBeNotReady_whenObserverIsNotReady() {
        doReturn(false).when(observer).isReady();
        assertFalse(underTest.isReady());
    }

    @Test
    public void testOnNext_shouldSend_whenNotCancelled() {
        doReturn(false).when(observer).isCancelled();
        underTest.onNext(1L);
        verify(observer).onNext(1L);
    }

    @Test
    public void testOnNext_shouldNotSend_whenIsCancelled() {
        doReturn(true).when(observer).isCancelled();
        underTest.onNext(1L);
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    public void testOnNext_shouldNotSend_whenIsClosed() {
        doReturn(false).when(observer).isCancelled();
        underTest.onCompleted();
        underTest.onNext(1L);
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    public void testOnError_shouldCloseObserverAndSendError_whenFirstTime() {
        RuntimeException exception = new RuntimeException();
        underTest.onError(exception);
        verify(observer).onError(exception);
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentCalls_always() {
        RuntimeException exception = new RuntimeException();
        RuntimeException anotherException = new RuntimeException();
        underTest.onError(exception);
        underTest.onError(anotherException);
        verify(observer, times(1)).onError(any(Exception.class));
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentOnCompleteCalls_always() {
        RuntimeException exception = new RuntimeException();
        underTest.onError(exception);
        underTest.onCompleted();
        verify(observer, times(1)).onError(any(Exception.class));
        verify(observer, never()).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnCompleted_shouldCloseObserverAndSendComplete_whenFirstTime() {
        underTest.onCompleted();
        verify(observer).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnCompleted_shouldIgnoreSubsequentCalls_always() {
        underTest.onCompleted();
        underTest.onCompleted();
        verify(observer, times(1)).onCompleted();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldIgnoreSubsequentOnErrorCalls_always() {
        RuntimeException exception = new RuntimeException();
        underTest.onCompleted();
        underTest.onError(exception);
        verify(observer, times(1)).onCompleted();
        verify(observer, never()).onError(exception);
        assertFalse(underTest.isOpened());
    }

}
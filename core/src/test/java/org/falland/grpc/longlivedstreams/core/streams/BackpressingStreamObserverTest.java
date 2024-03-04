package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;
import org.falland.grpc.longlivedstreams.core.strategy.BackpressureStrategy;
import org.falland.grpc.longlivedstreams.core.strategy.ExceptionOnOverflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BackpressingStreamObserverTest {

    private final FlowControlledObserver<Long> observer = mock(FlowControlledObserver.class);
    private Runnable onReadyHandler;
    private BackpressureStrategy<Long> strategy;
    private BackpressingStreamObserver<Long> underTest;

    @BeforeEach
    public void init() {
        ArgumentCaptor<Runnable> handlerCaptor = ArgumentCaptor.forClass(Runnable.class);
        strategy = new ExceptionOnOverflow<>(5);
        underTest = BackpressingStreamObserver.<Long>builder()
                .withObserver(observer)
                .withStrategy(strategy)
                .withBatchSize(2)
                .build();
        verify(observer).setOnReadyHandler(handlerCaptor.capture());
        onReadyHandler = handlerCaptor.getValue();
    }

    @AfterEach
    public void tearDown() {
        underTest.onCompleted();
    }

    @Test
    public void testOnNext_shouldNotAddMessageToStrategyAndNotSend_whenObserverIsNotOpened() {
        doReturn(false).when(observer).isOpened();
        underTest.onNext(1L);
        assertNull(strategy.poll());
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    public void testOnNext_shouldAddMessageToStrategyAndNotSend_whenObserverIsNotReady() {
        doReturn(true).when(observer).isOpened();
        doReturn(false).when(observer).isReady();
        underTest.onNext(1L);
        assertEquals(1L, strategy.poll());
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    public void testOnNext_shouldSendImmediately_whenObserverIsReady() {
        doReturn(true).when(observer).isOpened();
        doReturn(true).when(observer).isReady();
        underTest.onNext(1L);
        verify(observer).onNext(1L);
    }

    @Test
    public void testOnNext_shouldSendImmediatelyUpToBatchSize_whenObserverIsReady() {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(-1L);
        underTest.onNext(0L);
        doReturn(true).when(observer).isReady();
        underTest.onNext(1L);
        assertEquals(1L, strategy.poll());
        verify(observer).onNext(-1L);
        verify(observer).onNext(0L);
        verify(observer, never()).onNext(1L);
    }

    @Test
    public void testTrySend_shouldSendInOnReadyHandler_whenObserverBecomesReadyEventually() {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        doReturn(true).when(observer).isReady();
        onReadyHandler.run();
        verify(observer).onNext(1L);
    }

    @Test
    public void testTrySend_shouldSendInOnReadyHandlerUpToBatchSize_whenObserverBecomesReadyEventually() {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        underTest.onNext(2L);
        underTest.onNext(3L);
        doReturn(true).when(observer).isReady();
        onReadyHandler.run();

        assertEquals(3L, strategy.poll());
        verify(observer).onNext(1L);
        verify(observer).onNext(2L);
        verify(observer, never()).onNext(3L);
    }

    @Test
    public void testTrySend_shouldNotFailWhenQueueIsEmpty_whenObserverBecomesReadyEventually() {
        doReturn(true).when(observer).isOpened();
        doReturn(true).when(observer).isReady();
        onReadyHandler.run();
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    public void testTrySend_shouldCloseObserver_whenObserverOnNextFailsWithException() {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(observer).onNext(anyLong());
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        doReturn(true).when(observer).isReady();
        onReadyHandler.run();
        verify(observer, times(1)).onNext(anyLong());
        verify(observer).onError(exception);
    }

    @Test
    public void testTrySend_shouldCloseObserver_whenObserverOnNextFailsWithException_immediateSend() {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(observer).onNext(anyLong());
        doReturn(true).when(observer).isOpened();
        doReturn(true).when(observer).isReady();
        underTest.onNext(1L);
        verify(observer, times(1)).onNext(anyLong());
        verify(observer).onError(exception);
    }

    @Test
    public void testIsOpened_shouldReturnFalse_whenObserverIsNotOpened() {
        doReturn(false).when(observer).isOpened();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testIsOpened_shouldReturnTrue_whenObserverIsOpened() {
        doReturn(true).when(observer).isOpened();
        assertTrue(underTest.isOpened());
    }

    @Test
    public void testOnError_shouldCloseObserverAndStrategy_always() {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        Exception exception = new Exception();
        underTest.onError(exception);

        assertNull(strategy.poll());
        verify(observer).onError(exception);
    }

    @Test
    public void testOnComplete_shouldCloseObserverAndStrategy_always() {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        underTest.onCompleted();

        assertNull(strategy.poll());
        verify(observer).onCompleted();
    }

    @Test
    @Timeout(1)
    public void testOnNextConcurrentSend_shouldSerializeSends_whenOnNextIsExecutedConcurrentlyWithOnReadyHandler() throws InterruptedException {
        doReturn(true).when(observer).isOpened();
        underTest.onNext(1L);
        underTest.onNext(2L);
        underTest.onNext(3L);
        Thread onReadyThread = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()) {
                onReadyHandler.run();
            }
        });
        doReturn(true).when(observer).isReady();
        onReadyThread.start();
        underTest.onNext(4L);
        underTest.onNext(5L);
        TimeUnit.MILLISECONDS.sleep(500L);
        onReadyThread.interrupt();
        verify(observer, times(1)).onNext(1L);
        verify(observer, times(1)).onNext(2L);
        verify(observer, times(1)).onNext(3L);
        verify(observer, times(1)).onNext(4L);
        verify(observer, times(1)).onNext(5L);

        onReadyThread.join();
    }

}
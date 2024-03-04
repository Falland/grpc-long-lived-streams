package org.falland.grpc.longlivedstreams.core.keepalive;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class KeepAliveStreamObserverTest {

    @SuppressWarnings("unchecked")
    private final ControlledStreamObserver<Long> delegate = mock(ControlledStreamObserver.class);
    private final Duration keepAliveDuration = Duration.ofMillis(10);
    private KeepAliveStreamObserver<Long> underTest;

    @BeforeEach
    void setup() {
        underTest = KeepAliveStreamObserver.<Long>builder()
                .withStreamObserver(delegate)
                .withHeartBeatProducer(() -> 42L)
                .withKeepAliveDuration(keepAliveDuration).build();
    }

    @AfterEach
    void tearDown() {
        underTest.close();
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testSendHeartBeat_shouldSendHeartbeat_whenTimeElapses() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(1)).onNext(42L);
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testSendHeartBeat_shouldNotFail_whenOnNextThrows() throws InterruptedException {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(delegate).onNext(anyLong());
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(2)).onNext(42L);
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testClose_shouldNotSendHeartbeat_whenStopped() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(1)).onNext(42L);
        underTest.close();
        clearInvocations(delegate);
        TimeUnit.MILLISECONDS.sleep(100);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void testIsOpened_shouldReturnFalse_whenObserverIsNotOpened() {
        doReturn(false).when(delegate).isOpened();
        assertFalse(underTest.isOpened());
    }

    @Test
    public void testIsOpened_shouldReturnTrue_whenObserverIsOpened() {
        doReturn(true).when(delegate).isOpened();
        assertTrue(underTest.isOpened());
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testOnError_shouldNotSendHeartbeat_whenStopped() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(1)).onNext(42L);
        clearInvocations(delegate);
        Exception exception = new Exception();
        underTest.onError(exception);
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate).onError(exception);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testOnComplete_shouldNotSendHeartbeat_whenStopped() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(1)).onNext(42L);
        clearInvocations(delegate);
        underTest.onCompleted();
        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate).onCompleted();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    void testClose_shouldNotCloseExecutor_whenExternal() throws InterruptedException {
        underTest.close();
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        underTest = KeepAliveStreamObserver.<Long>builder()
                .withStreamObserver(delegate)
                .withExecutor(executorService)
                .withKeepAliveDuration(keepAliveDuration)
                .withHeartBeatProducer(() -> 42L)
                .build();

        TimeUnit.MILLISECONDS.sleep(100);
        verify(delegate, atLeast(1)).onNext(42L);
        underTest.close();
        clearInvocations(delegate);
        TimeUnit.MILLISECONDS.sleep(100);
        verifyNoMoreInteractions(delegate);
        assertFalse(executorService.isShutdown());
        assertFalse(executorService.isTerminated());
        executorService.shutdownNow();
    }

}

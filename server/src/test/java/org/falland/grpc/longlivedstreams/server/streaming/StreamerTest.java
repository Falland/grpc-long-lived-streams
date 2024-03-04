package org.falland.grpc.longlivedstreams.server.streaming;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;
import org.falland.grpc.longlivedstreams.core.keepalive.KeepAliveStreamObserver;
import org.falland.grpc.longlivedstreams.core.strategy.BackpressureStrategy;
import org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StreamerTest {
    private Streamer<Long> underTest;
    @Mock
    private ServerCallStreamObserver<Long> observer;
    @Mock
    private StreamObserver<Long> wrongObserverClass;

    @Mock
    private FlowControlledObserver<Long> toRegister1;

    @Mock
    private FlowControlledObserver<Long> toRegister2;
    @Mock
    private BackpressureStrategy<Long> strategy;

    private AutoCloseable mocksHolder;

    @BeforeEach
    public void beforeEach() {
        mocksHolder = MockitoAnnotations.openMocks(this);
        doReturn(true).when(observer).isReady();
        doReturn(true).when(toRegister1).isOpened();
        doReturn(true).when(toRegister2).isOpened();

        underTest = new Streamer<>(10, Duration.of(1000, ChronoUnit.MILLIS), "test");
    }

    @AfterEach
    public void afterEach() throws Exception {
        mocksHolder.close();
    }

    @Test
    public void testSubscribe_shouldThrowException_whenSubscriptionIsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> underTest.register(null));
    }

    @Test
    public void testFreeFlowStreamObserver_shouldReturnFreeFlowObserver() {
        var result = underTest.freeFlowStreamObserver(observer);
        assertNotNull(result);
        assertEquals(FlowControlledObserver.class, result.getClass());
        assertTrue(result.isOpened());
    }

    @Test
    public void testFreeFlowStreamObserver_shouldThrowException_whenObserverIsNull() {
        assertThrows(NullPointerException.class, () -> underTest.freeFlowStreamObserver(null));
    }

    @Test
    public void testFreeFlowStreamObserver_shouldThrowException_whenObserverIsNotServerCallStreamObserver() {
        assertThrows(IllegalArgumentException.class, () -> underTest.freeFlowStreamObserver(wrongObserverClass));
    }

    @Test
    public void testBackPressingStreamObserver_shouldReturnBackPressingStreamObserver() {
        var result = underTest.backPressingStreamObserver(observer, strategy);
        assertNotNull(result);
        assertEquals(BackpressingStreamObserver.class, result.getClass());
        assertTrue(result.isOpened());
    }

    @Test
    public void testBackPressingStreamObserver_shouldThrowException_whenObserverIsNull() {
        assertThrows(NullPointerException.class, () -> underTest.backPressingStreamObserver(null, strategy));
    }

    @Test
    public void testBackPressingStreamObserver_shouldThrowException_whenStrategyIsNull() {
        assertThrows(NullPointerException.class, () -> underTest.backPressingStreamObserver(observer, null));
    }

    @Test
    public void testBackPressingStreamObserver_shouldThrowException_whenObserverIsNotServerCallStreamObserver() {
        assertThrows(IllegalArgumentException.class, () -> underTest.backPressingStreamObserver(wrongObserverClass, strategy));
    }@Test
    public void testKeepAliveStreamObserver_shouldKeepAliveObserver() {
        var result = underTest.keepAliveStreamObserver(toRegister1, () -> 42L);
        assertNotNull(result);
        assertEquals(KeepAliveStreamObserver.class, result.getClass());
        assertTrue(result.isOpened());
        result.onCompleted();
    }

    @Test
    public void testKeepAliveStreamObserver_shouldThrowException_whenObserverIsNull() {
        assertThrows(NullPointerException.class, () -> underTest.keepAliveStreamObserver(null, () -> 1L));
    }

    @Test
    public void testKeepAliveStreamObserver_shouldThrowException_whenHeartBeatProducerIsNull() {
        assertThrows(NullPointerException.class, () -> underTest.keepAliveStreamObserver(observer, null));
    }

    @Test
    public void testKeepAliveStreamObserver_shouldThrowException_whenObserverIsNotServerCallStreamObserver() {
        assertThrows(IllegalArgumentException.class, () -> underTest.keepAliveStreamObserver(wrongObserverClass, () -> 42L));
    }

    @Test
    public void testRegister_shouldRegisterStream() {
        underTest.register(underTest.backPressingStreamObserver(observer, strategy));
        assertTrue(underTest.hasStreams());
    }

    @Test
    public void testSendMessage_shouldUpdateStreams_thatAreOpened() {
        underTest.register(toRegister1);
        underTest.register(toRegister2);
        assertTrue(underTest.hasStreams());

        underTest.sendMessage(1L);
        verify(toRegister1).onNext(1L);
        verify(toRegister2).onNext(1L);
    }

    @Test
    public void testSendMessage_shouldNotUpdateStreams_thatAreNotOpened() {
        underTest.register(toRegister1);
        underTest.register(toRegister2);
        doReturn(false).when(toRegister2).isOpened();

        assertTrue(underTest.hasStreams());

        underTest.sendMessage(1L);
        verify(toRegister1).onNext(1L);
        verify(toRegister2, never()).onNext(1L);
    }

    @Test
    public void testSendMessage_shouldDeleteStream_whenStreamIsNotOpened() {
        underTest.register(toRegister1);
        doReturn(false).when(toRegister1).isOpened();

        assertTrue(underTest.hasStreams());

        underTest.sendMessage(1L);
        verify(toRegister1, never()).onNext(1L);
        assertFalse(underTest.hasStreams());
    }

    @Test
    public void testSendMessage_shouldDeleteStream_whenStreamThrowsError() {
        underTest.register(toRegister1);
        doThrow(RuntimeException.class).when(toRegister1).onNext(anyLong());

        assertTrue(underTest.hasStreams());

        underTest.sendMessage(1L);
        assertFalse(underTest.hasStreams());
    }

    @Test
    public void testComplete_shouldCompleteAllStreams_thatAreRegistered() {
        underTest.register(toRegister1);
        underTest.register(toRegister2);

        assertTrue(underTest.hasStreams());

        underTest.complete();
        verify(toRegister1).onCompleted();
        verify(toRegister2).onCompleted();
    }

    @Test
    public void testCompleteWithError_shouldCompleteWithErrorAllStreams_thatAreRegistered() {
        underTest.register(toRegister1);
        underTest.register(toRegister2);

        assertTrue(underTest.hasStreams());

        RuntimeException exception = new RuntimeException();
        underTest.completeWithError(exception);
        verify(toRegister1).onError(exception);
        verify(toRegister2).onError(exception);
    }
}
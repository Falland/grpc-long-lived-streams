package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.ClientCallStreamObserver;
import org.falland.grpc.longlivedstreams.client.UpdateProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class ClientReceivingObserverTest {
    private ClientReceivingObserver<Long, Long> underTest;
    private UpdateProcessor<Long> processor;
    private Consumer<Throwable> errorHandler;
    private Runnable completionHandler;
    private Runnable onReadyHandler;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void beforeEach() {
        processor = mock(UpdateProcessor.class);
        errorHandler = mock(Consumer.class);
        completionHandler = mock(Runnable.class);
        onReadyHandler = mock(Runnable.class);

        underTest = new ClientReceivingObserver<>(processor, errorHandler, completionHandler, onReadyHandler);
    }

    @Test
    public void testOnNext_shouldUpdateProcessor_whenNoError() {
        underTest.onNext(1L);
        verify(processor).processUpdate(1L);
    }

    @Test
    public void testOnNext_shouldCallErrorHandler_whenProcessorThrows() {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(processor).processUpdate(anyLong());
        underTest.onNext(1L);

        verify(processor).processUpdate(1L);
        verify(errorHandler).accept(exception);
    }

    @Test
    public void testOnComplete_shouldCallCompletionHandler() {
        underTest.onCompleted();
        verify(completionHandler).run();
    }

    @Test
    public void testOnError_shouldCallErrorHandler() {
        RuntimeException exception = new RuntimeException();
        underTest.onError(exception);
        verify(errorHandler).accept(exception);
    }

    @Test
    public void testBeforeStart_shouldSetOnReadyHandlerToPassedStreamObserver() {
        @SuppressWarnings("unchecked") ClientCallStreamObserver<Long> passedStreamObserver = mock(ClientCallStreamObserver.class);
        underTest.beforeStart(passedStreamObserver);
        verify(passedStreamObserver).setOnReadyHandler(onReadyHandler);
    }

}
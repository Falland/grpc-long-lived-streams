package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;

class TransformingStreamObserverTest {

    private final ControlledStreamObserver<String> delegate = mock(ControlledStreamObserver.class);

    private final TransformingStreamObserver<String,Long> underTest = new TransformingStreamObserver<>(delegate, l -> l == 0L ? null : String.valueOf(l));

    @Test
    public void testOnNext_shouldTransformValue_whenTransformerReturnsNonNullObject() {
        underTest.onNext(1L);

        verify(delegate).onNext("1");
    }

    @Test
    public void testOnNext_shouldNotPass_whenTransformerReturnsNull() {
        underTest.onNext(0L);

       verifyNoInteractions(delegate);
    }

    @Test
    public void testOnError_shouldCallDelegate() {
        RuntimeException t = new RuntimeException();
        underTest.onError(t);

        verify(delegate).onError(t);
    }

    @Test
    public void testOnComplete_shouldCallDelegate() {
        underTest.onCompleted();

        verify(delegate).onCompleted();
    }

    @Test
    public void testIsOpened_shouldReturnTrue_whenDelegateIsOpened() {
        doReturn(true).when(delegate).isOpened();

        assertTrue(underTest.isOpened());
    }
    @Test
    public void testIsOpened_shouldReturnFalse_whenDelegateIsNotOpened() {
        doReturn(false).when(delegate).isOpened();

        assertFalse(underTest.isOpened());
    }

}
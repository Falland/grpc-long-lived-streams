package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FilteringStreamObserverTest {

    private final ControlledStreamObserver<Long> delegate = mock(ControlledStreamObserver.class);

    private final FilteringStreamObserver<Long> underTest = new FilteringStreamObserver<>(delegate, l -> l%2 == 0);

    @Test
    public void testOnNext_shouldFilterAllOdds() {
        underTest.onNext(1L);
        underTest.onNext(3L);
        underTest.onNext(11L);

        verifyNoInteractions(delegate);
    }

    @Test
    public void testOnNext_shouldPassAllEvens() {
        underTest.onNext(2L);
        underTest.onNext(6L);
        underTest.onNext(12L);

        verify(delegate).onNext(2L);
        verify(delegate).onNext(6L);
        verify(delegate).onNext(12L);
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
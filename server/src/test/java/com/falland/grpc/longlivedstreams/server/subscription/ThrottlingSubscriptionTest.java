package com.falland.grpc.longlivedstreams.server.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThrottlingSubscriptionTest {

    private ThrottlingSubscription<Long, Long> underTest;
    private SubscriptionObserver<Long> observer;
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeEach
    public void beforeEach() {
        observer = mock(SubscriptionObserver.class);
        doReturn(true).when(observer).isOpened();
        doReturn(true).when(observer).isReady();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        //we compact on modulo 10, this gives [0..9] keys for all longs
        underTest = ThrottlingSubscription.builder(observer, l -> l%10)
                .withExecutor(scheduledExecutorService)
                .withThreadCoolDownWhenNotReady(Duration.ofMillis(1))
                .build();
    }

    @AfterEach
    public void afterEach() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    public void testTrySend_shouldSendFirstLatestUpdate_whenObserverIsReady() {
        underTest.processUpdate(1L);
        underTest.processUpdate(11L);

        verify(observer, after(100).times(1)).onNext(11L);
    }

    @Test
    public void testTrySend_shouldSendUpdate_whenObserverIsReady() {
        underTest.processUpdate(1L);

        verify(observer, after(100).times(1)).onNext(1L);
    }

    @Test
    public void testTrySend_shouldNotSend_whenNoUpdates() {
        verify(observer, after(100).never()).onNext(anyLong());
    }

    @Test
    public void testTrySend_shouldNotSend_whenObserverIsNotReady() {
        doReturn(false).when(observer).isReady();
        underTest.processUpdate(1L);

        verify(observer, after(100).never()).onNext(anyLong());
    }

    @Test
    public void testGetType_shouldBeThrottling_always() {
        assertEquals(SubscriptionType.THROTTLING, underTest.getType());
    }

    @Test
    public void testIsActive_shouldReturnTrue_whenObserverIsOpened() {
        assertTrue(underTest.isActive());
    }

    @Test
    public void testIsActive_shouldReturnFalse_whenObserverIsNotOpened() {
        doReturn(false).when(observer).isOpened();
        assertFalse(underTest.isActive());
    }

    @Test
    public void testSendMessage_shouldCloseObserver_whenItFailsOnNext() {
        doThrow(RuntimeException.class).when(observer).onNext(1L);

        underTest.processUpdate(1L);

        verify(observer, after(100).times(1)).onError(any(RuntimeException.class));
    }

}
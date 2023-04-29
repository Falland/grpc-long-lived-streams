package org.falland.grpc.longlivedstreams.server.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class FullFlowSubscriptionTest {

    private FullFlowSubscription<Long> underTest;
    private SubscriptionObserver<Long> observer;
    private Duration threadCoolDown;

    @BeforeEach
    public void beforeEach() {
        observer = mock(SubscriptionObserver.class);
        threadCoolDown = Duration.of(100, ChronoUnit.MILLIS);
        //we compact on modulo 10, this gives [0..9] keys for all longs
        underTest = FullFlowSubscription.builder(observer)
                .withClientId("test")
                .withQueueSize(2)
                .withServiceName("test-service")
                .withThreadCoolDownWhenNotReady(threadCoolDown)
                .build();
    }

    @AfterEach
    public void afterEach() {
        underTest.onCompleted();
    }

    @Test
    public void testProcessUpdates_shouldCloseWithError_whenQueueSizeIsOverflown() {
        underTest.processUpdate(1L);
        underTest.processUpdate(1L);
        underTest.processUpdate(1L);
        verify(observer).onError(FullFlowSubscription.TOO_SLOW_EXCEPTION);
    }

    @Test
    //possibly flaky, but I can't think of better approach
    public void testSend_shouldCoolDown_whenObserverIsNotReady() throws InterruptedException {
        //half of cooldown
        long halfCoolDown = threadCoolDown.toMillis() / 2;
        TimeUnit.MILLISECONDS.sleep(halfCoolDown);
        verify(observer, atLeast(1)).isReady();
        //second half plus something
        TimeUnit.MILLISECONDS.sleep(halfCoolDown + 30);
        verify(observer, atLeast(2)).isReady();
    }

    @Test
    //possibly flaky, but I can't think of better approach
    public void testSend_shouldNotSend_whenObserverIsReadyButQueueIsEmpty() throws InterruptedException {
        doReturn(true).when(observer).isReady();
        //To make sure we were not in the cooldown
        TimeUnit.MILLISECONDS.sleep(2 * threadCoolDown.toMillis());
        verify(observer, never()).onNext(anyLong());
    }

    @Test
    //possibly flaky, but I can't think of better approach
    public void testSend_shouldSendAllUpdates_whenObserverIsReady() throws InterruptedException {
        doReturn(true).when(observer).isReady();
        //To make sure we were not in the cooldown
        TimeUnit.MILLISECONDS.sleep(threadCoolDown.toMillis());
        underTest.processUpdate(1L);
        underTest.processUpdate(1L);
        //let's give sending thread some time
        TimeUnit.MILLISECONDS.sleep(2 * threadCoolDown.toMillis());
        verify(observer, times(2)).onNext(1L);
    }

    @Test
    public void testGetType_shouldBeFullFlow_always() {
        assertEquals(SubscriptionType.FULL_FLOW, underTest.getType());
    }

    @Test
    @Disabled
    public void testIsActive_shouldReturnTrue_whenObserverIsOpened() {
        //TODO: flacky test
        doReturn(true).when(observer).isOpened();
        assertTrue(underTest.isActive());
    }

    @Test
    public void testIsActive_shouldReturnFalse_whenObserverIsNotOpened() {
        doReturn(false).when(observer).isOpened();
        assertFalse(underTest.isActive());
    }
}
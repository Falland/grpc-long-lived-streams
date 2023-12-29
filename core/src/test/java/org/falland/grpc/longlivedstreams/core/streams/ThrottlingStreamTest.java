package org.falland.grpc.longlivedstreams.core.streams;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class ThrottlingStreamTest {

    private final TestControlledStreamObserver<Long> observer = new TestControlledStreamObserver<>(l -> l == 42L);
    private ThrottlingStream<Long, Long> underTest;
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeEach
    public void beforeEach() {
        observer.setOpened(true);
        observer.setReady(true);
        scheduledExecutorService = spy(Executors.newSingleThreadScheduledExecutor());
        //we compact on modulo 10, this gives [0..9] keys for all longs
        underTest = ThrottlingStream.<Long,Long>builder()
                .withObserver(observer)
                .withKeyExtractor(l -> l%10)
                .withExecutor(scheduledExecutorService)
                .withCoolDownDuration(Duration.ofMillis(1))
                .build();
    }

    @AfterEach
    public void afterEach() {
        scheduledExecutorService.shutdownNow();
        observer.reset();
    }

    @Test
    @Timeout(1)
    public void testTrySend_shouldSendLatestUpdate_whenObserverIsReady() throws InterruptedException {
        underTest.onNext(1L);
        underTest.onNext(11L);
        Collection<Long> messages = observer.onNextMessages();
        assertThat(messages.size()).isEqualTo(1);
        assertThat(messages).isEqualTo(List.of(11L));
    }

    @Test
    @Timeout(1)
    public void testTrySend_shouldSendUpdate_whenObserverIsReady() throws InterruptedException {
        underTest.onNext(1L);
        Collection<Long> messages = observer.onNextMessages();
        assertThat(messages.size()).isEqualTo(1);
        assertThat(messages).isEqualTo(List.of(1L));
    }

    @Test
    @Timeout(1)
    public void testTrySend_shouldNotSend_whenNoUpdates() throws InterruptedException {
        observer.awaitIsReadyAndGetPhaseNumber();
        Collection<Long> messages = observer.onNextMessages(500);
        assertThat(messages.size()).isEqualTo(0);
    }

    @Test
    @Timeout(1)
    public void testTrySend_shouldNotSend_whenObserverIsNotReady() throws InterruptedException {
        observer.setReady(false);
        underTest.onNext(1L);
        Collection<Long> messages = observer.onNextMessages(500);
        assertThat(messages.size()).isEqualTo(0);
    }

    @Test
    public void testGetType_shouldBeThrottling_always() {
        assertEquals(StreamType.THROTTLING, underTest.type());
    }

    @Test
    public void testIsActive_shouldReturnTrue_whenObserverIsOpened() {
        assertTrue(underTest.isActive());
    }

    @Test
    public void testIsActive_shouldReturnFalse_whenObserverIsNotOpened() {
        observer.setOpened(false);
        assertFalse(underTest.isActive());
    }

    @Test
    @Timeout(1)
    public void testSendMessage_shouldCloseObserver_whenItFailsOnNext() {
        underTest.onNext(42L);
        observer.awaitOnError();
        assertThat(observer.getErrorReceived()).isNotNull();
        assertThat(observer.getErrorReceived()).isInstanceOf(RuntimeException.class);
        assertThat(observer.getErrorReceived().getMessage()).isEqualTo("Test. Throw during onNext");
    }

    @Test
    @Timeout(1)
    public void testSendMessage_shouldNotScheduleTrySends_whenStreamIsClosed() throws InterruptedException {
        observer.setOpened(false);
        reset(scheduledExecutorService);
        underTest.onNext(1L);
        observer.onNextMessages(500);

        verify(scheduledExecutorService, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

}
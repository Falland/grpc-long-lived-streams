package org.falland.grpc.longlivedstreams.core.subscription;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThrottlingSubscriptionTest {

    private final TestSubscriptionObserver<Long> observer = new TestSubscriptionObserver<>(l -> l == 42L);
    private ThrottlingSubscription<Long, Long> underTest;
    private ScheduledExecutorService scheduledExecutorService;

    @BeforeEach
    public void beforeEach() {
        observer.setOpened(true);
        observer.setReady(true);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        //we compact on modulo 10, this gives [0..9] keys for all longs
        underTest = ThrottlingSubscription.builder(observer, l -> l%10)
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
        underTest.processUpdate(1L);
        underTest.processUpdate(11L);
        Collection<Long> messages = observer.onNextMessages();
        assertThat(messages.size()).isEqualTo(1);
        assertThat(messages).isEqualTo(List.of(11L));
    }

    @Test
    @Timeout(1)
    public void testTrySend_shouldSendUpdate_whenObserverIsReady() throws InterruptedException {
        underTest.processUpdate(1L);
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
        underTest.processUpdate(1L);
        Collection<Long> messages = observer.onNextMessages(500);
        assertThat(messages.size()).isEqualTo(0);
    }

    @Test
    public void testGetType_shouldBeThrottling_always() {
        assertEquals(SubscriptionType.THROTTLING, underTest.type());
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
        underTest.processUpdate(42L);
        observer.awaitOnError();
        assertThat(observer.getErrorReceived()).isNotNull();
        assertThat(observer.getErrorReceived()).isInstanceOf(RuntimeException.class);
        assertThat(observer.getErrorReceived().getMessage()).isEqualTo("Test. Throw during onNext");
    }

}
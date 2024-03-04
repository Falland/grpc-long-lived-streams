package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

class FullFlowStreamTest {

//    private final Duration threadCoolDown = Duration.of(100, ChronoUnit.MILLIS);
//    private final TestControlledStreamObserver<Long> observer = new TestControlledStreamObserver<>();
//    private FullFlowStream<Long> underTest;
//
//    @BeforeEach
//    public void beforeEach() {
//        underTest = FullFlowStream.<Long>builder()
//                .withObserver(observer)
//                .withQueueSize(2)
//                .withCoolDownDuration(threadCoolDown)
//                .withThreadFactory(new ThreadFactoryImpl("test-"))
//                .build();
//    }
//
//    @AfterEach
//    public void afterEach() {
//        underTest.onCompleted();
//        observer.reset();
//    }
//
//    @Test
//    @Timeout(1)
//    public void testProcessUpdates_shouldCloseWithError_whenQueueSizeIsOverflown() {
//        underTest.onNext(1L);
//        underTest.onNext(1L);
//        underTest.onNext(1L);
//        assertThat(observer.getErrorReceived()).isNotNull();
//        assertThat(observer.getErrorReceived()).isEqualTo(FullFlowStream.TOO_SLOW_EXCEPTION);
//    }
//
//    @Test
//    @Timeout(1)
//    public void testSend_shouldCoolDown_whenObserverIsNotReady() throws InterruptedException {
//        //half of cooldown
//        long halfCoolDown = threadCoolDown.toMillis() / 2;
//        TimeUnit.MILLISECONDS.sleep(halfCoolDown);
//        assertThat(observer.awaitIsReadyAndGetPhaseNumber()).isAtLeast(1);
//        //second half plus something
//        TimeUnit.MILLISECONDS.sleep(halfCoolDown + 30);
//        assertThat(observer.awaitIsReadyAndGetPhaseNumber()).isAtLeast(2);
//    }
//
//    @Test
//    @Timeout(1)
//    public void testSend_shouldNotSend_whenObserverIsReadyButQueueIsEmpty() {
//        observer.setReady(true);
//        //wait for two calls
//        observer.awaitIsReadyAndGetPhaseNumber();
//        observer.awaitIsReadyAndGetPhaseNumber();
//        assertThat(observer.onNextCallCount()).isEqualTo(0);
//    }
//
//    @Test
//    @Timeout(1)
//    public void testSend_shouldSendAllUpdates_whenObserverIsReady() throws InterruptedException {
//        observer.setReady(true);
//        observer.awaitIsReadyAndGetPhaseNumber();
//        underTest.onNext(1L);
//        underTest.onNext(1L);
//        var messages = observer.onNextMessages();
//        //We will wait here for a bit in case we caught only the first message
//        messages = observer.onNextMessages(500);
//        assertThat(messages.size()).isEqualTo(2);
//        assertThat(messages).isEqualTo(List.of(1L, 1L));
//    }
//
//    @Test
//    @Timeout(1)
//    public void testSend_shouldNotLooseUpdate_whenObserverIsNotReady() throws InterruptedException {
//        observer.setReady(false);
//        underTest.onNext(1L);
//        observer.onNextMessages(100L);
//        observer.setReady(true);
//        observer.awaitIsReadyAndGetPhaseNumber();
//        var messages = observer.onNextMessages();
//        assertThat(messages.size()).isEqualTo(1);
//        assertThat(messages).isEqualTo(List.of(1L));
//    }
//
//    @Test
//    public void testGetType_shouldBeFullFlow_always() {
//        assertThat(StreamType.FULL_FLOW).isEqualTo(underTest.type());
//    }
//
//    @Test
//    public void testIsActive_shouldReturnTrue_whenObserverIsOpened() {
//        observer.setOpened(true);
//        assertThat(underTest.isActive()).isTrue();
//    }
//
//    @Test
//    public void testIsActive_shouldReturnFalse_whenObserverIsNotOpened() {
//        observer.setOpened(false);
//        assertThat(underTest.isActive()).isFalse();
//    }
}
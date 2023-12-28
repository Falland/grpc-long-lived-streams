package org.falland.grpc.longlivedstreams.server.streaming;

import com.google.protobuf.AbstractMessage;
import io.grpc.stub.ServerCallStreamObserver;
import org.falland.grpc.longlivedstreams.core.streams.StreamType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class StreamerTest {

    private final String addressString = "test.com:4242";
    private final SubscriptionKey fullKey = new SubscriptionKey(addressString, "testFull");
    private final SubscriptionKey throttlingKey = new SubscriptionKey(addressString, "testThrottling");
    private final SubscriptionDescriptor fullDescriptor = new SubscriptionDescriptor(
            fullKey.address(),
            fullKey.clientId(),
            StreamType.FULL_FLOW);
    private final SubscriptionDescriptor throttlingDescriptor = new SubscriptionDescriptor(
            throttlingKey.address(),
            throttlingKey.clientId(),
            StreamType.THROTTLING);
    private Streamer<AbstractMessage> underTest;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerFull;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerFull2;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerThrottle;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerThrottle2;

    private AutoCloseable mocksHolder;

    @BeforeEach
    public void beforeEach() {
        mocksHolder = MockitoAnnotations.openMocks(this);
        doReturn(true).when(observerFull).isReady();
        doReturn(true).when(observerFull2).isReady();
        doReturn(true).when(observerThrottle).isReady();
        doReturn(true).when(observerThrottle2).isReady();

        underTest = new Streamer<>("test", 10, Duration.of(1000, ChronoUnit.MICROS));
    }

    @AfterEach
    public void afterEach() throws Exception {
        underTest.stop();
        mocksHolder.close();
    }

    @Test
    public void testSubscribe_shouldThrowException_whenSubscriptionIsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> underTest.subscribe(null));
    }

    @Test
    public void testCreateSubscription_shouldCreateFullFlowSubscription_whenIsForFullFlow() {
        ServerGrpcStream<AbstractMessage> subscription = underTest.createFullSubscription(
                fullKey,
                observerFull);
        assertEquals(fullKey, subscription.subscriptionKey());
        Assertions.assertEquals(StreamType.FULL_FLOW, subscription.type());
        assertTrue(subscription.isActive());
    }

    @Test
    public void testCreateSubscription_shouldCreateThrottlingSubscription_whenIsForThrottling() {
        ServerGrpcStream<AbstractMessage> subscription = underTest.createThrottlingSubscription(
                throttlingKey, observerFull, message -> message);
        assertEquals(throttlingKey, subscription.subscriptionKey());
        assertEquals(StreamType.THROTTLING, subscription.type());
        assertTrue(subscription.isActive());
    }

    @Test
    public void testSubscribe_shouldCreateFullFlowSubscription_whenIsFullFlow() {
        underTest.subscribeFullFlow(fullKey, observerFull);
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(fullDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCreateThrottleSubscription_whenIsThrottle() {
        underTest.subscribeThrottling(throttlingKey, observerThrottle, message -> message);
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(throttlingDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewSubscription_whenSameKeyForFullFlow() {
        underTest.subscribeFullFlow(fullKey, observerFull);
        underTest.subscribeFullFlow(fullKey, observerFull2);
        verify(observerFull2, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(fullDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewSubscription_whenSameKeyForThrottle() {
        underTest.subscribeThrottling(throttlingKey,  observerThrottle, message -> message);
        underTest.subscribeThrottling(throttlingKey, observerThrottle2, message -> message);
        verify(observerThrottle2, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(throttlingDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewSubscription_whenTypeIsDifferent() {
        SubscriptionKey customKey = new SubscriptionKey(addressString, "test");
        underTest.subscribeThrottling(customKey, observerThrottle, message -> message);
        underTest.subscribeFullFlow(customKey, observerFull);
        verify(observerFull, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        SubscriptionDescriptor customDescriptor = new SubscriptionDescriptor(addressString, "test", StreamType.THROTTLING);
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(customDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNew_whenSubscriptionsHaveSameKey() {
        ServerGrpcStream<AbstractMessage> subscription1 = underTest.createFullSubscription(fullKey, observerFull);
        ServerGrpcStream<AbstractMessage> subscription2 = underTest.createFullSubscription(fullKey, observerFull2);

        underTest.subscribe(subscription1);
        underTest.subscribe(subscription2);
        verify(observerFull2, times(1)).onCompleted();
        verify(observerFull, never()).onCompleted();

        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(fullDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldNotCloseNew_whenSubscriptionsAreExactlySame() {
        ServerGrpcStream<AbstractMessage> subscription1 = underTest.createFullSubscription(fullKey, observerFull);

        underTest.subscribe(subscription1);
        underTest.subscribe(subscription1);
        verify(observerFull, never()).onCompleted();

        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(fullDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldAllowTwoClientsToSubscribe_always() {
        underTest.subscribeThrottling(throttlingKey, observerThrottle, message -> message);
        underTest.subscribeFullFlow(fullKey, observerFull);
        verify(observerThrottle, never()).onCompleted();
        verify(observerFull, never()).onCompleted();
        assertEquals(2, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(fullDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
        assertTrue(underTest.getSubscriptionDescriptors()
                        .stream()
                        .anyMatch(throttlingDescriptor::equals),
                underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubmitResponse_shouldUpdateAllClients_always() throws InterruptedException {
        underTest.subscribeThrottling(throttlingKey, observerThrottle, message -> message);
        underTest.subscribeFullFlow(fullKey, observerFull);
        assertEquals(2, underTest.getSubscriptionDescriptors().size());

        AbstractMessage msg = mock(AbstractMessage.class);
        underTest.submitResponse(msg);
        //Let's give threads to do their jobs
        TimeUnit.MILLISECONDS.sleep(50);
        verify(observerFull).onNext(msg);
        verify(observerThrottle).onNext(msg);

    }

    @Test
    public void testSubmitResponses_shouldUpdateAllClientsWithMultipleMessages_always() throws InterruptedException {
        underTest.subscribeThrottling(throttlingKey, observerThrottle, message -> message);
        underTest.subscribeFullFlow(fullKey, observerFull);
        assertEquals(2, underTest.getSubscriptionDescriptors().size());

        AbstractMessage msg1 = mock(AbstractMessage.class);
        AbstractMessage msg2 = mock(AbstractMessage.class);
        underTest.submitResponses(Arrays.asList(msg1, msg2));
        //Let's give threads to do their jobs
        TimeUnit.MILLISECONDS.sleep(50);
        verify(observerFull).onNext(msg1);
        verify(observerFull).onNext(msg2);
        verify(observerThrottle).onNext(msg1);
        verify(observerThrottle).onNext(msg2);
    }
}
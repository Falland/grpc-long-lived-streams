package io.grpc.longlivedstreams.server.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.longlivedstreams.server.subscription.GrpcSubscription;
import io.grpc.longlivedstreams.server.subscription.SubscriptionType;
import com.google.protobuf.AbstractMessage;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class StreamerTest {
    private Streamer<AbstractMessage> underTest;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerFull;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerFull2;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerThrottle;
    @Mock
    private ServerCallStreamObserver<AbstractMessage> observerThrottle2;

    @BeforeEach
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        doReturn(true).when(observerFull).isReady();
        doReturn(true).when(observerFull2).isReady();
        doReturn(true).when(observerThrottle).isReady();
        doReturn(true).when(observerThrottle2).isReady();

        underTest = new Streamer<>("test", 10, Duration.of(1000, ChronoUnit.MICROS));
    }

    @AfterEach
    public void afterEach() {
        underTest.stop();
    }

    @Test
    public void testSubscribe_shouldThrowException_whenSubscriptionIsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> underTest.subscribe(null));
    }

    @Test
    public void testCreateSubscription_shouldCreateFullFlowSubscription_whenIsForFullFlow() {
        GrpcSubscription<AbstractMessage> subscription = underTest.createFullSubscription("testFull", observerFull);
        assertEquals("testFull", subscription.getClientId());
        Assertions.assertEquals(SubscriptionType.FULL_FLOW, subscription.getType());
        assertTrue(subscription.isActive());
    }
    @Test
    public void testCreateSubscription_shouldCreateThrottlingSubscription_whenIsForThrottling() {
        GrpcSubscription<AbstractMessage> subscription = underTest.createThrottlingSubscription("testThrottling", observerFull, message -> message);
        assertEquals("testThrottling", subscription.getClientId());
        assertEquals(SubscriptionType.THROTTLING, subscription.getType());
        assertTrue(subscription.isActive());
    }

    @Test
    public void testSubscribe_shouldCreateFullFlowSubscription_whenIsFullFlow() {
        underTest.subscribeFullFlow("testFull", observerFull);
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testFull") &&
                                           sd.getSubscriptionType().equals(
                                                   SubscriptionType.FULL_FLOW)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCreateThrottleSubscription_whenIsThrottle() {
        underTest.subscribeThrottling("testThrottle", observerThrottle, message -> message);
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testThrottle") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.THROTTLING)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewSubscription_whenIsFullFlow() {
        underTest.subscribeFullFlow("testFull", observerFull);
        underTest.subscribeFullFlow("testFull", observerFull2);
        verify(observerFull2, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testFull") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.FULL_FLOW)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewSubscription_whenIsThrottle() {
        underTest.subscribeThrottling("testThrottle", observerThrottle, message -> message);
        underTest.subscribeThrottling("testThrottle", observerThrottle2, message -> message);
        verify(observerThrottle2, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testThrottle") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.THROTTLING)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewForOneClient_whenTypeIsDifferent() {
        underTest.subscribeThrottling("test", observerThrottle, message -> message);
        underTest.subscribeFullFlow("test", observerFull);
        verify(observerFull, times(1)).onCompleted();
        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("test") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.THROTTLING)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldCloseNewForOneClient_whenSubscriptionsAreDifferent() {
        GrpcSubscription<AbstractMessage> subscription1 = underTest.createFullSubscription("testFull", observerFull);
        GrpcSubscription<AbstractMessage> subscription2 = underTest.createFullSubscription("testFull", observerFull2);

        underTest.subscribe(subscription1);
        underTest.subscribe(subscription2);
        verify(observerFull2, times(1)).onCompleted();
        verify(observerFull, never()).onCompleted();

        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testFull") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.FULL_FLOW)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldNotCloseNewForOneClient_whenSubscriptionsAreSame() {
        GrpcSubscription<AbstractMessage> subscription1 = underTest.createFullSubscription("testFull", observerFull);

        underTest.subscribe(subscription1);
        underTest.subscribe(subscription1);
        verify(observerFull, never()).onCompleted();

        assertEquals(1, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testFull") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.FULL_FLOW)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubscribe_shouldAllowTwoClientsToSubscribe_always() {
        underTest.subscribeThrottling("testThrottle", observerThrottle, message -> message);
        underTest.subscribeFullFlow("testFull", observerFull);
        verify(observerThrottle, never()).onCompleted();
        verify(observerFull, never()).onCompleted();
        assertEquals(2, underTest.getSubscriptionDescriptors().size());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testFull") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.FULL_FLOW)),
                   underTest.getSubscriptionDescriptors().toString());
        assertTrue(underTest.getSubscriptionDescriptors()
                           .stream()
                           .anyMatch(sd -> sd.getClientAddress().equals("null") &&
                                           sd.getClientId().equals("testThrottle") &&
                                           sd.getSubscriptionType().equals(SubscriptionType.THROTTLING)),
                   underTest.getSubscriptionDescriptors().toString());
    }

    @Test
    public void testSubmitResponse_shouldUpdateAllClients_always() throws InterruptedException {
        underTest.subscribeThrottling("testThrottle", observerThrottle, message -> message);
        underTest.subscribeFullFlow("testFull", observerFull);
        assertEquals(2, underTest.getSubscriptionDescriptors().size());

        AbstractMessage msg = mock(AbstractMessage.class);
        underTest.submitResponse(msg);
        //Let's give threads to do their jobs
        TimeUnit.MILLISECONDS.sleep(50);
        verify(observerFull).onNext(msg);
        verify(observerThrottle).onNext(msg);

    }

    @Test
    public void testSubmitResponses_shouldUpdateAllClients_always() throws InterruptedException {
        underTest.subscribeThrottling("testThrottle", observerThrottle, message -> message);
        underTest.subscribeFullFlow("testFull", observerFull);
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
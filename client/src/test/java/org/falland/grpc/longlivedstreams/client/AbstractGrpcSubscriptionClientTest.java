package org.falland.grpc.longlivedstreams.client;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.client.streaming.ClientReceivingObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AbstractGrpcSubscriptionClientTest {
    private volatile StreamObserver<Long> simpleObserver;
    private volatile StreamObserver<Long> streamingObserver;
    private AtomicInteger simpleSubscriptionsCount;
    private Runnable onReadyHandler;
    private ClientCallStreamObserver<Long> clientStreamingObserver;
    private ClientConfiguration configuration;
    private UpdateProcessor<Long> updateProcessor;
    private Duration retrySubscritionDuration;

    private AbstractGrpcSubscriptionClient<Void, Long> simpleTestClient;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void init() {
        configuration = mock(ClientConfiguration.class);
        doReturn("localhost").when(configuration).hostName();
        doReturn(4242).when(configuration).port();
        updateProcessor = mock(UpdateProcessor.class);
        onReadyHandler = mock(Runnable.class);
        clientStreamingObserver = mock(ClientCallStreamObserver.class);

        retrySubscritionDuration = Duration.ofMillis(10);
        simpleSubscriptionsCount = new AtomicInteger(0);
        simpleTestClient = new TestSimpleGrpcClient(configuration, updateProcessor, retrySubscritionDuration);
    }

    @AfterEach
    public void stop() {
        simpleTestClient.stop();
    }

    @Test
    public void testStart_shouldCallSubscribe() throws InterruptedException {
        assertNull(simpleObserver);
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStart_shouldCallSubscribeOnce_whenStartCalledTwice() throws InterruptedException {
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(1, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStreamObserver_shouldPropagateUpdateToProcessor_whenMessageReceived() throws InterruptedException {
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onNext(1L);
        verify(updateProcessor).processUpdate(1L);
    }

    @Test
    public void testStreamingObserver_shouldSetOnReadyHandler_whenCalledBeforeStart() throws InterruptedException {
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(streamingObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        ((ClientReceivingObserver<Long, Long>)streamingObserver).beforeStart(clientStreamingObserver);
        verify(clientStreamingObserver).setOnReadyHandler(onReadyHandler);
    }

    @Test
    public void testStreamObserver_shouldNotReconnect_whenCompletedAndReconnectOnCompleteIsNotActive() throws InterruptedException {
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onCompleted();
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(1, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStreamObserver_shouldReconnect_whenCompletedAndReconnectOnCompleteIsActive() throws InterruptedException {
        simpleTestClient.stopNow();

        doReturn(true).when(configuration).reconnectOnComplete();
        simpleTestClient = new TestSimpleGrpcClient(configuration, updateProcessor, retrySubscritionDuration);

        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onCompleted();
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(2, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStreamObserver_shouldNotReconnect_whenReceivedUnrecoverableError() throws InterruptedException {
        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onError(new StatusRuntimeException(Status.UNIMPLEMENTED));
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(1, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStreamObserver_shouldReconnect_whenReceivedRecoverableStatusError() throws InterruptedException {
        simpleTestClient.stopNow();

        doReturn(true).when(configuration).reconnectOnComplete();
        simpleTestClient = new TestSimpleGrpcClient(configuration, updateProcessor, retrySubscritionDuration);

        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onError(new StatusRuntimeException(Status.INTERNAL));
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(2, simpleSubscriptionsCount.get());
    }

    @Test
    public void testStreamObserver_shouldReconnect_whenReceivedNonStatusError() throws InterruptedException {
        simpleTestClient.stopNow();

        doReturn(true).when(configuration).reconnectOnComplete();
        simpleTestClient = new TestSimpleGrpcClient(configuration, updateProcessor, retrySubscritionDuration);

        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onError(new RuntimeException());
        TimeUnit.MILLISECONDS.sleep(100);
        assertEquals(2, simpleSubscriptionsCount.get());
    }

    @Test
    public void testReconnect_shouldReconnect_afterReconnectDuration() throws InterruptedException {
        simpleTestClient.stopNow();

        doReturn(true).when(configuration).reconnectOnComplete();
        simpleTestClient = new TestSimpleGrpcClient(configuration, updateProcessor, retrySubscritionDuration);

        simpleTestClient.start();
        TimeUnit.MILLISECONDS.sleep(100);
        assertNotNull(simpleObserver);
        assertEquals(1, simpleSubscriptionsCount.get());

        simpleObserver.onError(new RuntimeException());
        TimeUnit.MILLISECONDS.sleep(retrySubscritionDuration.toMillis() / 2);
        assertEquals(1, simpleSubscriptionsCount.get());
        TimeUnit.MILLISECONDS.sleep(retrySubscritionDuration.toMillis() * 2);
        assertEquals(2, simpleSubscriptionsCount.get());
    }

    private class TestSimpleGrpcClient extends AbstractGrpcSubscriptionClient<Void, Long> {

        protected TestSimpleGrpcClient(ClientConfiguration clientConfiguration, UpdateProcessor<Long> processor, Duration retrySubscriptionDuration) {
            super(clientConfiguration, processor, retrySubscriptionDuration);
        }

        @Override
        protected void subscribe(Channel channel) {
            simpleObserver = this.simpleObserver();
            streamingObserver = this.clientStreamingCallObserver(onReadyHandler);
            simpleSubscriptionsCount.incrementAndGet();
        }
    }
}
package org.falland.grpc.longlivedstreams.client;

import com.google.protobuf.AbstractMessage;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.grpc.Status.Code.FAILED_PRECONDITION;
import static io.grpc.Status.Code.INVALID_ARGUMENT;
import static io.grpc.Status.Code.OUT_OF_RANGE;
import static io.grpc.Status.Code.PERMISSION_DENIED;
import static io.grpc.Status.Code.UNAUTHENTICATED;
import static io.grpc.Status.Code.UNIMPLEMENTED;

public abstract class AbstractGrpcSubscriptionClient<U extends AbstractMessage> implements GrpcConnectionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcSubscriptionClient.class);
    public static final EnumSet<Code> UNRECOVERABLE_CODES = EnumSet.of(UNIMPLEMENTED, UNAUTHENTICATED, FAILED_PRECONDITION, PERMISSION_DENIED, INVALID_ARGUMENT, OUT_OF_RANGE);
    private final ClientContext clientContext;
    private final UpdateProcessor<U> processor;
    private final ScheduledExecutorService connectionManagerThread;
    private final Duration retrySubscriptionDuration;
    //We can allow only one subscription per client
    private final Semaphore subscribePermits = new Semaphore(1);
    private final AtomicBoolean firstSubscriptionAttempt = new AtomicBoolean(true);
    private final AtomicInteger reconnectionAttempts = new AtomicInteger();
    @SuppressWarnings("FieldCanBeLocal")
    private ChannelStateListenerLogger logger;
    private volatile ScheduledFuture<?> reSubscriptionFuture;
    private volatile ManagedChannel channel;
    private volatile boolean isActive = true;

    protected AbstractGrpcSubscriptionClient(ClientContext clientContext,
                                             UpdateProcessor<U> processor,
                                             Duration retrySubscriptionDuration) {
        this.clientContext = clientContext;
        this.processor = processor;
        this.retrySubscriptionDuration = retrySubscriptionDuration;
        this.connectionManagerThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl(clientContext.clientName() + "-connection-manager-thread-"));
    }

    public ClientContext getClientContext() {
        return clientContext;
    }

    public void start() {
        openChannel();
    }

    /**
     * Initiates an orderly shutdown in which preexisting calls continue but new calls are immediately cancelled.
     */
    public void stop() {
        closeChannel();
        connectionManagerThread.submit(this::safeUnsubscribe);
        connectionManagerThread.shutdown();
    }

    /**
     * Initiates a forceful shutdown in which preexisting and new calls are cancelled.
     */
    @SuppressWarnings("unused")
    public void stopNow() {
        closeChannelNow();
        connectionManagerThread.submit(this::safeUnsubscribe);
        connectionManagerThread.shutdown();
    }

    protected abstract void subscribe(Channel channel);

    protected void unsubscribe() {
    }

    protected void onUnrecoverableError(Throwable throwable) {
        LOGGER.error("Unrecoverable error received form Streamer", throwable);
    }

    @SuppressWarnings("rawtypes")
    protected void openChannel() {
        ManagedChannelBuilder channelBuilder =
                ManagedChannelBuilder.forAddress(clientContext.hostName(), clientContext.port())
                        .maxInboundMessageSize(clientContext.maxInboundMessageSize());
        if (clientContext.usePlaintext()) {
            channelBuilder.usePlaintext();
        }
        if (clientContext.executor() != null) {
            channelBuilder.executor(clientContext.executor());
        }
        this.reconnectionAttempts.set(clientContext.maxAttemptsBeforeReconnect());
        this.firstSubscriptionAttempt.set(true);
        this.channel = channelBuilder.build();
        logger = new ChannelStateListenerLogger(channel, true, clientContext.clientName());
        logger.addGrpcConnectionListener(this);
        if (!connectionManagerThread.isShutdown()) {
            connectionManagerThread.submit(() -> safeSubscribe(channel));
        }
    }

    private void closeChannel() {
        if (channel != null) {
            channel.shutdown();
        }
    }

    private void closeChannelNow() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    private void safeSubscribe(Channel channel) {
        try {
            //avoid double subscription
            if (subscribePermits.tryAcquire() && isActive) {
                subscribe(channel);
            }
        } catch (Exception e) {
            LOGGER.error("Error during subscription for client {}. Reconnecting", clientContext.clientName(), e);
            scheduleSafeResubscribe(channel);
        }
    }

    private void scheduleSafeResubscribe(Channel channel) {
        if (reSubscriptionFuture != null && !reSubscriptionFuture.isDone()) {
            return;
        }
        if (!connectionManagerThread.isShutdown()) {
            reSubscriptionFuture = connectionManagerThread.schedule(() ->
                            Context.current().fork().run(() -> safeResubscribe(channel)),
                    retrySubscriptionDuration.toMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            reSubscriptionFuture = null;
        }
    }

    private void submitSafeUnsubscribe() {
        if (!connectionManagerThread.isShutdown()) {
            connectionManagerThread.submit(this::safeUnsubscribe);
        }
    }

    private void safeUnsubscribe() {
        try {
            unsubscribe();
        } catch (Exception e) {
            LOGGER.error("Error during un-subscription for client {}. Ignoring", clientContext.clientName(), e);
        } finally {
            subscribePermits.drainPermits();
            subscribePermits.release();
        }
    }

    private void safeResubscribe(Channel channel) {
        safeUnsubscribe();
        if (reconnectionAttempts.decrementAndGet() <= 0) {
            closeChannel();
            openChannel();
        } else {
            safeSubscribe(channel);
        }
    }

    public final StreamObserver<U> createResponseObserver(Channel stubChannel) {
        return new GrpcStreamObserver(clientContext.clientName(), stubChannel);
    }

    @Override
    public void onStateChanged(String channelName, ConnectivityState state) {
        if (state == ConnectivityState.IDLE && !firstSubscriptionAttempt.getAndSet(false)) {
            scheduleSafeResubscribe(channel);
        }
    }

    protected EnumSet<Code> unrecoverableCodes() {
        return UNRECOVERABLE_CODES;
    }

    private class GrpcStreamObserver implements StreamObserver<U> {
        private final String clientName;
        private final Channel channel;

        GrpcStreamObserver(String clientName, Channel channel) {
            this.clientName = clientName;
            this.channel = channel;
        }

        @Override
        public void onNext(U update) {
            LOGGER.debug("Received subscription response {}", update);
            try {
               processor.processUpdate(update);
            } catch (Exception e) {
                LOGGER.error("Error while processing update {}", update, e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (isRecoverable(throwable)) {
                LOGGER.debug("Received error for subscription. Resubscribing channel {} for client {}",
                        channel, clientName, throwable);
                scheduleSafeResubscribe(channel);
            } else {
                isActive = false;
                LOGGER.error("Received unrecoverable error. Stopping channel {} for client {}.",
                        channel, clientName, throwable);
                onUnrecoverableError(throwable);
                submitSafeUnsubscribe();
            }
        }

        @Override
        public void onCompleted() {
            LOGGER.info("Received stream complete for subscription. Resubscribing channel {} for client {}",
                    channel, clientName);
            scheduleSafeResubscribe(channel);
        }

        protected boolean isRecoverable(Throwable error) {
            if (error instanceof StatusRuntimeException) {
                StatusRuntimeException statusException = ((StatusRuntimeException) error);
                Code code = statusException.getStatus().getCode();
                return !unrecoverableCodes().contains(code);
            }
            //If not status exception then we consider the error recoverable
            return true;
        }
    }
}

package com.falland.grpc.longlivedstreams.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.AbstractMessage;
import io.grpc.*;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGrpcSubscriptionClient<U extends AbstractMessage> implements GrpcConnectionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcSubscriptionClient.class);

    private final ClientContext clientContext;
    private final List<UpdateProcessor<U>> processors;
    private final ScheduledExecutorService connectionManagerThread;
    private final Duration retrySubscriptionDuration;
    //We can allow only one subscription per client
    private final Semaphore subscribePermits = new Semaphore(1);
    private final AtomicBoolean firstSubscriptionAttempt = new AtomicBoolean(true);
    private final AtomicInteger reconnectionAttempts = new AtomicInteger();
    private ChannelStateListenerLogger logger;
    private volatile ScheduledFuture<?> resubscriptionFuture;
    private volatile ManagedChannel channel;
    private volatile boolean isActive = true;

    protected AbstractGrpcSubscriptionClient(ClientContext clientContext,
                                             List<UpdateProcessor<U>> processors,
                                             Duration retrySubscriptionDuration) {
        this.clientContext = clientContext;
        this.processors = Collections.unmodifiableList(processors);
        this.retrySubscriptionDuration = retrySubscriptionDuration;
        this.connectionManagerThread = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                                                          .setNameFormat(
                                                                                                  clientContext.getClientName()
                                                                                                  + "-connection-manager-thread-%d")
                                                                                          .build());
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
    public void stopNow() {
        closeChannelNow();
        connectionManagerThread.submit(this::safeUnsubscribe);
        connectionManagerThread.shutdown();
    }

    protected abstract void subscribe(Channel channel);

    protected void unsubscribe() {
    }

    protected void onUnrecoverableError(Throwable throwable) {
        //do nothing
    }

    @SuppressWarnings("rawtypes")
    private void openChannel() {
        ManagedChannelBuilder channelBuilder =
                ManagedChannelBuilder.forAddress(clientContext.hostName(), clientContext.port())
                        .maxInboundMessageSize(clientContext.getMaxInboundMessageSize());
        if (clientContext.usePlaintext()) {
            channelBuilder.usePlaintext();
        }
        this.reconnectionAttempts.set(clientContext.maxAttemptsBeforeReconnect());
        this.firstSubscriptionAttempt.set(true);
        this.channel = channelBuilder.build();
        logger = new ChannelStateListenerLogger(channel, true, clientContext.getClientName());
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
            LOGGER.error("Error during subscription for client {}. Reconnecting", clientContext.getClientName(), e);
            scheduleSafeResubscribe(channel);
        }
    }

    private void scheduleSafeResubscribe(Channel channel) {
        if (resubscriptionFuture != null && !resubscriptionFuture.isDone()) {
            return;
        }
        if (!connectionManagerThread.isShutdown()) {
            resubscriptionFuture = connectionManagerThread.schedule(() -> Context.current().fork()
                                                                            .run(() -> safeResubscribe(channel)),
                                                                    retrySubscriptionDuration.toMillis(),
                                                                    TimeUnit.MILLISECONDS);
        } else {
            resubscriptionFuture = null;
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
            LOGGER.error("Error during un-subscription for client {}. Ignoring", clientContext.getClientName(), e);
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
        return new GrpcStreamObserver(clientContext.getClientName(), stubChannel);
    }

    @Override
    public void onStateChanged(String channelName, ConnectivityState state) {
        if (state == ConnectivityState.IDLE && !firstSubscriptionAttempt.getAndSet(false)) {
            scheduleSafeResubscribe(channel);
        }
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received subscription response {}", update);
            }
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < processors.size(); i++) {
                try {
                    processors.get(i).processUpdate(update);
                } catch (Exception e) {
                    LOGGER.error("Error while processing update {} with processor {}", update, processors.get(i), e);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (isRecoverable(throwable)) {
                LOGGER.debug("Received error for subscription. Resubscribing channel {} for client {}", channel,
                             clientName, throwable);
                scheduleSafeResubscribe(channel);
            } else {
                isActive = false;
                LOGGER.error("Received unrecoverable error. Stopping channel {} for client {}.", channel, clientName,
                             throwable);
                onUnrecoverableError(throwable);
                submitSafeUnsubscribe();
            }
        }

        @Override
        public void onCompleted() {
            LOGGER.info("Received stream complete for subscription. Resubscribing channel {} for client {}", channel,
                        clientName);
            scheduleSafeResubscribe(channel);
        }

        private boolean isRecoverable(Throwable error) {
            if (error instanceof StatusRuntimeException) {
                StatusRuntimeException statusException = ((StatusRuntimeException) error);
                Code code = statusException.getStatus().getCode();
                switch (code) {
                    case UNIMPLEMENTED:
                    case UNAUTHENTICATED:
                    case FAILED_PRECONDITION:
                    case PERMISSION_DENIED:
                        return false;
                    default:
                        return true;
                }
            }
            //If not status exception then it's either internal in client or in custom logic - should be recoverable
            return true;
        }
    }
}

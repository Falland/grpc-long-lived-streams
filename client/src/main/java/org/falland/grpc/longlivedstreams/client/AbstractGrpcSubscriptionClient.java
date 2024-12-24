package org.falland.grpc.longlivedstreams.client;

import io.grpc.*;
import io.grpc.Status.Code;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.client.streaming.ClientReceivingObserver;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.grpc.Status.Code.*;

/**
 * This class provides the basic functionality for the gRPC client streaming, both server and client based.
 * The class supports automatic reconnects when error and completion is received.
 * In case of erroneous completion of the stream, client can differentiate between recoverable and unrecoverable errors, to see if the reconnect is feasible.
 * The unrecoverable errors are defined by {@link #unrecoverableCodes()} and checked in the {@link #isRecoverable(Throwable)}
 * Implementation class can override the behavior of checking the error codes to follow any custom logic
 * <p>
 * In case of regular completion, i.e. {@link StreamObserver#onCompleted()}, client will not reconnect by default.
 * There's a configuration parameter to make client to reconnect on completion.
 * <p>
 * The implementation of this class should only provide the {@link #subscribe(Channel)} and {@link #unsubscribe()} methods implementations
 * {@link #subscribe(Channel)} method is expected to contain the gRPC call and creation of the streamers
 * {@link #unsubscribe()} method is expected to contain any logic to clean up the state after the disconnect
 * <p>
 * The reconnection logic supports reopening channel in case {@link ClientConfiguration#maxAttemptsBeforeReconnect()} is reached.
 * This is done to break sticky session within gRPC connectivity and retry connecting to potentially different host.
 * Since we are working with long-lived streams this feature is a must for streams that spans across multiple hours/days.
 * <p>
 * The client would ensure to execute re-subscription only once in case of concurrent call (can happen due to multitude of reasons since the re-subscribe call can be called from multiple places).
 * Therefor it is always guaranteed to have no mre than 1 active stream in any moment in time.
 * <p>
 * For the finer grained control of the threads spawned by the client, the client can provide custom executor.
 * In this case gRPC channel would use the passed executor instead of the global shared one.
 * Be careful with custom executors, incorrect configuration of the executor can lead to excessive use of resources or blocked code execution.
 * Always test your set-up and do the customisations only after thorough testing.
 * @param <Req> Request message type
 * @param <Resp> Response message type
 */
public abstract class AbstractGrpcSubscriptionClient<Req, Resp> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcSubscriptionClient.class);
    public static final EnumSet<Code> UNRECOVERABLE_CODES = EnumSet.of(UNIMPLEMENTED, UNAUTHENTICATED, FAILED_PRECONDITION, PERMISSION_DENIED, INVALID_ARGUMENT, OUT_OF_RANGE);
    private final ClientConfiguration clientConfiguration;

    private final UpdateProcessor<Resp> processor;
    private final ScheduledExecutorService connectionManagerThread;
    private final Duration retrySubscriptionDuration;
    private final boolean reconnectOnComplete;
    //We can allow only one subscription per client
    private final Semaphore subscribePermits = new Semaphore(1);
    private final AtomicInteger reconnectionAttempts = new AtomicInteger();
    private volatile ScheduledFuture<?> reSubscriptionFuture;
    private volatile ManagedChannel channel;
    private volatile boolean isActive = true;

    protected AbstractGrpcSubscriptionClient(ClientConfiguration clientConfiguration,
                                             UpdateProcessor<Resp> processor,
                                             Duration retrySubscriptionDuration) {
        this.clientConfiguration = clientConfiguration;
        this.processor = processor;
        this.reconnectOnComplete = clientConfiguration.reconnectOnComplete();
        this.retrySubscriptionDuration = retrySubscriptionDuration;
        this.connectionManagerThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl(clientConfiguration.clientName() + "-connection-manager-thread-"));
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public void start() {
        if (channel == null) {
            openChannel();
        }
    }

    /**
     * Initiates an orderly shutdown in which preexisting calls continue but new calls are immediately cancelled.
     */
    public void stop() {
        connectionManagerThread.submit(this::safeUnsubscribe);
        connectionManagerThread.shutdown();
        closeChannel();
    }

    /**
     * Initiates a forceful shutdown in which preexisting and new calls are cancelled.
     */
    @SuppressWarnings("unused")
    public void stopNow() {
        connectionManagerThread.submit(this::safeUnsubscribe);
        connectionManagerThread.shutdownNow();
        closeChannelNow();
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
                ManagedChannelBuilder.forAddress(clientConfiguration.hostName(), clientConfiguration.port())
                        .maxInboundMessageSize(clientConfiguration.maxInboundMessageSize());
        if (clientConfiguration.usePlaintext()) {
            channelBuilder.usePlaintext();
        }
        if (clientConfiguration.executor() != null) {
            channelBuilder.executor(clientConfiguration.executor());
        }
        this.reconnectionAttempts.set(clientConfiguration.maxAttemptsBeforeReconnect());
        this.channel = channelBuilder.build();
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
            LOGGER.error("Error during subscription for client {}. Reconnecting", clientConfiguration.clientName(), e);
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
            LOGGER.error("Error during un-subscription for client {}. Ignoring", clientConfiguration.clientName(), e);
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

    public final StreamObserver<Resp> simpleObserver() {
        return new ClientReceivingObserver<>(processor, this::handleObserverError,
                this::handleObserverCompletion, () -> {
        });
    }

    public final StreamObserver<Resp> clientStreamingCallObserver(Runnable onReadyHandler) {
        return new ClientReceivingObserver<>(processor, this::handleObserverError,
                this::handleObserverCompletion, onReadyHandler);
    }

    public final StreamObserver<Resp> clientStreamingCallObserver(Runnable onReadyHandler, Consumer<ClientCallStreamObserver<Req>> onBeforeStart) {
        return new ClientReceivingObserver<>(processor, this::handleObserverError,
                this::handleObserverCompletion, onReadyHandler, onBeforeStart);
    }

    protected EnumSet<Code> unrecoverableCodes() {
        return UNRECOVERABLE_CODES;
    }

    private void handleObserverCompletion() {
        LOGGER.info("Received stream complete for subscription. Resubscribing channel {} for client {}",
                channel, clientConfiguration.clientName());
        if (reconnectOnComplete) {
            scheduleSafeResubscribe(channel);
        }
    }

    private void handleObserverError(Throwable t) {
        if (isRecoverable(t)) {
            LOGGER.debug("Received error for subscription. Resubscribing channel {} for client {}",
                    channel, clientConfiguration.clientName(), t);
            scheduleSafeResubscribe(channel);
        } else {
            isActive = false;
            LOGGER.error("Received unrecoverable error. Stopping channel {} for client {}.",
                    channel, clientConfiguration.clientName(), t);
            onUnrecoverableError(t);
            submitSafeUnsubscribe();
        }
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

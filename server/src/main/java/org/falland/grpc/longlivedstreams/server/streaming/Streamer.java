package org.falland.grpc.longlivedstreams.server.streaming;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.falland.grpc.longlivedstreams.core.subscription.FullFlowSubscription;
import org.falland.grpc.longlivedstreams.core.subscription.GrpcSubscription;
import org.falland.grpc.longlivedstreams.core.SubscriptionObserver;
import org.falland.grpc.longlivedstreams.core.subscription.SubscriptionType;
import org.falland.grpc.longlivedstreams.core.subscription.ThrottlingSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The class provides the safe server side streaming support for gRPC services
 * This class takes care of slow consumers/fast producers by exposing two types of subscriptions.
 * Full Flow: all messages produced are sent to the client. If the client is slow to consume then messages are queued.
 * In order to avoid unlimited queue growth there's a limit for the queue. Once the queue reaches the limit the stream is closed with exception.
 * It's up to the client to reconnect or halt.
 * Note! While disconnected the service does not store the messages produced, so client might lose messages in between reconnects
 * Throttling: only the latest messages are reaching the client. In case the client is too slow to consume all the messages
 * the key-based compaction will be performed. Key extraction function is provided when creating the subscription.
 * Compactor is guaranteed to preserve the key order. I.e. the key that appeared first in the stream will be sent first.
 *
 * @param <U> the message type of the streamer
 */
public class Streamer<U> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Streamer.class);
    private final Map<SubscriptionKey, ServerGrpcSubscription<U>> subscriptionsByKey = new ConcurrentHashMap<>();
    private final ScheduledExecutorService throttlingExecutor;
    protected final String serviceName;
    protected final int queueSize;
    protected final Duration coolDownDuration;

    public Streamer(String serviceName, int queueSize,
                    Duration threadCoolDownWhenNotReady) {
        this.serviceName = serviceName;
        this.queueSize = queueSize;
        this.coolDownDuration = threadCoolDownWhenNotReady;
        this.throttlingExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl(serviceName + "-throttler-", true));
    }

    /**
     * Creates the Full Flow subscription. You can use it to send the snapshot prior to finalize the subscription with {@link Streamer#subscribe(ServerGrpcSubscription)}
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     *
     * @param observer - the gRPC observer object provided from the method call site
     * @return GrpcSubscription with the type {@link SubscriptionType#FULL_FLOW}
     */
    public ServerGrpcSubscription<U> createFullSubscription(SubscriptionKey key, StreamObserver<U> observer) {
        SubscriptionObserver<U> subscriptionObserver = createObserver(observer);
        return new ServerGrpcSubscription<>(key,
                FullFlowSubscription.builder(subscriptionObserver)
                        .withQueueSize(queueSize)
                        .withCoolDownDuration(coolDownDuration)
                        .withThreadFactory(new ThreadFactoryImpl(getNamePrefix(key)))
                        .build());
    }

    private String getNamePrefix(SubscriptionKey key) {
        return serviceName + "-" + key.address() + "-" + key.clientId() + "-";
    }

    /**
     * Creates the Throttling subscription. You can use it to send the snapshot prior to finalize the subscription with {@link Streamer#subscribe(ServerGrpcSubscription)}
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     *
     * @param observer               - the gRPC observer object provided from the method call site
     * @param compactionKeyExtractor - the key extraction function that is going to be used for key-based compaction
     * @param <K>                    - the type of the key
     * @return GrpcSubscription with the type {@link SubscriptionType#THROTTLING}
     */
    public <K> ServerGrpcSubscription<U> createThrottlingSubscription(SubscriptionKey key, StreamObserver<U> observer,
                                                                      Function<U, K> compactionKeyExtractor) {
        SubscriptionObserver<U> subscriptionObserver = createObserver(observer);
        return new ServerGrpcSubscription<>(key,
                ThrottlingSubscription.builder(subscriptionObserver, compactionKeyExtractor)
                        .withCoolDownDuration(coolDownDuration)
                        .withExecutor(throttlingExecutor)
                        .build());
    }

    /**
     * Register the subscription object to the Streamer.
     * Note! In case the clientId and address was already registered for that particular Streamer with different subscription,
     * then the later subscription is going to be closed as result of this method. In case the subscription is exactly same (==), then nothing will happen.
     *
     * @param subscription - subscription to register
     */
    public void subscribe(ServerGrpcSubscription<U> subscription) {
        if (subscription == null) {
            throw new IllegalArgumentException("Subscription can not be null.");
        }
        handleDoubleSubscription(subscription);
    }

    /**
     * Creates and registers the Throttling subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     *
     * @param subscriptionKey        - the identification of the client by id and address
     * @param observer               - the gRPC observer object provided from the method call site
     * @param compactionKeyExtractor - the key extraction function that is going to be used for key-based compaction
     * @param <K>                    - the type of the key
     */
    public <K> void subscribeThrottling(SubscriptionKey subscriptionKey, StreamObserver<U> observer,
                                        Function<U, K> compactionKeyExtractor) {
        subscribe(createThrottlingSubscription(subscriptionKey, observer, compactionKeyExtractor));
    }

    /**
     * Creates and registers the Full Flow subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     *
     * @param subscriptionKey - the identification of the client
     * @param observer        - the gRPC observer object provided from the method call site
     */
    public void subscribeFullFlow(SubscriptionKey subscriptionKey, StreamObserver<U> observer) {
        subscribe(createFullSubscription(subscriptionKey, observer));
    }

    /**
     * Creates and registers the Persistent subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     *
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     */
    @SuppressWarnings("unused")
    public void subscribePersistent(String clientId, StreamObserver<U> observer) {
        throw new UnsupportedOperationException("The operation is currently unsupported");
    }

    public final boolean hasSubscriptions() {
        return !subscriptionsByKey.isEmpty();
    }

    public Collection<SubscriptionDescriptor> getSubscriptionDescriptors() {
        return subscriptionsByKey.values().stream()
                .map(s -> new SubscriptionDescriptor(s.subscriptionKey().address(),
                        s.subscriptionKey().clientId(),
                        s.type()))
                .collect(Collectors.toList());
    }

    public void submitResponse(U response) {
        if (hasSubscriptions()) {
            notifyListeners(response);
        }
    }

    public void submitResponses(Collection<U> responses) {
        if (hasSubscriptions()) {
            responses.forEach(this::notifyListeners);
        }
    }

    public final void completeStreamer() {
        throttlingExecutor.shutdownNow();
        subscriptionsByKey.values().forEach(GrpcSubscription::onCompleted);
        subscriptionsByKey.clear();
    }

    @SuppressWarnings("unused")
    public final void completeStreamerWithError(Throwable t) {
        subscriptionsByKey.values().forEach(o -> o.onError(t));
        subscriptionsByKey.clear();
    }

    public void stop() {
        completeStreamer();
    }

    protected void handleDoubleSubscription(ServerGrpcSubscription<U> subscription) {
        GrpcSubscription<U> previous = subscriptionsByKey.putIfAbsent(subscription.subscriptionKey(), subscription);
        if (previous != null && previous != subscription) {
            //We close new and not the previous stream
            closeSubscription(subscription);
        }
    }

    protected SubscriptionObserver<U> createObserver(StreamObserver<U> observer) {
        if (observer instanceof ServerCallStreamObserver<U>) {
            return new ServerSubscriptionObserver<>((ServerCallStreamObserver<U>) observer);
        }
        throw new IllegalArgumentException("Observer is of unknown type: [" + observer.getClass() + "]");
    }

    private void closeSubscription(ServerGrpcSubscription<U> subscription) {
        if (subscription != null) {
            var key = subscription.subscriptionKey();
            try {
                LOGGER.debug("Open stream found for client {} on address {}, closing.", key.clientId(),
                        key.address());
                subscription.onCompleted();
            } catch (Exception e) {
                LOGGER.error("Error while closing stream for client {} address {}", key.clientId(),
                        key.address(), e);
            }
        }
    }

    protected void notifyListeners(U update) {
        for (var subscription : subscriptionsByKey.values()) {
            try {
                if (subscription.isActive()) {
                    subscription.processUpdate(update);
                } else {
                    //the subscription is closed by now we can remove it
                    subscriptionsByKey.remove(subscription.subscriptionKey(), subscription);
                }
            } catch (Exception e) {
                LOGGER.debug("Error while handling update for client {} and address {}",
                        subscription.subscriptionKey().clientId(), subscription.subscriptionKey().address(), e);
                subscription.onError(e);
                subscriptionsByKey.remove(subscription.subscriptionKey(), subscription);
            }
        }
    }
}

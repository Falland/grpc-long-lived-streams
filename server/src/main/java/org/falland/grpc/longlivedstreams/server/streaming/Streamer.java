package org.falland.grpc.longlivedstreams.server.streaming;

import org.falland.grpc.longlivedstreams.server.address.AddressInterceptor;
import org.falland.grpc.longlivedstreams.server.subscription.FullFlowSubscription;
import org.falland.grpc.longlivedstreams.server.subscription.GrpcSubscription;
import org.falland.grpc.longlivedstreams.server.subscription.SubscriptionDescriptor;
import org.falland.grpc.longlivedstreams.server.subscription.SubscriptionObserver;
import org.falland.grpc.longlivedstreams.server.subscription.SubscriptionType;
import org.falland.grpc.longlivedstreams.server.subscription.ThrottlingSubscription;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The class that provides the safe server side streaming support for gRPC services
 * This class takes care of slow consumers/fast producers by exposing two types of subscriptions.
 * Full Flow: all messages produced are sent to the client. If the client is slow to consume then messages are queued.
 * In order to avoid unlimited queue growth there's a limit for the queue. Once the queue reaches the limit the stream is closed with exception.
 * It's up to the client to reconnect or halt.
 * Note! While disconnected the service does not store the messages produced, so client might lose messages in between reconnects
 * Throttling: only the latest messages are reaching the client. In case the client is too slow to consume all the messages
 * the key-based compaction will be performed. Key extraction function is provided when creating the subscription.
 * Compactor is guaranteed to preserve the key order. I.e . the key that appeared first in the stream will be sent first.
 * @param <U> the message type of the streamer
 */
public class Streamer<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Streamer.class);

    private final Map<String, GrpcSubscription<U>> clientAddressToStream = new ConcurrentHashMap<>();
    private final ScheduledExecutorService throttlingExecutor;
    protected final String serviceName;
    protected final int queueSize;
    protected final Duration threadCoolDownWhenNotReady;

    public Streamer(String serviceName, int queueSize,
                    Duration threadCoolDownWhenNotReady) {
        this.serviceName = serviceName;
        this.queueSize = queueSize;
        this.threadCoolDownWhenNotReady = threadCoolDownWhenNotReady;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(serviceName + "-throttler-%d")
                .build();
        this.throttlingExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Creates the Full Flow subscription. You can use it to send the snapshot prior to finalize the subscription with {@link Streamer#subscribe(GrpcSubscription)}
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     * @return GrpcSubscription with the type {@link SubscriptionType#FULL_FLOW}
     */
    public GrpcSubscription<U> createFullSubscription(String clientId, StreamObserver<U> observer) {
        SubscriptionObserver<U> subscriptionObserver = createObserver((ServerCallStreamObserver<U>) observer);
        return FullFlowSubscription.builder(subscriptionObserver)
                .withClientId(clientId)
                .withQueueSize(queueSize)
                .withServiceName(serviceName)
                .withThreadCoolDownWhenNotReady(threadCoolDownWhenNotReady)
                .build();
    }

    /**
     * Creates the Throttling subscription. You can use it to send the snapshot prior to finalize the subscription with {@link Streamer#subscribe(GrpcSubscription)}
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     * @param compactionKeyExtractor - the key extraction function that is going to be used for key-based compaction
     * @param <K> - the type of the key
     * @return GrpcSubscription with the type {@link SubscriptionType#THROTTLING}
     */
    public <K> GrpcSubscription<U> createThrottlingSubscription(String clientId, StreamObserver<U> observer,
                                                                Function<U, K> compactionKeyExtractor) {
        SubscriptionObserver<U> subscriptionObserver = createObserver((ServerCallStreamObserver<U>) observer);
        return ThrottlingSubscription.builder(subscriptionObserver, compactionKeyExtractor)
                .withClientId(clientId)
                .withServiceName(serviceName)
                .withThreadCoolDownWhenNotReady(threadCoolDownWhenNotReady)
                .withExecutor(throttlingExecutor)
                .build();
    }

    /**
     * Register the subscription object to the Streamer.
     * Note! In case the clientId and address was already registered for that particular Streamer with different subscription,
     * then the later subscription is going to be closed as result of this method. In case the subscription is exactly same (==), then nothing will happen.
     *
     * @param subscription - subscription to register
     */
    public void subscribe(GrpcSubscription<U> subscription) {
        if (subscription == null) {
            throw new IllegalArgumentException("Subscription can not be null.");
        }
        handleDoubleSubscription(getKey(subscription.getAddress(), subscription.getClientId()),
                                 subscription, subscription.getAddress());
    }

    /**
     * Creates and registers the Throttling subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     * @param compactionKeyExtractor - the key extraction function that is going to be used for key-based compaction
     * @param <K> - the type of the key
     */
    public <K> void subscribeThrottling(String clientId, StreamObserver<U> observer,
                                        Function<U, K> compactionKeyExtractor) {
        subscribe(createThrottlingSubscription(clientId, observer, compactionKeyExtractor));
    }

    /**
     * Creates and registers the Full Flow subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     */
    public void subscribeFullFlow(String clientId, StreamObserver<U> observer) {
        subscribe(createFullSubscription(clientId, observer));
    }

    /**
     * Creates and registers the Persistent subscription.
     * Make sure you don't create two subscriptions on one observer. This may lead to incorrect behaviour on the clint side.
     * @param clientId - the identification of the client
     * @param observer - the gRPC observer object provided from the method call site
     */
    public void subscribePersistent(String clientId, StreamObserver<U> observer) {
        throw new UnsupportedOperationException("The operation is currently unsupported");
    }

    public final boolean hasSubscriptions() {
        return !clientAddressToStream.isEmpty();
    }

    public Collection<SubscriptionDescriptor> getSubscriptionDescriptors() {
        return clientAddressToStream.values().stream()
                .map(sub -> new SubscriptionDescriptor(sub.getAddress(),
                                                       sub.getClientId(),
                                                       sub.getType()))
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
        clientAddressToStream.values().forEach(GrpcSubscription::onCompleted);
        clientAddressToStream.clear();
    }

    public final void completeStreamerWithError(Throwable t) {
        clientAddressToStream.values().forEach(o -> o.onError(t));
        clientAddressToStream.clear();
    }

    public void stop() {
        completeStreamer();
    }

    protected void handleDoubleSubscription(String key, GrpcSubscription<U> subscription, String address) {
        GrpcSubscription<U> previous = clientAddressToStream.putIfAbsent(key, subscription);
        if (previous != null && previous != subscription) {
            //We close new and not the previous stream
            closeSubscription(address, subscription);
        }
    }

    protected String getKey(String address, String clientId) {
        return address + "_" + clientId;
    }

    protected SubscriptionObserver<U> createObserver(ServerCallStreamObserver<U> observer) {
        SocketAddress address = AddressInterceptor.ADDRESS_KEY.get();
        String addressString = address == null ? "null" : address.toString();
        return new SubscriptionObserver<>(addressString, observer);
    }

    private void closeSubscription(String address, GrpcSubscription<U> subscription) {
        try {
            if (subscription != null) {
                LOGGER.debug("Open stream found for client {} on address {}, closing.", subscription.getClientId(),
                             address);
                subscription.onCompleted();
            }
        } catch (Exception e) {
            LOGGER.error("Error while closing stream for client {} address {}", subscription.getClientId(), address, e);
        }
    }

    protected void notifyListeners(U update) {
        for (Map.Entry<String, GrpcSubscription<U>> entry : clientAddressToStream.entrySet()) {
            GrpcSubscription<U> subscription = entry.getValue();
            try {
                if (subscription.isActive()) {
                    subscription.processUpdate(update);
                } else {
                    //the subscription is closed by now we can remove it
                    clientAddressToStream.remove(entry.getKey(), subscription);
                }
            } catch (Exception e) {
                LOGGER.debug("Error while handling update for client {}", subscription.getClientId(), e);
                subscription.onError(e);
                clientAddressToStream.remove(entry.getKey(), subscription);
            }
        }
    }
}

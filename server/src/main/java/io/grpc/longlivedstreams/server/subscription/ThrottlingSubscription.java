package io.grpc.longlivedstreams.server.subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrottlingSubscription<U, K> implements GrpcSubscription<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingSubscription.class);
    public static final String THROTTLED = "throttled";

    private final String clientId;
    private final SubscriptionObserver<U> observer;
    private final CompactingQueue<K, U> updatesQueue;
    private final ScheduledExecutorService throttledSender;
    private final long coolDownWhenNotReadyNano;

    private ThrottlingSubscription(Builder<U, K> builder) {
        this.observer = builder.observer;
        this.clientId = builder.clientId;
        String serviceName = builder.serviceName;
        this.updatesQueue = new CompactingQueue<>(builder.compactionKeyExtractor);
        this.coolDownWhenNotReadyNano = Objects.requireNonNull(builder.threadCoolDownWhenNotReady).toNanos();
        this.throttledSender = builder.throttledSender;

        throttledSender.schedule(this::trySendMessage, 0, TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("squid:S1181")
    //we need to catch throwable as DirectMemoryError is an Error and not Exception
    private void trySendMessage() {
        if (observer.isOpened() && observer.isReady()) {
            U update = updatesQueue.poll();
            if (update != null) {
                try {
                    observer.onNext(update);
                    throttledSender.schedule(this::trySendMessage, 0, TimeUnit.NANOSECONDS);
                    return;
                } catch (Throwable e) {
                    observer.onError(e);
                    //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                    LOGGER.debug("Error while handling update for client {}", clientId, e);
                }
            }
        }
        throttledSender.schedule(this::trySendMessage, coolDownWhenNotReadyNano, TimeUnit.NANOSECONDS);
    }

    @Override
    public String getAddress() {
        return observer.getAddress();
    }

    @Override
    public SubscriptionType getType() {
        return SubscriptionType.THROTTLING;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void processUpdate(U update) {
        updatesQueue.offer(update);
    }

    @Override
    public boolean isActive() {
        return observer.isOpened();
    }

    @Override
    public void onError(Throwable t) {
        observer.onError(t);
    }

    @Override
    public void onCompleted() {
        observer.onCompleted();
    }

    public static <U, K> Builder<U, K> builder(SubscriptionObserver<U> observer,
                                               Function<U, K> compactionKeyExtractor) {
        return new Builder<>(observer, compactionKeyExtractor);
    }

    public static class Builder<U, K> {
        private final SubscriptionObserver<U> observer;
        private final Function<U, K> compactionKeyExtractor;
        private String clientId;
        private String serviceName;
        private Duration threadCoolDownWhenNotReady;
        private ScheduledExecutorService throttledSender;

        public Builder(SubscriptionObserver<U> observer, Function<U, K> compactionKeyExtractor) {
            this.observer = observer;
            this.compactionKeyExtractor = compactionKeyExtractor;
        }

        public Builder<U, K> withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder<U, K> withServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder<U, K> withThreadCoolDownWhenNotReady(Duration threadCoolDownWhenNotReady) {
            this.threadCoolDownWhenNotReady = threadCoolDownWhenNotReady;
            return this;
        }

        public Builder<U, K> withExecutor(ScheduledExecutorService scheduledExecutorService) {
            this.throttledSender = scheduledExecutorService;
            return this;
        }

        public ThrottlingSubscription<U, K> build() {
            return new ThrottlingSubscription<>(this);
        }
    }
}

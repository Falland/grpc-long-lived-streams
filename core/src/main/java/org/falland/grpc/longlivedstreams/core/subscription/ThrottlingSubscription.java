package org.falland.grpc.longlivedstreams.core.subscription;

import org.falland.grpc.longlivedstreams.core.SubscriptionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ThrottlingSubscription<U, K> implements GrpcSubscription<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingSubscription.class);

    private final SubscriptionObserver<U> observer;
    private final CompactingQueue<K, U> updatesQueue;
    private final ScheduledExecutorService throttledSender;
    private final long coolDownWhenNotReadyNano;

    private ThrottlingSubscription(Builder<U, K> builder) {
        this.observer = builder.observer;
        this.updatesQueue = new CompactingQueue<>(builder.compactionKeyExtractor);
        this.coolDownWhenNotReadyNano = Objects.requireNonNull(builder.coolDownDuration).toNanos();
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
                    LOGGER.debug("Error while handling update {}", update, e);
                }
            }
        }
        throttledSender.schedule(this::trySendMessage, coolDownWhenNotReadyNano, TimeUnit.NANOSECONDS);
    }

    @Override
    public SubscriptionType type() {
        return SubscriptionType.THROTTLING;
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
        private Duration coolDownDuration;
        private ScheduledExecutorService throttledSender;

        public Builder(SubscriptionObserver<U> observer, Function<U, K> compactionKeyExtractor) {
            this.observer = observer;
            this.compactionKeyExtractor = compactionKeyExtractor;
        }

        /**
         * The duration to wait before trying to send message to StreamObserver once it signaled being not ready
         * @param coolDownDuration - the duration for sending thread to wait before re-attempting send
         * @return - builder
         */
        public Builder<U, K> withCoolDownDuration(Duration coolDownDuration) {
            this.coolDownDuration = coolDownDuration;
            return this;
        }

        public Builder<U, K> withExecutor(ScheduledExecutorService scheduledExecutorService) {
            this.throttledSender = scheduledExecutorService;
            return this;
        }

        public ThrottlingSubscription<U, K> build() {
            Objects.requireNonNull(throttledSender, "Executor can't be null");
            Objects.requireNonNull(coolDownDuration, "Cool down duration can't be null");
            return new ThrottlingSubscription<>(this);
        }
    }
}

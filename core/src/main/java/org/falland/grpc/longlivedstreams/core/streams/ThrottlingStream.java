package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.GrpcStream;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ThrottlingStream<U, K> implements GrpcStream<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingStream.class);
    private final ControlledStreamObserver<U> observer;
    private final CompactingQueue<K, U> updatesQueue;
    private final ScheduledExecutorService executor;
    private final long coolDownNanos;

    private ThrottlingStream(Builder<U, K> builder) {
        this.observer = builder.observer;
        this.updatesQueue = new CompactingQueue<>(builder.compactionKeyExtractor);
        this.coolDownNanos = Objects.requireNonNull(builder.coolDownDuration).toNanos();
        this.executor = builder.executor;
        executor.schedule(this::trySendMessage, 0, TimeUnit.NANOSECONDS);
    }

    //we need to catch throwable as DirectMemoryError is an Error and not Exception
    private void trySendMessage() {
        if (observer.isOpened()) {
            U update = updatesQueue.poll();
            while (observer.isReady() && update != null) {
                try {
                    observer.onNext(update);
                    update = updatesQueue.poll();
                } catch (Throwable e) {
                    observer.onError(e);
                    //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                    LOGGER.debug("Error while handling update {}", update, e);
                    return;
                }
            }
            executor.schedule(this::trySendMessage, coolDownNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public StreamType type() {
        return StreamType.THROTTLING;
    }

    @Override
    public void onNext(U update) {
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

    public static <U, K> Builder<U, K> builder() {
        return new Builder<>();
    }

    public static class Builder<U, K> {
        private ControlledStreamObserver<U> observer;
        private Function<U, K> compactionKeyExtractor;
        private Duration coolDownDuration;
        private ScheduledExecutorService executor;

        public Builder() {
        }

        public Builder<U, K> withObserver(ControlledStreamObserver<U> observer) {
            Objects.requireNonNull(observer, "Observer can't be null");
            this.observer = observer;
            return this;
        }

        public Builder<U, K> withKeyExtractor(Function<U, K> compactionKeyExtractor) {
            Objects.requireNonNull(compactionKeyExtractor, "Key extractor can't be null");
            this.compactionKeyExtractor = compactionKeyExtractor;
            return this;
        }

        /**
         * The duration to wait before trying to send message to StreamObserver once it signaled being not ready
         * Stream would also wait for cool down duration in case the sending queue is empty
         *
         * @param coolDownDuration - the duration for sending thread to wait before re-attempting send
         * @return - builder
         */
        public Builder<U, K> withCoolDownDuration(Duration coolDownDuration) {
            Objects.requireNonNull(coolDownDuration, "Cool down duration can't be null");
            this.coolDownDuration = coolDownDuration;
            return this;
        }

        public Builder<U, K> withExecutor(ScheduledExecutorService scheduledExecutorService) {
            Objects.requireNonNull(scheduledExecutorService, "Executor can't be null");
            this.executor = scheduledExecutorService;
            return this;
        }

        public ThrottlingStream<U, K> build() {
            return new ThrottlingStream<>(this);
        }
    }
}

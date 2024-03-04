package org.falland.grpc.longlivedstreams.core.keepalive;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.falland.grpc.longlivedstreams.core.strategy.DiscardNewOnOverflow;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The stream that wraps gRPC stream with keep alive logic.
 * The service uses very straightforward strategy. It sends a heartbeat message every defined keep alive period.
 * The heartbeat message is defined by the client.
 * <p>
 * Note! KeepAliveStream doesn't care of the semantics of the protocol/heartbeat message.
 * It is a sole responsibility of the client to provide a correct message that will be accepted by the stream consumers.
 * The recommendation therefore is to include heartbeat message into the stream protocol
 * <p>
 * Note! KeepAliveStream won't shut down the provided executor during {@link #close} and only cancel the heartbeat task
 * Note! In case executor is not provided KeepAliveStream will create internal executor and terminate it on {@link #close()}
 */
public class KeepAliveStreamObserver<V> implements ControlledStreamObserver<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardNewOnOverflow.class);
    private final ControlledStreamObserver<V> delegate;
    private final Supplier<V> heartBeatProducer;
    private final ScheduledExecutorService executor;
    private final boolean internalExecutor;
    private final ScheduledFuture<?> keepAliveFuture;

    private KeepAliveStreamObserver(Builder<V> builder) {
        this.delegate = builder.streamObserver;
        this.heartBeatProducer = builder.heartBeatProducer;
        this.executor = builder.executor;
        this.internalExecutor = builder.internalExecutor;
        long keepAlivePeriodNanos = builder.keepAliveDuration.toNanos();
        this.keepAliveFuture = executor.scheduleAtFixedRate(this::sendHeartbeat,
                keepAlivePeriodNanos,
                keepAlivePeriodNanos,
                TimeUnit.NANOSECONDS);
    }

    private void sendHeartbeat() {
        try {
            onNext(heartBeatProducer.get());
        } catch (Exception e) {
            LOGGER.error("Error while sending heartbeat. Skipping", e);
        }
    }

    public void close() {
        keepAliveFuture.cancel(true);
        if (internalExecutor) {
            executor.shutdownNow();
        }
    }

    @Override
    public void onNext(V value) {
        delegate.onNext(value);
    }

    @Override
    public boolean isOpened() {
        return delegate.isOpened();
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
        close();
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
        close();
    }

    public static <V> Builder<V> builder() {
        return new Builder<>();
    }

    public static class Builder<V> {
        private ScheduledExecutorService executor;
        private boolean internalExecutor = false;
        private ControlledStreamObserver<V> streamObserver;
        private Supplier<V> heartBeatProducer;
        private Duration keepAliveDuration;

        public Builder<V> withExecutor(@Nonnull ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder<V> withStreamObserver(@Nonnull ControlledStreamObserver<V> stream) {
            this.streamObserver = stream;
            return this;
        }

        public Builder<V> withHeartBeatProducer(@Nonnull Supplier<V> heartBeatProducer) {
            this.heartBeatProducer = heartBeatProducer;
            return this;
        }

        public Builder<V> withKeepAliveDuration(@Nonnull Duration keepAliveDuration) {
            this.keepAliveDuration = keepAliveDuration;
            return this;
        }

        public KeepAliveStreamObserver<V> build() {
            Objects.requireNonNull(streamObserver, "Stream was not set.");
            Objects.requireNonNull(heartBeatProducer, "Heartbeat producer was not set.");
            Objects.requireNonNull(keepAliveDuration, "Keep alive duration was not set.");
            if (executor == null) {
                executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("keep-alive-thread-", false));
                internalExecutor = true;
            }
            return new KeepAliveStreamObserver<>(this);
        }
    }
}

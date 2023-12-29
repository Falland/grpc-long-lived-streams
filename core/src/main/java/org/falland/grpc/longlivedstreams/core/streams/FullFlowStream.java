package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.GrpcStream;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

public class FullFlowStream<U> implements GrpcStream<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullFlowStream.class);
    public static final YouAreTooSlowException TOO_SLOW_EXCEPTION =
            new YouAreTooSlowException("The server queue is filled, you are not keeping up.");
    private final ExecutorService executor;
    private final ControlledStreamObserver<U> observer;
    private final long coolDownNanos;
    private final BlockingQueue<U> updatesQueue;
    private volatile boolean isActive = true;

    private FullFlowStream(Builder<U> builder) {
        int queueSize = builder.queueSize;
        this.observer = builder.observer;
        this.coolDownNanos = builder.coolDownDuration.toNanos();
        this.updatesQueue = new LinkedBlockingQueue<>(queueSize);
        this.executor = Executors.newSingleThreadExecutor(builder.threadFactory);

        executor.submit(this::trySendMessage);
    }

    @Override
    public StreamType type() {
        return StreamType.FULL_FLOW;
    }

    @Override
    public void onNext(U update) {
        if (!updatesQueue.offer(update)) {
            onError(TOO_SLOW_EXCEPTION);
        }
    }

    @SuppressWarnings("squid:S1181")
    //we need to catch throwable as DirectMemoryError is an Error and not Exception
    private void trySendMessage() {
        while (isActive) {
            if (!sendMessage()) {
                //Can be susceptible for spurious wake-ups but for this application it's benign outcome
                LockSupport.parkNanos(coolDownNanos);
            }
        }
    }

    private boolean sendMessage() {
        U update = updatesQueue.poll();
        if (observer.isReady() && update != null) {
            try {
                observer.onNext(update);
            } catch (Throwable e) {
                observer.onError(e);
                //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                LOGGER.debug("Error while handling update {}", update, e);
            }
            //We don't want to wait in case the error happened, so we treat it as a sent message
            return true;
        }
        return false;
    }

    @Override
    public boolean isActive() {
        return isActive && observer.isOpened();
    }

    @Override
    public void onError(Throwable t) {
        stop();
        observer.onError(t);
    }

    @Override
    public void onCompleted() {
        stop();
        observer.onCompleted();
    }

    private void stop() {
        isActive = false;
        executor.shutdownNow();
    }

    public static <U> Builder<U> builder() {
        return new Builder<>();
    }

    public static class Builder<U> {
        private ControlledStreamObserver<U> observer;
        private ThreadFactory threadFactory;
        private int queueSize;
        private Duration coolDownDuration;

        public Builder() {
        }

        public Builder<U> withObserver(ControlledStreamObserver<U> observer) {
            Objects.requireNonNull(observer, "Observer can't be null");
            this.observer = observer;
            return this;
        }

        public Builder<U> withThreadFactory(ThreadFactory threadFactory) {
            Objects.requireNonNull(threadFactory, "Thread factory can't be null");
            this.threadFactory = threadFactory;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<U> withQueueSize(int queueSize) {
            if (queueSize <= 0) {
                throw new IllegalArgumentException("Queue size can not be less or equal to zero.");
            }
            this.queueSize = queueSize;
            return this;
        }

        /**
         * The duration to wait before trying to send message to StreamObserver once it signaled being not ready
         * Stream would also wait for cool down duration in case the sending queue is empty
         *
         * @param coolDownDuration - the duration for sending thread to wait before re-attempting send
         * @return - builder
         */
        public Builder<U> withCoolDownDuration(Duration coolDownDuration) {
            Objects.requireNonNull(coolDownDuration, "Cool down duration can't be null");
            this.coolDownDuration = coolDownDuration;
            return this;
        }

        public FullFlowStream<U> build() {
            return new FullFlowStream<>(this);
        }
    }
}

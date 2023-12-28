package org.falland.grpc.longlivedstreams.core.subscription;

import org.falland.grpc.longlivedstreams.core.SubscriptionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

public class FullFlowSubscription<U> implements GrpcSubscription<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullFlowSubscription.class);
    public static final YouAreTooSlowException TOO_SLOW_EXCEPTION =
            new YouAreTooSlowException("The server queue is filled, you are not keeping up.");
    private final ExecutorService executor;
    private final SubscriptionObserver<U> observer;
    private final Duration coolDownDuration;
    private final BlockingQueue<U> updatesQueue;

    private volatile boolean isActive = true;

    private FullFlowSubscription(Builder<U> builder) {
        int queueSize = builder.queueSize;
        this.observer = Objects.requireNonNull(builder.observer);
        this.coolDownDuration = Objects.requireNonNull(builder.coolDownDuration);
        this.updatesQueue = new LinkedBlockingQueue<>(queueSize);
        this.executor = Executors.newSingleThreadExecutor(builder.threadFactory);

        executor.submit(this::trySendMessage);
    }

    @Override
    public SubscriptionType type() {
        return SubscriptionType.FULL_FLOW;
    }

    @Override
    public void processUpdate(U update) {
        if (!updatesQueue.offer(update)) {
            onError(TOO_SLOW_EXCEPTION);
        }
    }

    @SuppressWarnings("squid:S1181")
    //we need to catch throwable as DirectMemoryError is an Error and not Exception
    private void trySendMessage() {
        while (isActive) {
            try {
                boolean messageSent = false;
                if (observer.isReady()) {
                    messageSent = sendMessage(messageSent);
                }
                if (!messageSent) {
                    TimeUnit.NANOSECONDS.sleep(coolDownDuration.toNanos());
                }
            } catch (InterruptedException e) {
                //we are interrupted, hence the thread should stop, therefore we complete the stream
                LOGGER.info("Interrupted while waiting on stream to be ready. Closing stream");
                onCompleted();
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.debug("Error while sending message", e);
                onError(e);
            }
        }
    }

    private boolean sendMessage(boolean messageSent) {
        U update = updatesQueue.poll();
        if (update != null) {
            try {
                observer.onNext(update);
            } catch (Throwable e) {
                observer.onError(e);
                //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                LOGGER.debug("Error while handling update {}", update, e);
            }
            messageSent = true;
        }
        return messageSent;
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

    public static <U> Builder<U> builder(SubscriptionObserver<U> observer) {
        return new Builder<>(observer);
    }

    public static class Builder<U> {
        private final SubscriptionObserver<U> observer;
        private ThreadFactory threadFactory;
        private int queueSize;
        private Duration coolDownDuration;

        public Builder(SubscriptionObserver<U> observer) {
            this.observer = observer;
        }

        public Builder<U> withThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<U> withQueueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        /**
         * The duration to wait before trying to send message to StreamObserver once it signaled being not ready
         *
         * @param coolDownDuration - the duration for sending thread to wait before re-attempting send
         * @return - builder
         */
        public Builder<U> withCoolDownDuration(Duration coolDownDuration) {
            this.coolDownDuration = coolDownDuration;
            return this;
        }

        public FullFlowSubscription<U> build() {
            Objects.requireNonNull(threadFactory, "Thread factory can't be null");
            Objects.requireNonNull(coolDownDuration, "Cool down duration can't be null");
            if (queueSize <= 0) {
                throw new IllegalArgumentException("Queue size can not be less or equal to zero.");
            }
            return new FullFlowSubscription<>(this);
        }
    }
}

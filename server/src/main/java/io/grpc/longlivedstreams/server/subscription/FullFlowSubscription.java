package io.grpc.longlivedstreams.server.subscription;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

public class FullFlowSubscription<U> implements GrpcSubscription<U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullFlowSubscription.class);
    public static final YouAreTooSlowException TOO_SLOW_EXCEPTION =
            new YouAreTooSlowException("The server queue is filled, you are not keeping up.");

    private final String clientId;
    private final ExecutorService executor;
    private final SubscriptionObserver<U> observer;
    private final Duration threadCoolDownWhenNotReady;
    private final BlockingQueue<U> updatesQueue;

    private volatile boolean isActive = true;

    private FullFlowSubscription(Builder<U> builder) {
        int queueSize = builder.queueSize;
        String serviceName = builder.serviceName;

        this.clientId = builder.clientId;
        this.observer = Objects.requireNonNull(builder.observer);
        this.threadCoolDownWhenNotReady = Objects.requireNonNull(builder.threadCoolDownWhenNotReady);
        this.updatesQueue = new LinkedBlockingQueue<>(queueSize);
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(serviceName + "- " + clientId + "-%d")
                .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);

        executor.submit(this::trySendMessage);
    }

    @Override
    public String getAddress() {
        return observer.getAddress();
    }

    @Override
    public SubscriptionType getType() {
        return SubscriptionType.FULL_FLOW;
    }

    @Override
    public String getClientId() {
        return clientId;
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
                    U update = updatesQueue.poll();
                    if (update != null) {
                        observer.onNext(update);
                        messageSent = true;
                    }
                }
                if (!messageSent) {
                    TimeUnit.NANOSECONDS.sleep(threadCoolDownWhenNotReady.toNanos());
                }
            } catch (InterruptedException e) {
                //we are interrupted, hence the thread should stop, therefore we complete the stream
                LOGGER.info("Interrupted while waiting on stream to be ready. Closing stream for client {}", clientId);
                onCompleted();
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                LOGGER.debug("Error while handling update for client {}", clientId, e);
                onError(e);
            }
        }
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
        private String clientId;
        private int queueSize;
        private String serviceName;
        private Duration threadCoolDownWhenNotReady;

        public Builder(SubscriptionObserver<U> observer) {
            this.observer = observer;
        }

        public Builder<U> withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder<U> withQueueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public Builder<U> withServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder<U> withThreadCoolDownWhenNotReady(Duration threadCoolDownWhenNotReady) {
            this.threadCoolDownWhenNotReady = threadCoolDownWhenNotReady;
            return this;
        }

        public FullFlowSubscription<U> build() {
            return new FullFlowSubscription<>(this);
        }
    }
}

package org.falland.grpc.longlivedstreams.core.streams;

import io.grpc.stub.ClientCallStreamObserver;
import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.falland.grpc.longlivedstreams.core.strategy.BackpressureStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * The implementation of the {@link ControlledStreamObserver} that has flow control feature
 * The exact mechanism of back-pressing the producer is implemented by {@link BackpressureStrategy}
 * This implementation is build on the experimental features in {@link io.grpc.stub.ClientCallStreamObserver}:
 * <p>
 *    1) {@link ClientCallStreamObserver#isReady()} this method checks that the underlying gRPC code is ready to receive an update to send it over the wire
 * <p>
 *    2) {@link ClientCallStreamObserver#setOnReadyHandler(Runnable)} this is a callback that one of the sending threads call once the gRPC code becomes available
 * <p>
 * On reception of the update the code will call backpressure strategy to back-press if needed.
 * Next it will try to send the update immediately.
 * If this could not be performed, then the code will send updates within the onReadyHandler call.
 * This observer will try to send as many updates as it is allowed to by following factors:
 * <p>
 *     1) The change of {@link  ClientCallStreamObserver#isReady()} flag that indicates that no more updates could be processed
 * <p>
 *     2) The amount of messages retrieved from the backpressure strategy
 * <p>
 *     3) The batch size set by the builder. Note! By default, batch size is equal to {@link Integer#MAX_VALUE}, which is effectively infinite
 * <p>
 * When stopped (either {@link #onError(Throwable)} or {@link #onCompleted()} method is called) this observer will stop underlying backpressure strategy as well
 * Due to the fact that underlying observer can be called from multiple threads (message producer and gRPC sending thread)
 * updates sending can be performed concurrently.
 * In order to guarantee the atomicity of sends synchronization is used on delegate observer level
 * @param <V> type of the observer message
 */
public class BackpressingStreamObserver<V> implements ControlledStreamObserver<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackpressingStreamObserver.class);
    private final FlowControlledObserver<V> observer;
    private final BackpressureStrategy<V> strategy;
    private final int batchSize;

    private BackpressingStreamObserver(Builder<V> builder) {
        this.observer = builder.observer;
        this.strategy = builder.strategy;
        this.batchSize = builder.batchSize;
        this.observer.setOnReadyHandler(this::trySendMessage);
    }

    private void trySendMessage() {
        if (observer.isOpened()) {
            int batchCount = 0;
            boolean canSendMore = true;
            while (canSendMore) {
                synchronized (observer) {
                    if (observer.isReady() && batchSize > batchCount) {
                        V message = strategy.poll();
                        if (message != null) {
                            try {
                                observer.onNext(message);
                            } catch (Throwable e) {
                                //Usually this happens due to lack of flow control, too many messages do not fit into gRPC buffer leading to DirectMemoryError
                                this.onError(e);
                                LOGGER.debug("Error while handling update {}", message, e);
                                return;
                            }
                            batchCount++;
                        } else {
                            canSendMore = false;
                        }
                    } else {
                        canSendMore = false;
                    }
                }
            }
        }
    }

    @Override
    public void onNext(V message) {
        if (observer.isOpened()) {
            strategy.offer(message);
            trySendMessage();
        }
    }

    @Override
    public boolean isOpened() {
        return observer.isOpened();
    }

    @Override
    public void onError(Throwable t) {
        observer.onError(t);
        strategy.stop();
    }

    @Override
    public void onCompleted() {
        observer.onCompleted();
        strategy.stop();
    }

    public static <V> Builder<V> builder() {
        return new Builder<>();
    }

    public static class Builder<V> {
        private FlowControlledObserver<V> observer;
        private BackpressureStrategy<V> strategy;
        private int batchSize = Integer.MAX_VALUE;

        public Builder<V> withObserver(@Nonnull FlowControlledObserver<V> observer) {
            this.observer = observer;
            return this;
        }

        public Builder<V> withStrategy(@Nonnull BackpressureStrategy<V> strategy) {
            this.strategy = strategy;
            return this;
        }

        public Builder<V> withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public BackpressingStreamObserver<V> build() {
            Objects.requireNonNull(observer, "Observer was not set.");
            Objects.requireNonNull(strategy, "Strategy was not set.");
            return new BackpressingStreamObserver<>(this);
        }
    }
}

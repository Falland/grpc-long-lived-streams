package org.falland.grpc.longlivedstreams.server.streaming;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;
import org.falland.grpc.longlivedstreams.core.keepalive.KeepAliveStreamObserver;
import org.falland.grpc.longlivedstreams.core.strategy.BackpressureStrategy;
import org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;


/**
 * The class provides the server side streaming support for gRPC services
 * It provides an API that connects single producer of the streaming data with multiple clients making streaming calls.
 * Hence, the Streamer is generified with a single type - the type of streaming message.
 * There are three flavours of stream observers created by this class: free flow, back pressing and keep alive
 * <p>
 * Free Flow: the gRPC vanilla stream, no back pressure is distributed on the message flow. Good for sparse streams.
 * Note! Can lead performance degradation due to sending queue overflow which can overload the GC or even lead to OOM.
 * <p>
 * Back Pressing: the stream with back pressure ability. Check implementations of {@link BackpressureStrategy} class to see supported strategies.
 * Good for dense long-lived streams. The reason this whole library exists. In general strategies allow flexible tuning of back pressure.
 * Note! Optimal configuration is defined by the user of the class. The flexibility allows for suboptimal configurations.
 * Measure the stream behavior with several strategies before finalizing the resulting config that goes to prod.
 * <p>
 * Keep Alive: the stream that need to be kept open with heartbeat messages that are sent down the observer periodically.
 * <p>
 * Note! Streamer does not store the messages produced by default, so client might miss several messages before it is connected (or during reconnects)
 *
 * @param <M> the message type of the stream
 */
public class Streamer<M> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Streamer.class);
    private final Collection<ControlledStreamObserver<M>> registeredObservers;
    private final ScheduledExecutorService keepAliveExecutor;
    private final boolean internalExecutor;
    private final int batchSize;
    private final Duration keepAliveDuration;

    private Streamer(int batchSize, @Nonnull Duration keepAliveDuration,
                     @Nonnull ScheduledExecutorService keepAliveExecutor, boolean internalExecutor) {
        this.keepAliveDuration = keepAliveDuration;
        this.batchSize = batchSize;
        this.keepAliveExecutor = keepAliveExecutor;
        this.internalExecutor = internalExecutor;
        this.registeredObservers = new ConcurrentLinkedQueue<>();
    }

    public Streamer(int batchSize, @Nonnull Duration keepAliveDuration, @Nonnull String eserviceName,
                    @Nonnull ScheduledExecutorService keepAliveExecutor) {
        this(batchSize, keepAliveDuration, keepAliveExecutor, false);
    }

    public Streamer(int batchSize, @Nonnull Duration keepAliveDuration, @Nonnull String serviceName) {
        this(batchSize, keepAliveDuration, Executors.newScheduledThreadPool(
                        Runtime.getRuntime().availableProcessors(),
                        new ThreadFactoryImpl(serviceName + "-keepAlive-", false)),
                true);
    }

    /**
     * Creates a wrapper around vanilla StreamObserver observer.
     * This wrapper only guarantees that call to {@link StreamObserver#onError(Throwable)} or {@link StreamObserver#onCompleted()} are effectively final.
     * All subsequent calls are going to be ignored after that.
     * The message flow is not back-pressed and as free as vanilla observer allows it.
     * Make sure you don't create two wrappers on top of same vanilla observer. This might lead to nasty side effects on client side or in the stream itself.
     *
     * @param observer - the gRPC observer object provided from the method call site
     * @return ControlledStreamObserver class.
     */
    public ControlledStreamObserver<M> freeFlowStreamObserver(@Nonnull StreamObserver<M> observer) {
        return createObserver(observer);
    }

    /**
     * Creates the back pressing stream that uses provided backpressure strategy.
     * This would protect observer from overflowing internal send queue and remove the risk of OOM.
     * Check the provided implementations of {@link BackpressureStrategy} to see the available strategies.
     * You can use this stream to create subscription in Streamer {@link Streamer#register(ControlledStreamObserver)}
     * Make sure you don't create two wrappers on top of same vanilla observer. This might lead to nasty side effects on client side or in the stream itself.
     *
     * @param observer             - the gRPC observer object provided from the method call site
     * @param backpressureStrategy - the back pressure strategy to use for this stream. E.g. block producer on overflow, or compact send queue on the key, etc.
     * @return ControlledStreamObserver that uses {@link BackpressureStrategy}.
     */
    public ControlledStreamObserver<M> backPressingStreamObserver(@Nonnull StreamObserver<M> observer,
                                                                  @Nonnull BackpressureStrategy<M> backpressureStrategy) {
        return BackpressingStreamObserver.<M>builder()
                .withObserver(createObserver(observer))
                .withStrategy(backpressureStrategy)
                .withBatchSize(batchSize)
                .build();
    }

    /**
     * Creates the keep alive wrapper around grpc stream.
     * This would automatically send heartbeat message over provided stream within {@link #keepAliveDuration} period.
     * You can use this stream to create subscription in Streamer {@link Streamer#register(ControlledStreamObserver)}
     * Make sure you don't create two wrappers on top of same vanilla observer. This might lead to nasty side effects on client side or in the stream itself.
     *
     * @param streamToKeepAlive - the gRPC stream object
     * @param heartBeatProducer - the heartbeat producer that produces valid heartbeat messages.
     * @return ServerGrpcStream with the type of provided stream.
     */
    public ControlledStreamObserver<M> keepAliveStreamObserver(@Nonnull StreamObserver<M> streamToKeepAlive, @Nonnull Supplier<M> heartBeatProducer) {
        return KeepAliveStreamObserver.<M>builder()
                .withStreamObserver(createObserver(streamToKeepAlive))
                .withHeartBeatProducer(heartBeatProducer)
                .withExecutor(keepAliveExecutor)
                .withKeepAliveDuration(keepAliveDuration)
                .build();
    }

    /**
     * Register the stream to the Streamer.
     * When the stream is exactly same (==) the subsequent call to this method would not have any effect.
     * Note! Do not register different streams that share same observer, this might lead to nasty issues on the client side or in the stream itself
     * Note! If the stream is closed externally it wil be eventually removed from the subscription list and not be updated anymore
     *
     * @param streamToRegister - streamToRegister to register
     * @return - boolean, true is stream is registered, false when this exact stream was already registered
     */
    public boolean register(@Nonnull ControlledStreamObserver<M> streamToRegister) {
        if (streamToRegister == null) {
            throw new IllegalArgumentException("Stream observer can't be null");
        }
        return registeredObservers.add(streamToRegister);
    }

    /**
     * Checks if this Streamer has any subscriptions registered
     *
     * @return - true if there are registered subscriptions, false otherwise
     */
    public final boolean hasStreams() {
        return !registeredObservers.isEmpty();
    }

    /**
     * Sends messages to all registered streams in this Streamer
     * In case a single stream failure to send message (e.g. stream externally closed or client has dropped connection)
     * this stream will be excluded for the subscription list but other streams are guaranteed to receive this message
     */
    public void sendMessage(M message) {
        sendMessageInternal(message);
    }

    /**
     * Closes this Streamer.
     * Call to this method leads to all registered streams to be closed with {@link ControlledStreamObserver#onCompleted()} method call
     * The subscription list will be cleared as well
     * In case internal executor is used it will be closed as well.
     */
    public final void complete() {
        if (internalExecutor) {
            keepAliveExecutor.shutdownNow();
        }
        registeredObservers.forEach(ControlledStreamObserver::onCompleted);
        registeredObservers.clear();
    }

    /**
     * Closes this Streamer with provided error.
     * Call to this method leads to all registered streams to be closed with {@link ControlledStreamObserver#onError(Throwable)}} method call
     * The subscription list will be cleared as well
     * In case internal executor is used it will be closed as well.
     */
    @SuppressWarnings("unused")
    public final void completeWithError(Throwable t) {
        if (internalExecutor) {
            keepAliveExecutor.shutdownNow();
        }
        registeredObservers.forEach(o -> o.onError(t));
        registeredObservers.clear();
    }

    private FlowControlledObserver<M> createObserver(StreamObserver<M> observer) {
        if (observer instanceof FlowControlledObserver<M> toReturn) {
            return toReturn;
        }
        if (observer instanceof ServerCallStreamObserver<M>) {
            return new FlowControlledObserver<>((CallStreamObserver<M>) observer);
        }
        throw new IllegalArgumentException("Observer is of unknown type: [" + observer.getClass() + "]");
    }

    private void sendMessageInternal(M update) {
        var streamsIterator = registeredObservers.iterator();
        while (streamsIterator.hasNext()) {
            var stream = streamsIterator.next();
            try {
                if (stream.isOpened()) {
                    stream.onNext(update);
                } else {
                    //the stream is closed by now we can remove it
                    streamsIterator.remove();
                }
            } catch (Exception e) {
                streamsIterator.remove();
                stream.onError(e);
                LOGGER.debug("Error while handling update for stream {}", stream, e);
            }
        }
    }
}

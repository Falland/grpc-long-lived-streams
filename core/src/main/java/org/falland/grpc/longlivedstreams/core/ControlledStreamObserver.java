package org.falland.grpc.longlivedstreams.core;

import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.streams.FilteringStreamObserver;
import org.falland.grpc.longlivedstreams.core.streams.TransformingStreamObserver;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * It is a more controllable and safe structure than plain StreamObserver.
 * The underlying implementations try to solve common issues for the Java StreamObservers for dense long-lived streams:
 * <p>
 *     1) No flow control and back pressure - leading to OOM in worst case when the producer overwhelms the sending queue
 *<p>
 *     2) No support for long-lived streaming - heartbeats, re-subscriptions etc.
 * The implementations of this interface can provide the capabilities of backpressure, heart-beats etc.
 * This interface is a way to mark all observers that follow the approach of safe streaming.
 * @param <V> - type of update for this stream
 */
public interface ControlledStreamObserver<V> extends StreamObserver<V> {

    boolean isOpened();
}

package org.falland.grpc.longlivedstreams.core.strategy;

import org.falland.grpc.longlivedstreams.core.util.CompactingQueue;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeByKey<V, K> implements BackpressureStrategy<V> {

    private final CompactingQueue<K, V> updatesQueue;

    public MergeByKey(@Nonnull Function<V, K> keyFunction, @Nonnull BiFunction<V, V, V> mergeFunction) {
        this.updatesQueue = new CompactingQueue<>(keyFunction, mergeFunction);
    }
    public MergeByKey(@Nonnull Function<V, K> keyFunction) {
       this(keyFunction, (a, b) -> b);
    }

    @Override
    public void offer(V value) {
        updatesQueue.offer(value);
    }

    @Override
    public V poll() {
        return updatesQueue.poll();
    }

    @Override
    public void stop() {
        //ToDo add isActive flag
        updatesQueue.clear();
    }


}

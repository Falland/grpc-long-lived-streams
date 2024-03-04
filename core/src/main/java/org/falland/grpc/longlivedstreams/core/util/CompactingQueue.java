package org.falland.grpc.longlivedstreams.core.util;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The class is implementing queue-like api with fair compaction functionality.
 * Compaction is performed on a key basis.
 * Any two events with same key will be compacted to one by applying merge function to both of them
 * <p>
 * The fairness of the queue is ensured by keeping the order of the keys from {@link #offer} perspective
 * E.g. for events (Event E with key K is denoted as E{K}) E1{K1}, E2{K2}, E3{K1}, E4{K3}
 * The order of retrieval will be (having the merge is just take the latest) - E3{K1}, E2{K2}, E4{K3}
 * Therefore event E3 associated with K1, as it was appearing first in the sequence of events, will be retrieved first and so on
 * <p>
 * Note! The implementation is not checking correctness and side effects of extraction and merge function.
 * Note! Implementation would call extraction and merge functions exactly once for each offer
 * Note! If the merge function is null then take latest function is going to be used, i.e. (a,b) -> b
 * <p>
 * The class is thread safe for all methods
 *
 * @param <K> - the type of the key
 * @param <V> - the type of the value
 */
public class CompactingQueue<K, V> {
    private final Function<V, K> compactionKeyExtractor;
    private final BiFunction<V, V, V> mergeFunction;
    private final Map<K, V> orderedKeys = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public CompactingQueue(@Nonnull Function<V, K> compactionKeyExtractor, BiFunction<V, V, V> mergeFunction) {
        this.compactionKeyExtractor = compactionKeyExtractor;
        this.mergeFunction = mergeFunction == null ? (a, b) -> b : mergeFunction;
    }

    //We should not have any race here as we ensure we access the collection in direct order in offer and reversed in poll
    public void offer(V value) {
        K key = compactionKeyExtractor.apply(value);
        lock.lock();
        try {
            orderedKeys.merge(key, value, mergeFunction);
        } finally {
            lock.unlock();
        }
    }

    public V poll() {
        lock.lock();
        try {
            if (orderedKeys.isEmpty()) {
                return null;
            }
            var iterator = orderedKeys.values().iterator();
            V toReturn = iterator.next();
            iterator.remove();
            return toReturn;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return orderedKeys.size();
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            orderedKeys.clear();
        } finally {
            lock.unlock();
        }
    }
}

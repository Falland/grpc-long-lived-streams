package com.falland.grpc.longlivedstreams.server.subscription;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * The class is implementing non-blocking queue api with fair compaction functionality.
 * Compaction is performed on compaction key basis.
 * Any two events with same key will be compacted to one by keeping the latest one (based on order of {@link #offer} method call)
 *
 * The fairness of the queue is ensured by keeping the order of the keys same as the were from {@link #offer} perspective
 * E.g. for events (Event E with key K is denoted as E{K}) E1{K1}, E2{K2}, E3{K1}, E4{K3}
 * The order of retrieval will be (having the compaction in mind) - E3{K1}, E2{K2}, E4{K3}
 * Therefore K1 as it was appearing first in the sequence of events will be retrieved first and so on
 *
 * In other words the queue is ensuring the order of compacting keys
 * Please note that if provided extractor is effectively returning single value for all events then the queue becomes
 * a simple accumulator for the latest value in the steam of events
 *
 * The class is thread safe both for offer and poll methods
 * @param <K> - the type of the key
 * @param <V> - the type of the value
 */
public class CompactingQueue<K, V> {

    private final Function<V, K> compactionKeyExtractor;
    private final Map<K, V> latestValues = new ConcurrentHashMap<>();
    private final Queue<K> activeKeys = new ConcurrentLinkedQueue<>();

    public CompactingQueue(Function<V, K> compactionKeyExtractor) {
        this.compactionKeyExtractor = compactionKeyExtractor;
    }

    //We should not have any race here as we ensure we access the collection in direct order in offer and reversed in poll
    public void offer(V value) {
        K key = compactionKeyExtractor.apply(value);
        if (latestValues.put(key, value) == null) {
            activeKeys.offer(key);
        }
    }

    //We should not have any race here as we ensure we access the collection in direct order in offer and reversed in poll
    public V poll() {
        K key = activeKeys.poll();
        if (key != null) {
            return latestValues.remove(key);
        }
        return null;
    }

    public int size() {
        return latestValues.size();
    }
}

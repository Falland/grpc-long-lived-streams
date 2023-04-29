package com.falland.grpc.longlivedstreams.server.subscription;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompactingQueueTest {

    private CompactingQueue<Long, Long> underTest;

    @BeforeEach
    public void beforeEach() {
        //The key is module 10 of provided long, which gives us keys [0..9] for any long provided
        underTest = new CompactingQueue<>(l -> l % 10);
    }

    @Test
    public void testOffer_compactsSameKeys_always() {
        underTest.offer(1L);
        underTest.offer(1L);
        underTest.offer(1L);

        assertEquals(1L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_shouldKeepLatestValueForTheKey_always() {
        underTest.offer(1L);
        underTest.offer(11L);
        underTest.offer(111L);

        assertEquals(111L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_doesNotCompactDifferentKeys_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        assertEquals(1L, underTest.poll());
        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_keepsOrderOfKeys_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(11L);
        underTest.offer(3L);

        //Key for 1 and 11 is the same hence we poll latest event with this key, which is 11
        assertEquals(11L, underTest.poll());
        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testSize_shouldReturnExactQueueSize_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(11L);
        underTest.offer(3L);

        assertEquals(3, underTest.size());
    }

    @Test
    public void testPoll_shouldReturnNull_whenEmpty() {
        assertNull(underTest.poll());
    }
}
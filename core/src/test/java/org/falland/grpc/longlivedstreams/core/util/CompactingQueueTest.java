package org.falland.grpc.longlivedstreams.core.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CompactingQueueTest {

    private CompactingQueue<Long, Long> underTest;

    @BeforeEach
    public void beforeEach() {
        //The key is module 10 of provided long, which gives us keys [0..9] for any long provided
        //Merge function would just leave us with biggest of two numbers
        underTest = new CompactingQueue<>(l -> l % 10, (a, b) -> a > b ? a : b);
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
    public void testOffer_shouldNotCompactDifferentKeys_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        assertEquals(1L, underTest.poll());
        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_shouldKeepsOrderOfKeys_always() {
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
    public void testOffer_shouldMerge_always() {
        underTest.offer(11L);
        underTest.offer(121L);
        underTest.offer(1L);
        underTest.offer(31L);

        assertEquals(121L, underTest.poll());
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
    public void testClear_shouldRemoveAllElements_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(11L);
        underTest.offer(3L);

        assertEquals(3, underTest.size());
        underTest.clear();
        assertEquals(0, underTest.size());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_shouldUseDefaultMerge_whenNoneIsProvided() {
        underTest = new CompactingQueue<>(l -> l % 10, null);
        underTest.offer(121L);
        underTest.offer(11L);
        underTest.offer(1L);

        assertEquals(1, underTest.poll());
    }

    @Test
    public void testPoll_shouldReturnNull_whenEmpty() {
        assertNull(underTest.poll());
    }
}
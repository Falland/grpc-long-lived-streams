package org.falland.grpc.longlivedstreams.core.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MergeByKeyTest {

    private MergeByKey<Long, Long> underTest;

    @BeforeEach
    public void init() {
        underTest = new MergeByKey<>(l -> l%10, Math::max);
    }

    @Test
    public void testOffer_shouldMergeWithDefault_whenMergeIsNotSpecified() {
        underTest = new MergeByKey<>(l -> l%10);
        underTest.offer(1L);
        underTest.offer(121L);
        underTest.offer(11L);

        assertEquals(11L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_shouldMergeOnKey_always() {
        underTest.offer(1L);
        underTest.offer(121L);
        underTest.offer(11L);

        assertEquals(121L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testOffer_shouldMegeOnDifferentKEys_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(32L);
        underTest.offer(41L);

        assertEquals(41L, underTest.poll());
        assertEquals(32L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testPoll_shouldReturnNull_whenEmpty() {
        assertNull(underTest.poll());
    }

    @Test
    public void testPoll_shouldMergeOnLeftKeys_whenPolled() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        assertEquals(1L, underTest.poll());

        underTest.offer(32L);

        assertEquals(32L, underTest.poll());
        assertEquals(3L, underTest.poll());
    }

    @Test
    public void testStop_shouldClear_whenCalled() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        underTest.stop();

        assertNull(underTest.poll());
    }

}
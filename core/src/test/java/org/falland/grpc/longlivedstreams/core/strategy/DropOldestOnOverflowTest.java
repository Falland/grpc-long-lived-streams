package org.falland.grpc.longlivedstreams.core.strategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DropOldestOnOverflowTest {

    private DropOldestOnOverflow<Long> underTest;

    @BeforeEach
    public void init() {
        underTest = new DropOldestOnOverflow<>(3);
    }

    @Test
    public void testOffer_shouldAddUpToQueueSize_always() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        assertEquals(1L, underTest.poll());
        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
    }

    @Test
    public void testOffer_shouldDiscardOldestMessage_whenFull() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);
        underTest.offer(4L);

        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
        assertEquals(4L, underTest.poll());
        assertNull(underTest.poll());
    }

    @Test
    public void testPull_shouldReturnNull_whenEmpty() {
        assertNull(underTest.poll());
    }

    @Test
    public void testPoll_shouldAllowOffer_whenPolled() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        assertEquals(1L, underTest.poll());

        underTest.offer(4L);

        assertEquals(2L, underTest.poll());
        assertEquals(3L, underTest.poll());
        assertEquals(4L, underTest.poll());
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
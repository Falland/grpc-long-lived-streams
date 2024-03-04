package org.falland.grpc.longlivedstreams.core.strategy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BlockProducerOnOverflowTest {
    private BlockProducerOnOverflow<Long> underTest;

    @BeforeEach
    public void init() {
        underTest = new BlockProducerOnOverflow<>(3);
    }

    @AfterEach
    public void tearDown() {
        underTest.stop();
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
    public void testOffer_shouldBlockProducer_whenFull() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger(0);
        Thread producer = new Thread(() -> {
            underTest.offer(1L);
            counter.incrementAndGet();
            underTest.offer(2L);
            counter.incrementAndGet();
            underTest.offer(3L);
            counter.incrementAndGet();
            latch.countDown();
            underTest.offer(4L);
        });
        producer.start();
        latch.await();
        assertEquals(3, counter.get());

        producer.join(100);
        assertEquals(3, counter.get());
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

    @Test
    public void testStop_shouldNotAdd_afterCalled() {
        underTest.offer(1L);
        underTest.offer(2L);
        underTest.offer(3L);

        underTest.stop();

        assertNull(underTest.poll());

        underTest.offer(1L);

        assertNull(underTest.poll());
    }

}
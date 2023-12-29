package org.falland.grpc.longlivedstreams.core.streams;

import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class TestControlledStreamObserver<U> implements ControlledStreamObserver<U> {
    private final Predicate<U> throwOnNextIf;
    private final Queue<U> onNextMessages = new ConcurrentLinkedQueue<>();
    private final Phaser onNextPhaser = new Phaser(1);
    private final Phaser onErrorPhaser = new Phaser(1);
    private final Phaser isReadyCallPhaser = new Phaser(1);
    private volatile boolean isReady = false;
    private volatile boolean isOpened = false;
    private volatile Throwable errorReceived;

    public TestControlledStreamObserver(Predicate<U> throwOnNextIf) {
        this.throwOnNextIf = throwOnNextIf;
    }

    public TestControlledStreamObserver() {
        this(null);
    }

    public void setReady(boolean ready) {
        isReady = ready;
    }

    public void setOpened(boolean opened) {
        isOpened = opened;
    }

    public int onNextCallCount() {
        return onNextMessages.size();
    }

    public Collection<U> onNextMessages() throws InterruptedException {
        onNextPhaser.awaitAdvanceInterruptibly(onNextPhaser.getPhase());
        return new ArrayList<>(onNextMessages);
    }

    public Collection<U> onNextMessages(long waitForMillis) throws InterruptedException {
        try {
            onNextPhaser.awaitAdvanceInterruptibly(onNextPhaser.getPhase(), waitForMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return new ArrayList<>(onNextMessages);
        }
        return new ArrayList<>(onNextMessages);
    }

    public int awaitIsReadyAndGetPhaseNumber() {
        isReadyCallPhaser.awaitAdvance(isReadyCallPhaser.getPhase());
        return isReadyCallPhaser.getPhase();
    }

    public void awaitOnError() {
        onErrorPhaser.awaitAdvance(onErrorPhaser.getPhase());
    }

    public Throwable getErrorReceived() {
        return errorReceived;
    }

    @Override
    public boolean isReady() {
        isReadyCallPhaser.arrive();
        return isReady;
    }

    @Override
    public boolean isOpened() {
        return isOpened;
    }

    @Override
    public void onNext(U u) {
        onNextMessages.add(u);
        onNextPhaser.arrive();
        if (throwOnNextIf != null && throwOnNextIf.test(u)) {
            throw new RuntimeException("Test. Throw during onNext");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        this.errorReceived = throwable;
        onErrorPhaser.arrive();
    }

    @Override
    public void onCompleted() {
    }

    public void reset() {
        isReady = false;
        isOpened = false;
        onNextMessages.clear();
        errorReceived = null;
    }
}

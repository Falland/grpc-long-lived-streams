package org.falland.grpc.longlivedstreams.examples.apps.utils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;

public interface Client<U> {

    void start();

    Future<?> stop();

    void awaitOnComplete(int count, Duration maxAwaitTime) throws InterruptedException;

    List<U> getMessages();
}

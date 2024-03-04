package org.falland.grpc.longlivedstreams.examples.apps.utils;

import java.time.Duration;
import java.util.List;

public interface Client<U> {

    void start();

    void stop();

    void awaitOnComplete(int count, Duration maxAwaitTime) throws InterruptedException;

    List<U> getMessages();
}

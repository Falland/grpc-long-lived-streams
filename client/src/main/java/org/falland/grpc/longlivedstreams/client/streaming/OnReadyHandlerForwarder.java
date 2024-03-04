package org.falland.grpc.longlivedstreams.client.streaming;

import java.util.function.Consumer;

/**
 * The class that allows late binding of the onReadyHandler injection and actual logic for the handler.
 * This class is injected into the client-side stream observe and will execute the handler logic once it's set with {@link #accept(Runnable)} method
 */
public class OnReadyHandlerForwarder implements Runnable, Consumer<Runnable> {

    private volatile Runnable onReadyHandler;
    @Override
    public void run() {
        if (onReadyHandler != null) {
            onReadyHandler.run();
        }
    }

    @Override
    public void accept(Runnable runnable) {
        this.onReadyHandler = runnable;
    }
}

package org.falland.grpc.longlivedstreams.server.keepalive;

interface IdleAware {

    /**
     * Returns how long the call has been idle for.
     *
     * <p>
     * This method will be invoked at a fixed rate. The implementation counts
     * how many times this method has been invoked since the last message was
     * sent through the call. When this count is multiplied by the polling
     * interval it gives the approximate idle time of the call. If the idle
     * time exceeds the threshold then the call can be pinged to keep it alive.
     *
     * @return the idle count for the call
     */
    int idleCount();

    /**
     * Sends an empty message through the call.
     */
    void ping();
}

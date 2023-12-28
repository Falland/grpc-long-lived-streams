package org.falland.grpc.longlivedstreams.server;

import org.falland.grpc.longlivedstreams.server.address.AddressInterceptor;
import io.grpc.BindableService;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import org.falland.grpc.longlivedstreams.server.streaming.SubscriptionKey;

import java.net.SocketAddress;
import java.time.Duration;

public abstract class AbstractSubscriptionAware implements ServerServiceDefinitionWrapper {

    /**
     * This field represents the amount of time thread will sleep in case the observer is not ready for next update
     * This time should not be too big (nanos or micros rather than seconds), and is required to prevent busy looping
     */
    private final Duration threadCoolDownWhenNotReady;
    private final int queueSize;

    public AbstractSubscriptionAware(int queueSize, Duration threadCoolDownWhenNotReady) {
        assert queueSize > 0;
        this.threadCoolDownWhenNotReady = threadCoolDownWhenNotReady;
        this.queueSize = queueSize;
    }

    public Duration getThreadCoolDownWhenNotReady() {
        return threadCoolDownWhenNotReady;
    }

    public int getQueueSize() {
        return queueSize;
    }

    protected abstract BindableService getGrpcService();

    @Override
    public ServerServiceDefinition getServiceDefinition() {
        return ServerInterceptors.intercept(getGrpcService(), new AddressInterceptor());
    }

    public SubscriptionKey getSubscriptionKey(String clientId) {
        SocketAddress address = AddressInterceptor.ADDRESS_KEY.get();
        return new SubscriptionKey(address == null ? "null" : address.toString(), clientId);
    }

    @SuppressWarnings("unused")
    public void stop() {
    }

}

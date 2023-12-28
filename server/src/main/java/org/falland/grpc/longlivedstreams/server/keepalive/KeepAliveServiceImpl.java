package org.falland.grpc.longlivedstreams.server.keepalive;

import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class KeepAliveServiceImpl implements CallRegistry, KeepAliveService {

    private final int keepAliveIntervalSeconds;
    private final int checkPeriodSeconds;
    private final ScheduledExecutorService executor;
    private final Set<IdleAware> serverCalls;

    private ScheduledFuture<?> checkFuture;

    KeepAliveServiceImpl(int keepAliveIntervalSeconds, int checkPeriodSeconds) {
        this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
        this.checkPeriodSeconds = checkPeriodSeconds;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("application-keep-alive-runner-", true));
        serverCalls = new CopyOnWriteArraySet<>();
    }

    public void start() {
        checkFuture = executor.scheduleAtFixedRate(this::pingIdleCalls, 0, checkPeriodSeconds, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unused")
    public void stop() {
        if (checkFuture != null) {
            checkFuture.cancel(true);
        }
        executor.shutdown();
    }

    @Override
    @SuppressWarnings("squid:S2250")
    public Removable register(IdleAware serverCall) {
        serverCalls.add(serverCall);
        return () -> serverCalls.remove(serverCall);
    }

    @Override
    public ServerServiceDefinition intercept(ServerServiceDefinition serviceDefinition) {
        return ServerInterceptors.intercept(ServerInterceptors.useInputStreamMessages(serviceDefinition),
                new KeepAliveInterceptor(this));
    }

    void pingIdleCalls() {
        serverCalls.forEach(serverCall -> {
            int idleCount = serverCall.idleCount();
            int secondsIdle = idleCount * checkPeriodSeconds;
            if (secondsIdle > keepAliveIntervalSeconds) {
                serverCall.ping();
            }
        });
    }
}

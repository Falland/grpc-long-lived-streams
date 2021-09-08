package io.grpc.longlivedstreams.server.keepalive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
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
        this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("application-keep-alive-runner-%d")
                .setDaemon(true).build());
        serverCalls = new CopyOnWriteArraySet<>();
    }

    public void start() throws Exception {
        checkFuture = executor.scheduleAtFixedRate(this::pingIdleCalls, 0, checkPeriodSeconds, TimeUnit.SECONDS);
    }

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

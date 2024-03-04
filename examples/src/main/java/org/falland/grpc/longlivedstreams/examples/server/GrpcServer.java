package org.falland.grpc.longlivedstreams.examples.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GrpcServer {

    private final Server server;

    public GrpcServer(int port, List<BindableService> services) {
        var builder = ServerBuilder.forPort(port);
        services.forEach(builder::addService);
        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                GrpcServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}

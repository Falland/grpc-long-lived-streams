package org.falland.grpc.longlivedstreams.examples.apps.utils;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import com.google.protobuf.ByteString;
import org.falland.grpc.longlivedstreams.client.ClientConfiguration;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.falland.grpc.longlivedstreams.examples.client.ClientConfigurationImpl;
import org.falland.grpc.longlivedstreams.examples.client.WorldUpdateProcessor;
import org.falland.grpc.longlivedstreams.examples.server.GrpcServer;
import org.falland.grpc.longlivedstreams.examples.util.Payload;
import org.falland.grpc.longlivedstreams.examples.util.PortUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ServerStreamingApp {
    private final Supplier<Service<Hello, World>> serviceFactory;
    private final ClientFactory<World> clientFactory;

    private final int messagesToSend;

    public ServerStreamingApp(int messagesToSend, Supplier<Service<Hello, World>> serviceFactory, ClientFactory<World> clientFactory) {
        this.messagesToSend = messagesToSend;
        this.serviceFactory = serviceFactory;
        this.clientFactory = clientFactory;
    }

    public void run() throws IOException, InterruptedException {
        ThreadFactory factory = new ThreadFactoryImpl("test-client-worker-",false);
        ExecutorService clientExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        int port = PortUtils.INSTANCE.findFreePort();
        Service<Hello, World> service = serviceFactory.get();
        GrpcServer server = new GrpcServer(port, List.of(service.getBindableService()));
        server.start();

        WorldUpdateProcessor updateProcessor = new WorldUpdateProcessor();
        ClientConfiguration configuration = new ClientConfigurationImpl("localhost", "testClient", port, clientExecutor);
        Client<World> client = clientFactory.createClient(configuration, updateProcessor);
        client.start();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            service.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(Payload.INSTANCE.getPayload()))
                    .build());
            Thread.sleep(10);
        }

        while (client.getMessages().isEmpty()) {
//            noinspection BusyWait
            Thread.sleep(1);
        }
        System.out.println("Sent initial batch");
        System.out.println("Received so far : " + client.getMessages().size());
        long start = System.nanoTime();
        for (int i = 0; i < messagesToSend - 10; i++) {
            try {
                service.publishMessage(World.newBuilder()
                        .setPayload(ByteString.copyFrom(Payload.INSTANCE.getPayload()))
                        .build());
                TimeUnit.MICROSECONDS.sleep(100);
            } catch (Exception e) {
                System.out.println("Exception received " + e.getMessage());
            }
        }
        long elapsed = System.nanoTime() - start;
        client.awaitOnComplete(messagesToSend, Duration.ofSeconds(20));
        System.out.println("Received messages total : " + client.getMessages().size() + " out of " + messagesToSend);
        System.out.println("Received requests total : " + service.getMessages().size());
        System.out.println("Time to send messages : " + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
        System.out.println("Stopping app");
        client.stop();
        server.stop();
        clientExecutor.shutdown();
    }
}

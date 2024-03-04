package org.falland.grpc.longlivedstreams.examples.apps.utils;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import org.falland.grpc.longlivedstreams.client.ClientConfiguration;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.falland.grpc.longlivedstreams.examples.client.ClientConfigurationImpl;
import org.falland.grpc.longlivedstreams.examples.client.ClientStreamingClient;
import org.falland.grpc.longlivedstreams.examples.client.WorldUpdateProcessor;
import org.falland.grpc.longlivedstreams.examples.server.GrpcServer;
import org.falland.grpc.longlivedstreams.examples.util.PortUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

public class ClientStreamingApp {
    private final Supplier<Service<Hello, World>> serviceFactory;
    private final int messagesToSend;

    public ClientStreamingApp(int messagesToSend, Supplier<Service<Hello, World>> serviceFactory) {
        this.messagesToSend = messagesToSend;
        this.serviceFactory = serviceFactory;
    }

    public void run() throws IOException, InterruptedException {
        ThreadFactory factory = new ThreadFactoryImpl("test-client-worker-",false);
        ExecutorService clientExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        int port = PortUtils.INSTANCE.findFreePort();
        Service<Hello, World> service = serviceFactory.get();
        GrpcServer server = new GrpcServer(port, List.of(service.getBindableService()));
        server.start();

        WorldUpdateProcessor updateProcessor = new WorldUpdateProcessor();
        ClientConfiguration clientContext = new ClientConfigurationImpl("localhost", "testClient", port, clientExecutor);
        ClientStreamingClient client = new ClientStreamingClient(clientContext, updateProcessor, Duration.ofNanos(10));
        client.start();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            client.publishMessage(Hello.newBuilder().build());
            Thread.sleep(10);
        }

        while (service.getMessages().isEmpty()) {
//            noinspection BusyWait
            Thread.sleep(1);
        }
        System.out.println("Sent initial batch");
        System.out.println("Received so far : " + service.getMessages().size());

        for (int i = 0; i < messagesToSend - 10; i++) {
            try {
                client.publishMessage(Hello.newBuilder().build());
            } catch (Exception e) {
               System.out.println("Exception received " + e.getMessage());
            }
        }
        Thread.sleep(10_000);
        updateProcessor.awaitOnComplete(messagesToSend, Duration.ofSeconds(60));
        System.out.println("Received messages total : " + service.getMessages().size() + " out of " + messagesToSend);
        System.out.println("Received requests total : " + updateProcessor.getMessages().size());
        System.out.println("Stopping app");
        client.stop();
        server.stop();
        clientExecutor.shutdown();
    }
}

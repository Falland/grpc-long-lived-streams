package org.falland.grpc.longlivedstreams.examples.apps;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import com.google.protobuf.ByteString;
import org.falland.grpc.longlivedstreams.client.ClientContext;
import org.falland.grpc.longlivedstreams.core.util.ThreadFactoryImpl;
import org.falland.grpc.longlivedstreams.examples.GrpcServer;
import org.falland.grpc.longlivedstreams.examples.StreamingClient;
import org.falland.grpc.longlivedstreams.examples.StreamingClientContext;
import org.falland.grpc.longlivedstreams.examples.StreamingService;
import org.falland.grpc.longlivedstreams.examples.WorldUpdateProcessor;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.falland.grpc.longlivedstreams.examples.apps.Payload.payload;
import static org.falland.grpc.longlivedstreams.examples.apps.PortUtils.findFreePort;

public class FullFlowServerStreamingClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        ThreadFactory factory = new ThreadFactoryImpl("full-flow-test-client-worker-",false);
        ExecutorService clientExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        int port = findFreePort();
        StreamingService streamingService = new StreamingService(10_000, Duration.ofMillis(1));
        GrpcServer server = new GrpcServer(port, List.of(streamingService.getGrpcService()));
        server.start();

        WorldUpdateProcessor updateProcessor = new WorldUpdateProcessor();
        ClientContext clientContext = new StreamingClientContext("localhost", "testClient", port, clientExecutor);
        StreamingClient client = new StreamingClient(clientContext, updateProcessor, Duration.ofNanos(10), false);
        client.start();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
            Thread.sleep(10);
        }

        while (updateProcessor.getMessages().isEmpty()) {
            //noinspection BusyWait
            Thread.sleep(1);
        }
        System.out.println("Received initial batch");

        for (int i = 0; i < 100_000; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
        }
        updateProcessor.awaitOnComplete(Duration.ofSeconds(60));
        System.out.println("Received messages total : " + updateProcessor.getMessages().size());
        server.stop();
        client.stop();
        clientExecutor.shutdown();
    }
}

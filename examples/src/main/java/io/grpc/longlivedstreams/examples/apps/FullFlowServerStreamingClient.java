package io.grpc.longlivedstreams.examples.apps;

import io.grpc.longlivedstreams.client.ClientContext;
import com.google.protobuf.ByteString;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.longlivedstreams.examples.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static io.grpc.longlivedstreams.examples.apps.Payload.payload;
import static io.grpc.longlivedstreams.examples.apps.PortUtils.findFreePort;

public class FullFlowServerStreamingClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = findFreePort();
        StreamingService streamingService = new StreamingService(10_000, Duration.ofMillis(1));
        GrpcServer server = new GrpcServer(port, List.of(streamingService.getGrpcService()));
        server.start();

        WorldUpdateProcessor updateProcessor = new WorldUpdateProcessor();
        ClientContext clientContext = new StreamingClientContext("localhost", "testClient", port);
        StreamingClient client = new StreamingClient(clientContext, List.of(updateProcessor), Duration.ofNanos(10), false);
        client.start();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
            Thread.sleep(10);
        }

        while (updateProcessor.getMessages().isEmpty()) {
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
    }
}

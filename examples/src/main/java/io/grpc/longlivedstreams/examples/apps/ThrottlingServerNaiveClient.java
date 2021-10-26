package io.grpc.longlivedstreams.examples.apps;

import com.google.protobuf.ByteString;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.longlivedstreams.examples.GrpcServer;
import io.grpc.longlivedstreams.examples.NaiveClient;
import io.grpc.longlivedstreams.examples.StreamingService;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static io.grpc.longlivedstreams.examples.apps.Payload.payload;
import static io.grpc.longlivedstreams.examples.apps.PortUtils.findFreePort;

public class ThrottlingServerNaiveClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = findFreePort();
        StreamingService streamingService = new StreamingService(10_000, Duration.ofMillis(1));
        GrpcServer server = new GrpcServer(port, List.of(streamingService.getGrpcService()));
        server.start();

        NaiveClient client = new NaiveClient(port);
        client.subscribe();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
            Thread.sleep(10);
        }

        while (client.getMessages().isEmpty()) {
            Thread.sleep(1);
        }
        System.out.println("Received initial batch");

        for (int i = 0; i < 100_000; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setGroup(i % 10)
                    .setPayload(ByteString.copyFrom(payload)).build());
        }
        client.awaitOnComplete(Duration.ofSeconds(60));
        System.out.println("Received messages total : " + client.getMessages().size());
        server.stop();
    }
}

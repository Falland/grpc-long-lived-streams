package com.falland.grpc.longlivedstreams.examples.apps;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import com.falland.grpc.longlivedstreams.examples.GrpcServer;
import com.falland.grpc.longlivedstreams.examples.NaiveClient;
import com.falland.grpc.longlivedstreams.examples.StreamingService;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class ThrottlingServerNaiveClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = PortUtils.findFreePort();
        StreamingService streamingService = new StreamingService(10_000, Duration.ofMillis(1));
        GrpcServer server = new GrpcServer(port, List.of(streamingService.getGrpcService()));
        server.start();

        NaiveClient client = new NaiveClient(port);
        client.subscribe();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(Payload.payload)).build());
            Thread.sleep(10);
        }

        while (client.getMessages().isEmpty()) {
            Thread.sleep(1);
        }
        System.out.println("Received initial batch");

        for (int i = 0; i < 100_000; i++) {
            streamingService.publishMessage(World.newBuilder()
                    .setGroup(i % 10)
                    .setPayload(ByteString.copyFrom(Payload.payload)).build());
        }
        client.awaitOnComplete(Duration.ofSeconds(60));
        System.out.println("Received messages total : " + client.getMessages().size());
        server.stop();
    }
}

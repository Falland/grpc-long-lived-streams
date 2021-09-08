package io.grpc.longlivedstreams.examples.apps;

import com.google.protobuf.ByteString;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.longlivedstreams.examples.GrpcServer;
import io.grpc.longlivedstreams.examples.NaiveClient;
import io.grpc.longlivedstreams.examples.NaiveService;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static io.grpc.longlivedstreams.examples.apps.Payload.payload;
import static io.grpc.longlivedstreams.examples.apps.PortUtils.findFreePort;

public class NaiveServerNaiveClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = findFreePort();
        NaiveService naiveService = new NaiveService();
        GrpcServer server = new GrpcServer(port, List.of(naiveService.getGrpcService()));
        server.start();

        NaiveClient client = new NaiveClient(port);
        client.subscribe(false);
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            naiveService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
            Thread.sleep(10);
        }

        while (client.getMessages().isEmpty()) {
            Thread.sleep(1);
        }
        System.out.println("Received initial batch");

        for (int i = 0; i < 100_000; i++) {
            naiveService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(payload)).build());
        }
        client.awaitOnComplete(Duration.ofSeconds(60));
        System.out.println("Received messages total : " + client.getMessages().size());
        server.stop();
    }
}

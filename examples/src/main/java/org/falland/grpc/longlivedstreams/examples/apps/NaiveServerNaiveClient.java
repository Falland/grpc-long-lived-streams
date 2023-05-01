package org.falland.grpc.longlivedstreams.examples.apps;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import com.falland.gprc.longlivedstreams.proto.pricing.v1.PriceUpdate;
import org.falland.grpc.longlivedstreams.examples.GrpcServer;
import org.falland.grpc.longlivedstreams.examples.NaiveClient;
import org.falland.grpc.longlivedstreams.examples.NaivePricingService;
import org.falland.grpc.longlivedstreams.examples.NaiveService;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class NaiveServerNaiveClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = PortUtils.findFreePort();
        NaiveService naiveService = new NaiveService();
        NaivePricingService naivePricingService = new NaivePricingService();
        GrpcServer server = new GrpcServer(port, List.of(naiveService.getGrpcService()));
        server.start();

        NaiveClient client = new NaiveClient(port);
        client.subscribe();
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            naiveService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(Payload.payload)).build());
            Thread.sleep(10);
        }

        while (client.getMessages().isEmpty()) {
            Thread.sleep(1);
        }
        System.out.println("Received initial batch");

        for (int i = 0; i < 100_000; i++) {
            naiveService.publishMessage(World.newBuilder()
                    .setPayload(ByteString.copyFrom(Payload.payload)).build());
            naivePricingService.publishMessage(PriceUpdate.newBuilder().build());
        }
        client.awaitOnComplete(Duration.ofSeconds(60));
        System.out.println("Received messages total : " + client.getMessages().size());
        server.stop();
    }
}

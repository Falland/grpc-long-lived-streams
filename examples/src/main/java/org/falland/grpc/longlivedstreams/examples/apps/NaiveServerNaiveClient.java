package org.falland.grpc.longlivedstreams.examples.apps;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import org.falland.grpc.longlivedstreams.examples.apps.utils.ClientFactory;
import org.falland.grpc.longlivedstreams.examples.apps.utils.ServerStreamingApp;
import org.falland.grpc.longlivedstreams.examples.client.NaiveClient;
import org.falland.grpc.longlivedstreams.examples.service.NaiveService;
import org.falland.grpc.longlivedstreams.examples.service.StreamingService;

import java.io.IOException;

public class NaiveServerNaiveClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        ClientFactory<World> clientFactory = (configuration, updateProcessor) -> new NaiveClient(configuration.port(), updateProcessor);

        ServerStreamingApp app = new ServerStreamingApp(100_000, NaiveService::new, clientFactory);
        app.run();
//        int port = PortUtils.INSTANCE.findFreePort();
//        NaiveService naiveService = new NaiveService();
//        GrpcServer server = new GrpcServer(port, List.of(naiveService.getGrpcService()));
//        server.start();
//
//        NaiveClient client = new NaiveClient(port);
//        client.subscribe();
//        Thread.sleep(1000);
//        for (int i = 0; i < 10; i++) {
//            naiveService.publishMessage(World.newBuilder()
//                    .setPayload(ByteString.copyFrom(Payload.INSTANCE.getPayload()))
//                    .build());
//            Thread.sleep(10);
//        }
//
//        while (client.getMessages().isEmpty()) {
//            Thread.sleep(1);
//        }
//        System.out.println("Received initial batch");
//
//        for (int i = 0; i < 100_000; i++) {
//            naiveService.publishMessage(World.newBuilder()
//                    .setPayload(ByteString.copyFrom(Payload.INSTANCE.getPayload()))
//                    .build());
//        }
//        client.awaitOnComplete(Duration.ofSeconds(60));
//        System.out.println("Received messages total : " + client.getMessages().size());
//        server.stop();
    }
}

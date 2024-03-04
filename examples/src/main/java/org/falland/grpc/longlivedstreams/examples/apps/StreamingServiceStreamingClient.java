package org.falland.grpc.longlivedstreams.examples.apps;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.ResponseStrategy;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import org.falland.grpc.longlivedstreams.examples.apps.utils.ClientFactory;
import org.falland.grpc.longlivedstreams.examples.apps.utils.ServerStreamingApp;
import org.falland.grpc.longlivedstreams.examples.client.ServerStreamingClient;
import org.falland.grpc.longlivedstreams.examples.service.StreamingService;

import java.io.IOException;
import java.time.Duration;


public class StreamingServiceStreamingClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        ResponseStrategy strategy = ResponseStrategy.BLOCK;
        Duration retrySubscriptionDuration = Duration.ofMillis(1);
        ClientFactory<World> clientFactory = (configuration, updateProcessor) -> new ServerStreamingClient(configuration, updateProcessor, retrySubscriptionDuration, strategy);
        ServerStreamingApp app = new ServerStreamingApp(100_000, StreamingService::new, clientFactory);

        app.run();
    }
}

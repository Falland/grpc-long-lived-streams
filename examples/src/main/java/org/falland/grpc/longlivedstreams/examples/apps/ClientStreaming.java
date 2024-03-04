package org.falland.grpc.longlivedstreams.examples.apps;

import org.falland.grpc.longlivedstreams.examples.apps.utils.ClientStreamingApp;
import org.falland.grpc.longlivedstreams.examples.service.StreamingService;

import java.io.IOException;

public class ClientStreaming {

    public static void main(String[] args) throws IOException, InterruptedException {
        ClientStreamingApp clientStreamingApp = new ClientStreamingApp(100_000, StreamingService::new);
        clientStreamingApp.run();
    }
}

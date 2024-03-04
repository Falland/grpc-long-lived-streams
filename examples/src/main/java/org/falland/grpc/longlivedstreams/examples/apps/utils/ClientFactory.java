package org.falland.grpc.longlivedstreams.examples.apps.utils;

import org.falland.grpc.longlivedstreams.client.ClientConfiguration;
import org.falland.grpc.longlivedstreams.examples.client.WorldUpdateProcessor;

public interface ClientFactory<U> {

    Client<U> createClient(ClientConfiguration configuration, WorldUpdateProcessor updateProcessor);
}

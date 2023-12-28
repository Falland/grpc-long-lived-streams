package org.falland.grpc.longlivedstreams.server.streaming;

import org.falland.grpc.longlivedstreams.core.streams.StreamType;

public record SubscriptionDescriptor(String addressString, String clientId, StreamType type) {
}

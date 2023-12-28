package org.falland.grpc.longlivedstreams.server.streaming;

import org.falland.grpc.longlivedstreams.core.subscription.SubscriptionType;

public record SubscriptionDescriptor(String addressString, String clientId, SubscriptionType type) {
}

package io.grpc.longlivedstreams.server.subscription;

public class SubscriptionDescriptor {

    private final String clientAddress;
    private final String clientId;
    private final SubscriptionType subscriptionType;

    public SubscriptionDescriptor(String clientAddress, String clientId,
                                  SubscriptionType subscriptionType) {
        this.clientAddress = clientAddress;
        this.clientId = clientId;
        this.subscriptionType = subscriptionType;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public String getClientId() {
        return clientId;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    @Override
    public String toString() {
        return "SubscriptionDescriptor{" +
               "clientAddress='" + clientAddress + '\'' +
               ", clientId='" + clientId + '\'' +
               ", subscriptionType=" + subscriptionType +
               '}';
    }
}

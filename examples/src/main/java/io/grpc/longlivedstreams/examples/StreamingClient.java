package io.grpc.longlivedstreams.examples;

import io.grpc.longlivedstreams.client.AbstractGrpcSubscriptionClient;
import io.grpc.longlivedstreams.client.ClientContext;
import io.grpc.longlivedstreams.client.UpdateProcessor;
import io.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import io.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;

import java.time.Duration;
import java.util.List;

public class StreamingClient extends AbstractGrpcSubscriptionClient<World> {
    private final boolean isThrottling;

    public StreamingClient(ClientContext clientContext, List<UpdateProcessor<World>> updateProcessors,
                              Duration retrySubscriptionDuration, boolean isThrottling) {
        super(clientContext, updateProcessors, retrySubscriptionDuration);
        this.isThrottling = isThrottling;
    }

    @Override
    protected void subscribe(Channel channel) {
        Hello request = Hello.newBuilder().setClientId(getClientContext().getClientName())
                .setIsThrottling(isThrottling).build();
        HelloWorldGrpc.newStub(channel).sayServerStreaming(request, this.createResponseObserver(channel));
    }
}

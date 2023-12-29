package org.falland.grpc.longlivedstreams.examples;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import org.falland.grpc.longlivedstreams.client.AbstractGrpcSubscriptionClient;
import org.falland.grpc.longlivedstreams.client.ClientContext;
import org.falland.grpc.longlivedstreams.client.UpdateProcessor;

import java.time.Duration;

public class StreamingClient extends AbstractGrpcSubscriptionClient<World> {
    private final boolean isThrottling;

    public StreamingClient(ClientContext clientContext, UpdateProcessor<World> updateProcessor,
                              Duration retrySubscriptionDuration, boolean isThrottling) {
        super(clientContext, updateProcessor, retrySubscriptionDuration);
        this.isThrottling = isThrottling;
    }

    @Override
    protected void subscribe(Channel channel) {
        Hello request = Hello.newBuilder().setClientId(getClientContext().clientName())
                .setIsThrottling(isThrottling).build();
        HelloWorldGrpc.newStub(channel).sayServerStreaming(request, this.createResponseObserver(channel));
    }
}

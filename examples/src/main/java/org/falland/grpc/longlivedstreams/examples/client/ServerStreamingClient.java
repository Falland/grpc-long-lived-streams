package org.falland.grpc.longlivedstreams.examples.client;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.ResponseStrategy;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import org.falland.grpc.longlivedstreams.client.AbstractGrpcSubscriptionClient;
import org.falland.grpc.longlivedstreams.client.ClientConfiguration;
import org.falland.grpc.longlivedstreams.examples.apps.utils.Client;

import java.time.Duration;
import java.util.List;

public class ServerStreamingClient extends AbstractGrpcSubscriptionClient<World> implements Client<World> {
    private final ResponseStrategy responseStrategy;
    private final WorldUpdateProcessor updateProcessor;

    public ServerStreamingClient(ClientConfiguration configuration, WorldUpdateProcessor updateProcessor,
                                 Duration retrySubscriptionDuration, ResponseStrategy responseStrategy) {
        super(configuration, updateProcessor, retrySubscriptionDuration);
        this.responseStrategy = responseStrategy;
        this.updateProcessor = updateProcessor;
    }

    @Override
    protected void subscribe(Channel channel) {
        Hello request = Hello.newBuilder().setClientId(getClientConfiguration().clientName())
                .setResponseStrategy(responseStrategy).build();
        HelloWorldGrpc.newStub(channel).sayServerStreaming(request, this.simpleObserver());
    }

    @Override
    public void awaitOnComplete(int count, Duration maxAwaitTime) throws InterruptedException {
        updateProcessor.awaitOnComplete(count, maxAwaitTime);
    }

    @Override
    public List<World> getMessages() {
        return updateProcessor.getMessages();
    }
}

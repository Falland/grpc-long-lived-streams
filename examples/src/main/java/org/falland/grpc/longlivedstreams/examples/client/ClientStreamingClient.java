package org.falland.grpc.longlivedstreams.examples.client;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import io.grpc.stub.CallStreamObserver;
import org.falland.grpc.longlivedstreams.client.AbstractGrpcSubscriptionClient;
import org.falland.grpc.longlivedstreams.client.ClientConfiguration;
import org.falland.grpc.longlivedstreams.client.UpdateProcessor;
import org.falland.grpc.longlivedstreams.client.streaming.OnReadyHandlerForwarder;
import org.falland.grpc.longlivedstreams.client.streaming.OnReadyHandlerForwardingStreamObserver;
import org.falland.grpc.longlivedstreams.core.strategy.ExceptionOnOverflow;
import org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver;

import java.time.Duration;

public class ClientStreamingClient extends AbstractGrpcSubscriptionClient<World> {
    private volatile BackpressingStreamObserver<Hello> stream;

    public ClientStreamingClient(ClientConfiguration clientContext, UpdateProcessor<World> updateProcessor,
                                 Duration retrySubscriptionDuration) {
        super(clientContext, updateProcessor, retrySubscriptionDuration);
    }

    @Override
    protected void subscribe(Channel channel) {
        OnReadyHandlerForwarder forwarder = new OnReadyHandlerForwarder();
        var clientStream = HelloWorldGrpc.newStub(channel).sayClientStreaming(this.clientStreamingCallObserver(forwarder));
        stream = BackpressingStreamObserver.<Hello>builder()
                .withObserver(new OnReadyHandlerForwardingStreamObserver<>((CallStreamObserver<Hello>) clientStream, forwarder))
                .withStrategy(new ExceptionOnOverflow<>(10))
                .build();

    }

    public void publishMessage(Hello message) {
        stream.onNext(message);
    }
}

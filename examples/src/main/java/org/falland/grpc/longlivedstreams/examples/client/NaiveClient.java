package org.falland.grpc.longlivedstreams.examples.client;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.examples.apps.utils.Client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class NaiveClient implements Client<World> {

    private final HelloWorldGrpc.HelloWorldStub client;
    private final WorldUpdateProcessor updateProcessor;
    private final AtomicBoolean hadOnError = new AtomicBoolean(false);
    private final AtomicBoolean hadOnComplete = new AtomicBoolean(false);

    public NaiveClient(int port, WorldUpdateProcessor updateProcessor) {
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + port)
                .usePlaintext().build();
        this.client = HelloWorldGrpc.newStub(channel);
        this.updateProcessor = updateProcessor;
    }



    public void subscribe() {
        client.sayServerStreaming(Hello.newBuilder().build(), new StreamObserver<>() {
            @Override
            public void onNext(World world) {
                processMessage(world);
            }

            @Override
            public void onError(Throwable throwable) {
                hadOnError.set(true);
            }

            @Override
            public void onCompleted() {
                hadOnComplete.set(true);
            }
        });
    }

    private void processMessage(World world) {
        updateProcessor.processUpdate(world);
    }

    @Override
    public void start() {
        subscribe();
    }

    @Override
    public Future<?> stop() {
        return null;
    }

    public void awaitOnComplete(int count, Duration maxAwaitTime) throws InterruptedException {
        updateProcessor.awaitOnComplete(count, maxAwaitTime);
    }

    public List<World> getMessages() {
        return updateProcessor.getMessages();
    }
}

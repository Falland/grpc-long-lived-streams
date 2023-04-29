package com.falland.grpc.longlivedstreams.examples;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NaiveClient {

    private static final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private final HelloWorldGrpc.HelloWorldStub client;
    private final List<World> messages = new CopyOnWriteArrayList<>();
    private final AtomicBoolean hadOnError = new AtomicBoolean(false);
    private final AtomicBoolean hadOnComplete = new AtomicBoolean(false);

    public NaiveClient(int port) {
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + port)
                .usePlaintext().build();
        client = HelloWorldGrpc.newStub(channel);
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
        messages.add(world);
    }

    public void awaitOnComplete(Duration maxAwaitTime) throws InterruptedException {
        int previousSize = 0;
        Instant endTime = Instant.now().plusNanos(maxAwaitTime.toNanos());
        while (messages.size() > previousSize && Instant.now().isBefore(endTime)) {
            previousSize = messages.size();
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    public List<World> getMessages() {
        return List.copyOf(messages);
    }
}

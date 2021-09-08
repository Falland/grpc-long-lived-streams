package io.grpc.longlivedstreams.examples;

import io.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import io.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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

    public void subscribe(boolean isThrottling) {
        client.sayServerStreaming(Hello.newBuilder().setIsThrottling(isThrottling).build(), new StreamObserver<World>() {
            @Override
            public void onNext(World world) {
                messages.add(world);
                try {
                    TimeUnit.MICROSECONDS.sleep(rnd.nextInt(50) * 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("On error received");
                System.err.println(throwable);
                hadOnError.set(true);
            }

            @Override
            public void onCompleted() {
                System.out.println("On complete received");
                hadOnComplete.set(true);
            }
        });
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

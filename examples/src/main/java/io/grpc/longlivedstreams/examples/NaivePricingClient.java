package io.grpc.longlivedstreams.examples;

import io.gprc.longlivedstreams.proto.pricing.v1.PriceSubscriptionRequest;
import io.gprc.longlivedstreams.proto.pricing.v1.PriceUpdate;
import io.gprc.longlivedstreams.proto.pricing.v1.PricingServiceGrpc;
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

public class NaivePricingClient {

    private static final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private final PricingServiceGrpc.PricingServiceStub client;
    private final List<PriceUpdate> messages = new CopyOnWriteArrayList<>();
    private final AtomicBoolean hadOnError = new AtomicBoolean(false);
    private final AtomicBoolean hadOnComplete = new AtomicBoolean(false);

    public NaivePricingClient(int port) {
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + port)
                .usePlaintext().build();
        client = PricingServiceGrpc.newStub(channel);
        subscribe();
    }

    public void subscribe() {
        client.subscribeForPriceUpdate(PriceSubscriptionRequest.newBuilder().build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(PriceUpdate update) {
                        processMessage(update);
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

    private void processMessage(PriceUpdate world) {
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

    public List<PriceUpdate> getMessages() {
        return List.copyOf(messages);
    }
}

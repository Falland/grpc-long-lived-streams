package org.falland.grpc.longlivedstreams.examples.service;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.BindableService;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.core.ControlledStreamObserver;
import org.falland.grpc.longlivedstreams.core.FlowControlledObserver;
import org.falland.grpc.longlivedstreams.core.strategy.*;
import org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver;
import org.falland.grpc.longlivedstreams.examples.apps.utils.Service;
import org.falland.grpc.longlivedstreams.server.streaming.Streamer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class StreamingService implements Service<Hello, World> {

    private final Streamer<World> streamer;
    private final int queueSize;
    private final List<Hello> messages = new CopyOnWriteArrayList<>();

    private final HelloWorldGrpc.HelloWorldImplBase delegate = new HelloWorldGrpc.HelloWorldImplBase() {
        @Override
        public void sayServerStreaming(Hello request, StreamObserver<World> responseObserver) {

            FlowControlledObserver<World> controlledStream = new FlowControlledObserver<>((CallStreamObserver<World>) responseObserver);
            ControlledStreamObserver<World> observerToRegister =  switch (request.getResponseStrategy()) {
                case FREE_FLOW -> streamer.freeFlowStreamObserver(controlledStream);
                case MERGE -> BackpressingStreamObserver.<World>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new MergeByKey<>(World::getGroup))
                        .build();
                case DROP_OLD_ON_OVERFLOW -> BackpressingStreamObserver.<World>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new DropOldestOnOverflow<>(queueSize))
                        .build();
                case DROP_NEW_ON_OVERFLOW -> BackpressingStreamObserver.<World>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new DiscardNewOnOverflow<>(queueSize))
                        .build();
                case BLOCK -> BackpressingStreamObserver.<World>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new BlockProducerOnOverflow<>(queueSize))
                        .build();
                default -> BackpressingStreamObserver.<World>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new ExceptionOnOverflow<>(queueSize))
                        .build();
            };
            streamer.register(observerToRegister);
        }

        @Override
        public StreamObserver<Hello> sayClientStreaming(StreamObserver<World> responseObserver) {
            responseObserver.onNext(World.newBuilder().build());
            return new StreamObserver<>() {
                @Override
                public void onNext(Hello value) {
                    messages.add(value);
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("Stream closed with error " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    System.out.println("Stream completed");
                }
            };
        }
    };

    public List<Hello> getMessages() {
        return List.copyOf(messages);
    }

    public StreamingService(int queueSize) {
        this.queueSize = queueSize;
        this.streamer = new Streamer<>(100, Duration.ofMillis(10), "testWorldService");
    }

    public StreamingService() {
        this(10_000);
    }

    public void publishMessage(World message) {
        streamer.sendMessage(message);
    }

    public void completeStream() {
        streamer.complete();
    }

    @Override
    public BindableService getBindableService() {
        return delegate;
    }
}

package io.grpc.longlivedstreams.examples;

import io.grpc.longlivedstreams.server.AbstractSubscriptionAware;
import io.grpc.longlivedstreams.server.streaming.Streamer;
import io.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import io.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import io.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

import java.time.Duration;

public class StreamingService extends AbstractSubscriptionAware {

    private final Streamer<World> streamer;

    private final HelloWorldGrpc.HelloWorldImplBase delegate = new HelloWorldGrpc.HelloWorldImplBase() {
        @Override
        public void sayServerStreaming(Hello request, StreamObserver<World> responseObserver) {
            System.out.println("New client has connected");
            if (request.getIsThrottling()) {
                streamer.subscribeThrottling(request.getClientId(), responseObserver, World::getGroup);
            } else {
                streamer.subscribeFullFlow(request.getClientId(), responseObserver);
            }
        }
    };

    public StreamingService(int queueSize, Duration threadCoolDownWhenNotReady) {
        super(queueSize, threadCoolDownWhenNotReady);
        this.streamer = new Streamer<>("testWorldService", getQueueSize(), getThreadCoolDownWhenNotReady());
    }

    public void publishMessage(World message) {
       streamer.submitResponse(message);
    }

    public void completeStream() {
        streamer.completeStreamer();
    }

    @Override
    public BindableService getGrpcService() {
        return delegate;
    }
}

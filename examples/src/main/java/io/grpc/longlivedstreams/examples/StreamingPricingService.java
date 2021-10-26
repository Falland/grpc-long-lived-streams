package io.grpc.longlivedstreams.examples;

import io.gprc.longlivedstreams.proto.pricing.v1.PriceSubscriptionRequest;
import io.gprc.longlivedstreams.proto.pricing.v1.PriceUpdate;
import io.gprc.longlivedstreams.proto.pricing.v1.PricingServiceGrpc;
import io.grpc.BindableService;
import io.grpc.longlivedstreams.server.AbstractSubscriptionAware;
import io.grpc.longlivedstreams.server.streaming.Streamer;
import io.grpc.stub.StreamObserver;

import java.time.Duration;

public class StreamingPricingService extends AbstractSubscriptionAware {

    private final Streamer<PriceUpdate> streamer;

    private final PricingServiceGrpc.PricingServiceImplBase delegate = new PricingServiceGrpc.PricingServiceImplBase() {
        @Override
        public void subscribeForPriceUpdate(PriceSubscriptionRequest request,
                                            StreamObserver<PriceUpdate> responseObserver) {
            switch (request.getSubscriptionType()) {
                case THROTTLING:
                    streamer.subscribeThrottling(request.getClientId(), responseObserver,
                            PriceUpdate::getInstrumentId);
                    return;
                case PERSISTENT:
                    streamer.subscribePersistent(request.getClientId(), responseObserver);
                    return;
                default:  //By default, we serve clients as FullFlow
                    streamer.subscribeFullFlow(request.getClientId(), responseObserver);
            }
        }
    };

    public void publishMessage(PriceUpdate message) {
        streamer.submitResponse(message);
    }

    public StreamingPricingService(int queueSize, Duration threadCoolDownWhenNotReady) {
        super(queueSize, threadCoolDownWhenNotReady);
        this.streamer = new Streamer<>("testWorldService", getQueueSize(), getThreadCoolDownWhenNotReady());
        publishMessage(null);
    }

    public void completeStream() {
        streamer.completeStreamer();
    }

    @Override
    public BindableService getGrpcService() {
        return delegate;
    }
}

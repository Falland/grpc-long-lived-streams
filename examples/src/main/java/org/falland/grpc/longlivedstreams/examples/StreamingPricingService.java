package org.falland.grpc.longlivedstreams.examples;

import com.falland.gprc.longlivedstreams.proto.pricing.v1.PriceSubscriptionRequest;
import com.falland.gprc.longlivedstreams.proto.pricing.v1.PriceUpdate;
import com.falland.gprc.longlivedstreams.proto.pricing.v1.PricingServiceGrpc;
import org.falland.grpc.longlivedstreams.server.AbstractSubscriptionAware;
import org.falland.grpc.longlivedstreams.server.streaming.Streamer;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

import java.time.Duration;

@SuppressWarnings("unused")
public class StreamingPricingService extends AbstractSubscriptionAware {

    private final Streamer<PriceUpdate> streamer;

    private final PricingServiceGrpc.PricingServiceImplBase delegate = new PricingServiceGrpc.PricingServiceImplBase() {
        @Override
        public void subscribeForPriceUpdate(PriceSubscriptionRequest request,
                                            StreamObserver<PriceUpdate> responseObserver) {
            switch (request.getSubscriptionType()) {
                case THROTTLING:
                    streamer.subscribeThrottling(getSubscriptionKey(request.getClientId()), responseObserver,
                            PriceUpdate::getInstrumentId);
                    return;
                case PERSISTENT:
                    streamer.subscribePersistent(request.getClientId(), responseObserver);
                    return;
                default:  //By default, we serve clients as FullFlow
                    streamer.subscribeFullFlow(getSubscriptionKey(request.getClientId()), responseObserver);
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

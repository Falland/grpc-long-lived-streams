package org.falland.grpc.longlivedstreams.examples;

import com.falland.gprc.longlivedstreams.proto.pricing.v1.PriceSubscriptionRequest;
import com.falland.gprc.longlivedstreams.proto.pricing.v1.PriceUpdate;
import com.falland.gprc.longlivedstreams.proto.pricing.v1.PricingServiceGrpc;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class NaivePricingService {

    private final Collection<StreamObserver<PriceUpdate>> observers = ConcurrentHashMap.newKeySet();

    private final PricingServiceGrpc.PricingServiceImplBase delegate =
            new PricingServiceGrpc.PricingServiceImplBase() {
                @Override
                public void subscribeForPriceUpdate(PriceSubscriptionRequest request,
                                                    StreamObserver<PriceUpdate> responseObserver) {
                    observers.add(responseObserver);
                }
            };

    public void publishMessage(PriceUpdate update) {
        for (StreamObserver<PriceUpdate> observer : observers) {
            try {
                observer.onNext(update);
            } catch (Throwable e) {
                observers.remove(observer);
                observer.onError(Status.INTERNAL
                        .withCause(e)
                        .withDescription(e.getMessage())
                        .asException());
            }
        }
    }

    public void completeStream() {
        for (StreamObserver<PriceUpdate> observer : observers) {
            try {
                observer.onCompleted();
            } catch (Throwable e) {
                System.out.println("Stream error");
                System.out.println(e);
                observers.remove(observer);
                observer.onError(Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
            }
        }
    }

    public BindableService getGrpcService() {
        return delegate;
    }
}

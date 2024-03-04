package org.falland.grpc.longlivedstreams.examples.service;

import com.falland.gprc.longlivedstreams.proto.helloworld.v1.Hello;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.HelloWorldGrpc;
import com.falland.gprc.longlivedstreams.proto.helloworld.v1.World;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.falland.grpc.longlivedstreams.examples.apps.utils.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class NaiveService implements Service<Hello, World> {

    private final Collection<StreamObserver<World>> observers = ConcurrentHashMap.newKeySet();

    private final HelloWorldGrpc.HelloWorldImplBase delegate = new HelloWorldGrpc.HelloWorldImplBase() {
        @Override
        public void sayServerStreaming(Hello request, StreamObserver<World> responseObserver) {
            System.out.println("New client has connected");
            observers.add(responseObserver);
        }
    };

    @Override
    public Collection<Hello> getMessages() {
        return null;
    }

    public void publishMessage(World message) {
        for (StreamObserver<World> observer : observers) {
            try {
                observer.onNext(message);
            } catch (Throwable e) {
                System.out.println("Stream error");
                System.out.println(e);
                observers.remove(observer);
                observer.onError(Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
            }
        }
    }

    public void completeStream() {
        for (StreamObserver<World> observer : observers) {
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

    @Override
    public BindableService getBindableService() {
        return delegate;
    }
}

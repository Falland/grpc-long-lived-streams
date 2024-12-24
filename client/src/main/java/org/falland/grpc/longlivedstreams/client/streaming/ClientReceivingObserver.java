package org.falland.grpc.longlivedstreams.client.streaming;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.falland.grpc.longlivedstreams.client.UpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * This class is the call-back observer provided to gRPC client.
 * Client would call respective method on this observer once any message is received through the client channel.
 * This class is used also to support a client streaming back-pressing stream observers.
 * It utilises the before start method which is called on this observer before gRPC call is made to inject the onReady handler to a client-side stream observer.
 * It looks ugly and strange, but that is the current gRPC approach.
 * In order for this to work the {@link OnReadyHandlerForwarder} class is used to allow late binding of the onReadyHandler and the {@link org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver} logic
 * The forwarder is going to be injected first int the client stream, as it has to be done in the {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)}.
 * Next the forwarder is going to be wrapped into {@link OnReadyHandlerForwardingStreamObserver} class to mimic normal {@link org.falland.grpc.longlivedstreams.core.FlowControlledObserver} behavior that {@link org.falland.grpc.longlivedstreams.core.streams.BackpressingStreamObserver} can use.
 * It is very clumsy and shady, but unfortunately currently there's no way around it
 *
 * @param <Req> - request message type
 * @param <Resp> - response message type
 */
public class ClientReceivingObserver<Req, Resp> implements ClientResponseObserver<Req, Resp> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientReceivingObserver.class);

    private final UpdateProcessor<Resp> updateProcessor;
    private final Consumer<Throwable> errorHandler;
    private final Runnable completionHandler;
    private final Runnable onReadyHandler;
    private final Consumer<ClientCallStreamObserver<Req>> onBeforeStart;

    public ClientReceivingObserver(UpdateProcessor<Resp> updateProcessor,
                                   Consumer<Throwable> errorHandler,
                                   Runnable completionHandler,
                                   Runnable onReadyHandler, Consumer<ClientCallStreamObserver<Req>> onBeforeStart) {
        this.updateProcessor = updateProcessor;
        this.errorHandler = errorHandler;
        this.completionHandler = completionHandler;
        this.onReadyHandler = onReadyHandler;
        this.onBeforeStart = onBeforeStart;
    }

    public ClientReceivingObserver(UpdateProcessor<Resp> updateProcessor,
                                   Consumer<Throwable> errorHandler,
                                   Runnable completionHandler,
                                   Runnable onReadyHandler) {
        this(updateProcessor, errorHandler, completionHandler, onReadyHandler, (ignored) -> {});
    }

    @Override
    public void onNext(Resp update) {
        try {
            updateProcessor.processUpdate(update);
        } catch (Exception e) {
            LOGGER.error("Error while processing update {}", update, e);
            errorHandler.accept(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        errorHandler.accept(throwable);
    }

    @Override
    public void onCompleted() {
       completionHandler.run();
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<Req> requestStream) {
        requestStream.setOnReadyHandler(onReadyHandler);
        onBeforeStart.accept(requestStream);
    }
}

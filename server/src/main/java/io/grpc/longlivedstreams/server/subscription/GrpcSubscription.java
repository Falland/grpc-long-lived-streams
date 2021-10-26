package io.grpc.longlivedstreams.server.subscription;

import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

public interface GrpcSubscription<U> {

    void processUpdate(U update);

    boolean isActive();

    void onError(Throwable t);

    void onCompleted();

    String getAddress();

    SubscriptionType getType();

    String getClientId();

    /**
     * Returns the subscription that filters updates according to the predicate provided
     * If the message is filtered the underlying (this) subscription would not see it
     * @param filter - the predicate to filter out messages, must be not null
     * @return {@link FilteringGrpcSubscription} instance
     */
    default GrpcSubscription<U> withFilter(@Nonnull Predicate<U> filter) {
        return new FilteringGrpcSubscription<>(this, filter);
    }

    /**
     * Returns the subscription that transforms the update from one type to another
     * That might be handy when the stream type is hard to use/filter.
     * Transformer should always return some value that is not null, otherwise the {@link TransformingGrpcSubscription.TransformationException} will be thrown
     * @param transformer - the transformer function, must not be null
     * @param <T> - the message type that needs to be transformed to a underlying type
     * @return - {@link TransformingGrpcSubscription} instance
     */
    default <T> GrpcSubscription<T> withTransformation(@Nonnull Function<T,U> transformer) {
        return new TransformingGrpcSubscription<>(this, transformer);
    }
}

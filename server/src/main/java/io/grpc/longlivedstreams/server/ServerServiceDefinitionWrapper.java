package io.grpc.longlivedstreams.server;

import io.grpc.ServerServiceDefinition;
import io.grpc.longlivedstreams.server.address.AddressInterceptor;

/**
 * The interface that wraps gRPC server service definition
 * This interface is useful when you want to wrap your {@link io.grpc.BindableService} in an interceptor
 *
 * An example for such an intercepted service can be {@link AbstractSubscriptionAwareComponent}.
 * It uses {@link AddressInterceptor} to propagate client address to gRPC call
 */
public interface ServerServiceDefinitionWrapper {

    /**
     * Get the gRPC service definition for direct bind in gRPC server.
     *
     * @return the ServerServiceDefinition.
     */
    ServerServiceDefinition getServiceDefinition();
}

package org.falland.grpc.longlivedstreams.server;

import io.grpc.BindableService;
import org.falland.grpc.longlivedstreams.server.address.AddressInterceptor;
import io.grpc.ServerServiceDefinition;

/**
 * The interface that provides gRPC server service definition in order to attach it to the gRPC server
 * This interface is useful when you want to decorate your {@link io.grpc.BindableService} with some interceptors
 * An example for such an intercepted service can be {@link ClientAddressServiceDefinitionProvider}
 * It uses {@link AddressInterceptor} to propagate client address to gRPC call
 */
public interface ServerServiceDefinitionProvider extends BindableServiceProvider {

    /**
     * Get the gRPC service definition for direct bind in gRPC server.
     * @return the ServerServiceDefinition.
     */
    default ServerServiceDefinition getServiceDefinition(){
        return getBindableService().bindService();
    }
}

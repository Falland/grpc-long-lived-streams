package org.falland.grpc.longlivedstreams.server;

import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import org.falland.grpc.longlivedstreams.server.address.AddressInterceptor;

/**
 * The class decorates the service with the client address information extracted from the client call
 */
public abstract class ClientAddressServiceDefinitionProvider implements ServerServiceDefinitionProvider {

    @Override
    public ServerServiceDefinition getServiceDefinition() {
        return ServerInterceptors.intercept(getBindableService(), new AddressInterceptor());
    }
}

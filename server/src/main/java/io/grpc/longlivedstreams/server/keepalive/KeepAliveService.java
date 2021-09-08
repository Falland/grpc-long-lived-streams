package io.grpc.longlivedstreams.server.keepalive;

import io.grpc.longlivedstreams.server.ServerServiceDefinitionWrapper;
import io.grpc.ServerServiceDefinition;

/**
 * A service that allows gRPC services to be wrapped with application level
 * keep alive logic.
 *
 * <p>
 * The interceptor works on a per-service level, rather than the server level.
 * Suggested usage is to inject the {@link KeepAliveService} into any component
 * implementing {@link ServerServiceDefinitionWrapper} and add the interceptor when the
 * service is bound, for example:
 *
 * <pre>
 * &#64;Override
 * public ServerServiceDefinition getService() {
 *    return ServerInterceptors.intercept(ServerInterceptors.useInputStreamMessages(serviceDefinition),
 *                 new KeepAliveInterceptor(this));
 * }
 * </pre>
 */
public interface KeepAliveService {

    /**
     * Wraps the given service with an interceptor that adds an application
     * level keep alive function.
     *
     * @param serviceDefinition the service definition to intercept
     * @return the wrapped service definition
     */
    ServerServiceDefinition intercept(ServerServiceDefinition serviceDefinition);
}

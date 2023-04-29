package org.falland.grpc.longlivedstreams.server.keepalive;

import org.falland.grpc.longlivedstreams.server.keepalive.CallRegistry.Removable;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

@SuppressWarnings("squid:S119")
class KeepAliveInterceptor implements ServerInterceptor {

    private final CallRegistry callRegistry;

    KeepAliveInterceptor(CallRegistry callRegistry) {
        this.callRegistry = callRegistry;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (call.getMethodDescriptor().getType().serverSendsOneMessage()) {
            // ignore any non SERVER_STREAMING calls
            return next.startCall(call, headers);
        }
        IdleAwareServerCall<ReqT, RespT> keptAliveCall = new IdleAwareServerCall<>(call);
        Removable removable = callRegistry.register(keptAliveCall);
        return new KeepAliveServerCallListener<>(next.startCall(keptAliveCall, headers), removable);
    }
}

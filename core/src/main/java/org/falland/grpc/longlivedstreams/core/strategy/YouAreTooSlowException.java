package org.falland.grpc.longlivedstreams.core.strategy;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

public class YouAreTooSlowException extends StatusRuntimeException {

    public YouAreTooSlowException(String message) {
        super(Status.fromCode(Code.ABORTED).withDescription(message));
    }
}

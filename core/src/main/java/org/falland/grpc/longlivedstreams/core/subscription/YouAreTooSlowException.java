package org.falland.grpc.longlivedstreams.core.subscription;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

public class YouAreTooSlowException extends StatusRuntimeException {

    public YouAreTooSlowException(String message) {
        super(Status.fromCode(Code.OUT_OF_RANGE).withDescription(message));
    }
}

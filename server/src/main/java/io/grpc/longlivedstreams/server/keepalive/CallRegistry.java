package io.grpc.longlivedstreams.server.keepalive;

interface CallRegistry {

    /**
     * Registers a call to be kept alive by the service.
     *
     * @param serverCall the server call
     * @return a handback through which the call can be removed
     */
    Removable register(IdleAware serverCall);

    interface Removable {
        /**
         * Removes the server call from the service.
         */
        void remove();
    }

}

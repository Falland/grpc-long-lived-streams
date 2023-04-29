package com.falland.grpc.longlivedstreams.client;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

class ChannelStateListenerLogger implements Runnable {
    private final ManagedChannel channel;
    private final boolean autoReconnect;
    private final String channelName;
    private final Logger logger;
    private final List<GrpcConnectionListener> connectionListenersList = new LinkedList<>();
    private ConnectivityState previousState = ConnectivityState.SHUTDOWN;

    ChannelStateListenerLogger(ManagedChannel channel, boolean autoReconnect, String channelName) {
        this.channel = channel;
        this.autoReconnect = autoReconnect;
        this.channelName = channelName;
        this.logger = LoggerFactory.getLogger(channelName);
    }

    @Override
    public void run() {
        ConnectivityState state = channel.getState(autoReconnect);
        //We need to do this as after each call to the listener the listeners are cleared
        channel.notifyWhenStateChanged(state, this);
        logger.info("Channel {} state changed: {} -> {}", channelName, previousState, state);
        notifyListeners(channelName, state);
        previousState = state;
    }

    public void addGrpcConnectionListener(GrpcConnectionListener grpcConnectionListener) {
        connectionListenersList.add(grpcConnectionListener);
    }

    private void notifyListeners(String channelName, ConnectivityState state) {
        connectionListenersList.forEach(l -> l.onStateChanged(channelName, state));
    }
}

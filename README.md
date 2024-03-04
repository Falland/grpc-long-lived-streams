# grpc-long-lived-streams
This repository provides harness for building long-lived streaming apps on gRPC for Java
The main point for his library to exit is the asynchronous nature of Java gRPC StreamObserver.onNext(U update) method implementation
This provides non-blocking API call to the calling code, but this mechanism does not have any back pressure capabilities whatsoever.
That leads to a very unpleasant side effects for dense and long-lived streams.
When message producer spikes up on the rate in which updates are produced the internal gRPC/Netty send queue become to grow.
If gRPC client is slow or the network is not fast enough the queue might eat up all the Java heap.
When this happens the application starts to consume a lot of CPU and spins GC heavily, basically rendering the app unresposive.
Or even worse the whole app can crash with OOM. 

THe built-in gRPC mechanisms are very rudimentary to build a robust solution straight away.
StreamObserver has an extension to tell whether the stream is ready to take in next message and call some code once it is ready.

This library provides the toolset built on top of those mechanisms.
The library is structured in four modules:

## Core

This module provides tooling that allows you to provide proper back-pressure to producer.
With this library you can decorate the vanilla StreamObserver:
```java
BackpressingStreamObserver.<~>builder()
                        .withObserver(controlledStream)
                        .withStrategy(new BlockProducerOnOverflow<>(queueSize))
                        .build();
```
This snippet shows that there are two classes that contain all the magic.
First is [BackpressureStrategy](core/src/main/java/org/falland/grpc/longlivedstreams/core/strategy/BackpressureStrategy.java) (interface for BlockProducerOnOverflow) - defines the strategy for backpressure
You can choose a strategy from the existing strategies or implement your own
Second is [BackpressingStreamObserver](core/src/main/java/org/falland/grpc/longlivedstreams/core/streams/BackpressingStreamObserver.java) - this class works with gRPC mechanisms in order to ensure that messages are sent once the underlying layer is ready to receive them.

## Server

This module allows you to build server streaming solution that support multiple clients.
Check [Streamer](server/src/main/java/org/falland/grpc/longlivedstreams/server/streaming/Streamer.java) class for more information

## Client

This module allows you to build clients that can both listen to server streams and reconnect if needed, but also to stream themselves.
The client-side streaming supports same back-pressure strategies as server-side streaming.
Main class are [AbstractGrpcSubscriptionClient](client/src/main/java/org/falland/grpc/longlivedstreams/client/AbstractGrpcSubscriptionClient.java) and [ClientReceivingObserver](client/src/main/java/org/falland/grpc/longlivedstreams/client/streaming/ClientReceivingObserver.java)

## Examples

To see the code in action go to examples module
Run [test](examples/src/main/java/org/falland/grpc/longlivedstreams/examples/apps) apps
You can compare the behavior of different approaches and backpressure strategies
# grpc-long-lived-streams
This repository provides harness for building long lived streaming apps on gRPC 

Please check server module for server-side streaming support.
Main classes are Streamer and GrpcSubscription

Please check client module for client-side streaming support.
Main class is AbstractGrpcSubscriptionClient

To see the code in action go to examples module
Run [test](examples/src/main/java/io/grpc/longlivedstreams/examples/apps) apps
You can compare the behavior of different approaches and backpressure strategies
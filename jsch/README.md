## Overview

This is a helper library for managing and interacting with JetStream we are exploring a few options for how such a library will look and determine what will go into core NATS clients and what will be left as an external library.

This this library is not an official blessed way for interacting with JetStream yet but as a set of examples of all the capabilities this is valuable and it's for us a starting point to learning what patterns work well

## Terminology

The best introduction to JetStream is the main [README](https://github.com/nats-io/jetstream#readme) but we are planning to rename `Message Sets` to `Streams` and `Observables` to `Consumers`.  This library already use these new terms

## Setup

As some parts of this library will go into the core NATS client and others not we are not sure how it will get a NATS connection, for now we did a basic - but ugly - thing just to delay answering that.

```go
nc, _ := nats.Connect("localhost")

jsch.SetConnection(nc)
```

This will then use the NATS Connection you supply for all future interaction with JetStream

## Streams
### Creating Streams

Before anything you have to create a stream, the basic pattern is:

```go
stream, err := jsch.NewStream("ORDERS", jsch.Subjects("ORDERS.*"), jsch.MaxAge(24*365*time.Hour), jsch.FileStorage())
```

This can get quite verbose so you might have a template configuration of your own chosing to create many similar sets.

```go
template := &server.MsgSetConfig{
	Retention:      server.StreamPolicy,
	MaxObservables: -1,
	MaxMsgs:        -1,
	MaxBytes:       -1,
	MaxAge:         24 * 365 * time.Hour,
	MaxMsgSize:     -1,
	Replicas:       1,
	NoAck:          false,
    Storage:        server.FileStorage,
}

orders, _ := jsch.NewStreamFromTemplate("ORDERS", template, jsch.Subjects("ORDERS.*"))
archive, _ := jsch.NewStreamFromTemplate("ARCHIVE", template, jsch.Subjects("ARCHIVE"), jsch.MaxAge(5*template.MaxAge))
```

The `jsch.NewStream` uses `jsch.DefaultStream` as starting template.  We also have `jsch.DefaultWorkQueue` to help you with a sane starting point.

You can even copy set configurations this way:

```go
orders, err := jsch.NewStream("ORDERS", jsch.Subjects("ORDERS.*"), jsch.MaxAge(24*365*time.Hour), jsch.FileStorage())
staging, err := jsch.NewStreamFromTemplate("STAGING", orders.Configuration(), jsch.Subjects("STAGINGORDERS.*"))
```

### Loading references to existing streams

Once a Stream exist you can load it later:

```go
orders, err := jsch.LoadStream("ORDERS")
```

This will fail if the stream does not exist, create and load can be combined:

```go
orders, err := jsch.LoadOrNewFromTemplate("ORDERS", template, jsch.Subjects("ORDERS.*"))
```

This will create the Stream if it doesn't exist, else load the existing one - though no effort is made to ensure the loaded one matches the desired configuration in that case.

### Associated Consumers

With a stream handle you can et lists of known Consumers using `stream.ConsumerNames()`, or create new Consumers within the stream using `stream.NewConsumer` and `stream.NewConsumerFromTemplate`. Consumers can also be loaded using `stream.LoadConsumer` and you can combine load and create using `stream.LoadOrNewConsumer` and `stream.LoadOrNewConsumerFromTemplate`.

These methods just proxy to the Consumer specific ones which will be discussed below.

### Other actions

There are a number of other functions allowing you to purge messages, read individual messages, get statistics and access the configuration. Review the godoc for details.

## Consumers

### Creating

Above you saw that once you have a handle to a stream you can create and load consumers, you can access the consumer directly though, lets create one:

```go
consumer, err := jsch.NewConsumer("ORDERS", "NEW", jsch.FilterSubject("ORDERS.received"), jsch.SampleFrequency("100"))
```

Like with Streams we have `NewConsumerFromTemplate`, `LoadOrNewConsumer` and `LoadOrNewConsumerFromTemplate` and we supply 2 default templates to help you `DefaultConsumer` and `SampledDefaultConsumer`.

Many options exist to set starting points, durability and more - everything that you will find in the `jsm` utility, review the godoc for full details.

### Consuming

Push-based Consumers are accessed using the normal NATS subscribe dynamics, we have a few helpers:

```go
sub, err := consumer.Subscribe(func(m *nats.Msg) {
   // handle the message
})
```

We have all the usual `Subscribe`, `ChanSubscribe`, `ChanQueueSubscribe`, `SubscribeSync`, `QueueSubscribe`, `QueueSubscribeSync` and `QueueSubscribeSyncWithChan`, these just proxy to the standard go library functions of the same name but it helps you with the topic names etc.

For Pull-based Consumers we have:

```go
// 1 message
msg, err := consumer.NextMsg()

// 10 messages
msgs, err := consumer.NextMsgs(10)
```

When consuming these messages they have metadata attached that you can parse:

```go
msg, _ := consumer.NextMsg()
meta, _ := jsch.ParseJSMsgMetadata(msg)
```

At this point you have access to `Stream`, `Consumer` for the names and `StreamSequence`, `ConsumerSequence` to determine which exact message and `Delivered` for how many times it was redelivered.

### Other Actions

There are a number of other functions to help you determine if its Pull or Push, is it Durable, Sampled and to access the full configuration.

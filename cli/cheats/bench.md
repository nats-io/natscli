# benchmark core nats publish with 10 publishers on subject foo
nats bench pub foo --clients 10 --msgs 10000 --size 512

# benchmark core nats subscribe for 4 clients on subject foo
nats bench sub foo --clients 5 --msgs 10000

# benchmark core nats request-reply with queuing
## run 4 clients servicing requests
nats bench service serve --clients 4 testservice

## run 4 clients making synchronous requests on the service at subject testservice
nats bench service request --clients 4 testservice --msgs 20000

# benchmark JetStream asynchronously acknowledged publishing of batches of 1000 on subject foo creating the stream first
nats bench js pub foo --create --batch 1000

# benchmark JetStream synchronous publishing on subject foo using 10 clients and purging the stream first
nats bench js pub foo --purge --batch=1 --clients=10

# benchmark JetStream delivery of messages from a stream using an ephemeral ordered consumer, disabling the progress bar
nats bench js ordered --no-progress

# benchmark JetStream delivery of messages from a stream through a durable consumer shared by 4 clients using the Consume() (callback) method.
nats bench js consume --clients 4

# benchmark JetStream delivery of messages from a stream through a durable consumer with no acks shared by 4 clients using the fetch() method with batches of 1000.
nats bench js fetch --clients 4 --acks=none --batch=1000

# simulate a message processing time of 50 microseconds
nats bench service serve testservice --sleep 50us

# generate load by publishing messages at an interval of 100 nanoseconds rather than back to back
nats bench pub foo --sleep=100ns

# remember when benchmarking JetStream
Once you are finished benchmarking, remember to free up the resources (i.e. memory and files) consumed by the stream using 'nats stream rm'.

You can get more accurate results by disabling the progress bar using the `--no-progress` flag.
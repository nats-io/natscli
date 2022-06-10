# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench testsubject --pub 10 --sub 10 --msgs 10000 --size 512

# benchmark core nats request-reply without subscribers using a queue
nats bench testsubject --pub 1 --sub 1 --msgs 10000 --no-queue

# benchmark core nats request-reply with queuing
nats bench testsubject --sub 4 --reply
nats bench testsubject --pub 4 --request --msgs 20000

# benchmark JetStream synchronously acknowledged publishing purging the data first
nats bench testsubject --js --syncpub --pub 10  --msgs 10000 --purge

# benchmark JS publish and push consumers at the same time purging the data first
nats bench testsubject --js --pub 4 --sub 4 --purge

# benchmark JS stream purge and async batched publishing to the stream
nats bench testsubject --js --pub 4 --purge

# benchmark JS stream get replay from the stream using a push consumer
nats bench testsubject --js --sub 4

# benchmark JS stream get replay from the stream using a pull consumer
nats bench testsubject --js --sub 4 --pull

# simulate a message processing time (for reply mode and pull JS consumers) of 50 microseconds
nats bench testsubject --reply --sub 1 --acksleep 50us

# generate load by publishing messages at an interval of 100 nanoseconds rather than back to back
nats bench testsubject --pub 1 --pubsleep 100ns

# remember when benchmarking JetStream
Once you are finished benchmarking, remember to free up the resources (i.e. memory and files) consumed by the stream using 'nats stream rm'

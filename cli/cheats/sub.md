# To subscribe to messages, in a queue group and acknowledge any JetStream ones
nats sub source.subject --queue work --ack

# To subscribe to a randomly generated inbox
nats sub --inbox

# To dump all messages to files, 1 file per message
nats sub --inbox --dump /tmp/archive

# To process all messages using xargs 1 message at a time through a shell command
nats sub subject --dump=- | xargs -0 -n 1 -I "{}" sh -c "echo '{}' | wc -c"

# To receive new messages received in a stream with the subject ORDERS.new
nats sub ORDERS.new --next

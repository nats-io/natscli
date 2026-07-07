# To publish 100 messages with a random body between 100 and 1000 characters
nats pub destination.subject "{{ Random 100 1000 }}" -H Count:{{ Count }} --count 100

# To publish messages from STDIN
echo "hello world" | nats pub destination.subject

# To publish messages from STDIN in a headless (non-tty) context
echo "hello world" | nats pub --force-stdin destination.subject

# To request a response from a server and show just the raw result
nats request destination.subject "hello world" -H "Content-type:text/plain" --raw

# To listen on STDIN and publish one message per newline
nats pub destination.subject --send-on=newline
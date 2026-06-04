# Adding, Removing, Viewing a Stream
nats stream add
nats stream info STREAMNAME
nats stream rm STREAMNAME

# Editing a single property of a stream
nats stream edit STREAMNAME --description "new description"
# Editing a stream configuration in your editor
EDITOR=vi nats stream edit -i STREAMNAME

# Show a list of streams, including basic info or compatible with pipes
nats stream list
nats stream list -n

# Find all empty streams or streams with messages
nats stream find --empty
nats stream find --empty --invert

# Creates a new Stream based on the config of another, does not copy data
nats stream copy ORDERS ARCHIVE --description "Orders Archive" --subjects ARCHIVE

# Get message 12344, delete a message, delete all messages
nats stream get ORDERS 12345
nats stream rmm ORDERS 12345

# Purge messages from streams
nats stream purge ORDERS
# deletes up to, but not including, 1000
nats stream purge ORDERS --seq 1000
nats stream purge ORDERS --keep 100
nats stream purge ORDERS --subject one.subject

# Page through a stream
nats stream view ORDERS
nats stream view --id 1000
nats stream view --since 1h
nats stream view --subject one.subject

# Backup and restore
nats stream backup ORDERS backups/orders/$(date +%Y-%m-%d)
nats stream restore ORDERS backups/orders/$(date +%Y-%m-%d)

# Marks a stream as read only
nats stream seal ORDERS

# Force a cluster leader election
nats stream cluster ORDERS down

# Evict the stream from a node
stream cluster peer-remove ORDERS nats1.example.net

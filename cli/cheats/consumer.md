# Adding, Removing, Viewing a Consumer
nats consumer add
nats consumer info ORDERS NEW
nats consumer rm ORDERS NEW

# Editing a consumer
nats consumer edit ORDERS NEW --description "new description"

# Get messages from a consumer
nats consumer next ORDERS NEW --ack
nats consumer next ORDERS NEW --no-ack
nats consumer sub ORDERS NEW --ack

# Force leader election on a consumer
nats consumer cluster down ORDERS NEW

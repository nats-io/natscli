# Create or update
nats context add development --server nats.dev.example.net:4222 [other standard connection properties]
nats context add ngs --description "NGS Connection in Orders Account" --nsc nsc://acme/orders/new
nats context edit development [standard connection properties]

# View contexts
nats context ls
nats context info development --json

# Validate all connections are valid and that connections can be established
nats context validate --connect

# Select a new default context
nats context select

# Connecting using a context
nats pub --context development subject body

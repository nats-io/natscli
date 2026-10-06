# to decode jwt from .creds file and get prettified readable json in stdout
nats jwt --creds ./some.creds

# to decode jwt from a creds file and get json bytes in stdout that you can pipe to tools like jq
nats jwt --creds ./some.creds --json

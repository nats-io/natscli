# To see all available schemas using regular expressions
nats schema search 'response|request'

# To view a specific schema
nats schema info io.nats.jetstream.api.v1.stream_msg_get_request --yaml

# To validate a JSON input against a specific schema
nats schema validate io.nats.jetstream.api.v1.stream_msg_get_request request.json

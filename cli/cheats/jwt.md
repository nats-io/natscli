# decode a user creds file, shows the JWT part only, never the seed
nats jwt decode ~/.local/share/nats/nsc/keys/creds/O/A/user.creds

# decode a raw JWT
nats jwt decode eyJ0eXAiOiJKV1Qi...

# full claims as JSON for jq
nats jwt decode user.jwt --json | jq .nats
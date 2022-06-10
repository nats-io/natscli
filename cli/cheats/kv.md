# to create a replicated KV bucket
nats kv add CONFIG --replicas 3

# to store a value in the bucket
nats kv put CONFIG username bob

# to read just the value with no additional details
nats kv get CONFIG username --raw

# view an audit trail for a key if history is kept
nats kv history CONFIG username

# to see the bucket status
nats kv status CONFIG

# observe real time changes for an entire bucket
nats kv watch CONFIG
# observe real time changes for all keys below users
nats kv watch CONFIG 'users.>''

# create a bucket backup for CONFIG into backups/CONFIG
nats kv status CONFIG
nats stream backup <stream name> backups/CONFIG

# restore a bucket from a backup
nats stream restore <stream name> backups/CONFIG

# list known buckets
nats kv ls

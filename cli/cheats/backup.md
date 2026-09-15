# Checking a backup taken with nats stream backup
nats backup validate ./orders-backup
nats backup info ./orders-backup

# Writing an edited copy of a backup, the target must not exist or be empty
nats backup edit ./orders-backup ./orders-new --subject 'ORDERS.new.>' --exclude-subject 'ORDERS.new.test.*'
nats backup edit ./orders-backup ./orders-recent --after 24h
nats backup edit ./orders-backup ./orders-window --after 2026-09-01T00:00:00Z --before 2026-09-02T00:00:00Z
nats backup edit ./orders-backup ./orders-range --first-seq 1000 --last-seq 2000
nats backup edit ./orders-backup ./orders-batch --header X-Batch:7 --exclude-header X-Test
nats backup edit ./orders-backup ./orders-urgent --payload-match urgent

# Keeping only the newest messages of every subject, or the latest revision of every key
nats backup edit ./orders-backup ./orders-latest --last-per-subject 1
nats backup edit ./kv-backup ./kv-compacted --kv-compact

# Renumbering the kept messages from 1, this drops all consumers
nats backup edit ./orders-backup ./orders-fresh --subject 'ORDERS.new.>' --renumber

# Previewing an edit without writing anything
nats backup edit ./orders-backup ./orders-new --subject 'ORDERS.new.>' --dry-run

# Producing a shareable backup with names and subjects hashed and bodies zeroed
# the key file ./orders-shared.keys.json is written beside the target and is the sensitive artifact
nats backup edit ./orders-backup ./orders-shared --obfuscate
nats backup edit ./audit-backup ./audit-shared --obfuscate --obfuscation-key ./orders-shared.keys.json

# Looking up the originals behind obfuscated names and subjects
nats backup lookup ./orders-shared.keys.json e8g0nh0n9reafabq idem7issknbfa9pu.ll81625jh1leedit

# Listing every subject in a backup with its message count
nats backup info ./orders-backup --subjects

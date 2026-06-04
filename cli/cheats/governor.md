# to create governor with 10 slots and 1 minute timeout
nats governor add cron 10 1m

# to view the configuration and state
nats governor view cron

# to reset the governor, clearing all slots
nats governor reset cron

# to run long-job.sh when a slot is available, giving up after 20 minutes without a slot
nats governor run cron $(hostname -f) --max-wait 20m long-job.sh'

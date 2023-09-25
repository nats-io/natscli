# To see all servers, including their server ID and show a response graph
nats server ping --id --graph --user system

# To see information about a specific server
nats server info nats1.example.net --user system
nats server info NCAXNST2VH7QGBVYBEDQGX73GMBXTWXACUTMQPTNKWLOYG2ES67NMX6M --user system

# To list all servers and show basic summaries, expecting responses from 10 servers
nats server list 10 --user system

# To report on current connections
nats server report connections
nats server report connz --account WEATHER
nats server report connz --sort in-msgs
nats server report connz --top 10 --sort in-msgs

# To limit connections report to surveyor connections and all from a specific IP using https://expr.medv.io/docs/Language-Definition
nats server report connz --filter 'lower(conns.name) matches "surveyor" || conns.ip == "46.101.44.80"'

# To report on accounts
nats server report accounts
nats server report accounts --account WEATHER --sort in-msgs --top 10

# To report on JetStream usage by account WEATHER
nats server report jetstream --account WEATHER --sort cluster

# To generate a NATS Server bcrypt command
nats server password
nats server pass -p 'W#OZwVN-UjMb8nszwvT2LQ'
nats server pass -g
PASSWORD='W#OZwVN-UjMb8nszwvT2LQ' nats server pass

# To request raw monitoring data from servers
nats server request subscriptions --detail --filter-account WEATHER --cluster EAST
nats server req variables --name nats1.example.net
nats server req connections --filter-state open
nats server req connz --subscriptions --name nats1.example.net
nats server req gateways --filter-name EAST
nats server req leafnodes --subscriptions
nats server req accounts --account WEATHER
nats server req jsz --leader

# To manage JetStream cluster RAFT membership
nats server raft step-down

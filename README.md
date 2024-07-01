## The NATS Command Line Interface

A command line utility to interact with and manage NATS.

This utility replaces various past tools that were named in the form `nats-sub` and `nats-pub`, adds several new capabilities
and support full JetStream management.

### Features

* JetStream management
* JetStream data and configuration backup
* Message publish and subscribe
* Service requests and creation
* Benchmarking and Latency testing
* Super Cluster observation
* Configuration context maintenance
* NATS eco system schema registry

### Installation

Releases are [published to GitHub](https://github.com/nats-io/natscli/releases/) where zip, rpm and debs for various
operating systems can be found.

#### Installation via go install

The nats cli can be installed directly via `go install`.
To install the latest version:

```
go install github.com/nats-io/natscli/nats@latest
```

To install a specific release:

```
go install github.com/nats-io/natscli/nats@v0.0.33
```

#### macOS installation via Homebrew

For macOS `brew` can be used to install the latest version:

```nohighlight
brew tap nats-io/nats-tools
brew install nats-io/nats-tools/nats
```

#### Arch Linux installation via yay

For Arch users there is an [AUR package](https://aur.archlinux.org/packages/natscli/) that you can install with:

```
yay natscli
```

#### Installation from the shell

The following script will install the latest version of the nats cli on Linux and macOS:

```nohighlight
curl -sf https://binaries.nats.dev/nats-io/natscli/nats@latest | sh
```

#### Nightly docker images

Nightly builds are included in the `synadia/nats-server:nightly` Docker images.

### Configuration Contexts

The `nats` CLI supports multiple named configurations, for the rest of the document we'll interact via `demo.nats.io`.
To enable this we'll create a `demo` configuration and set it as default.

First we add a configuration to capture the default `localhost` configuration.
```
nats context add localhost --description "Localhost"
```
Output
```
NATS Configuration Context "localhost"

  Description: Localhost
  Server URLs: nats://127.0.0.1:4222
```

Next we add a context for `demo.nats.io:4222` and we select it as default.

```
nats context add nats --server demo.nats.io:4222 --description "NATS Demo" --select
```
Output
```
NATS Configuration Context "nats"

  Description: NATS Demo
  Server URLs: demo.nats.io:4222
```

These are the contexts, the `*` indicates the default
```
nats context ls
```
Output
```
Known contexts:

   localhost           Localhost
   nats*               NATS Demo
```

The context is selected as default, use `nats context --help` to see how to add, remove and edit contexts.

### Configuration file

nats-cli stores contextes in `~/.config/nats/context`. Those contextes are stored as JSON documents. You can find the description and expected value for this configuration file by running `nats --help` and look for the global flags.

### JetStream management

For full information on managing JetStream please refer to the [JetStream Documentation](https://docs.nats.io/jetstream)

As of [nats-server v2.2.0](https://docs.nats.io/release-notes/whats_new/whats_new_22) JetStream is GA.

### Publish and Subscribe

The `nats` CLI can publish messages and subscribe to subjects.

#### Basic Behaviours

We will subscribe to the `cli.demo` subject:

```
nats sub cli.demo 
```
Output
```
12:30:25 Subscribing on cli.demo
``` 

We can now publish messages to the `cli.demo` subject.

First we publish a single message:

```
nats pub cli.demo "hello world" 
```
Output
```
12:31:20 Published 11 bytes to "cli.demo"
```

Next we publish 5 messages with a counter and timestamp in the format `message 5 @ 2020-12-03T12:33:18+01:00`:

```
nats pub cli.demo "message {{.Count}} @ {{.TimeStamp}}" --count=5
```
Output
```
12:33:17 Published 33 bytes to "cli.demo"
12:33:17 Published 33 bytes to "cli.demo"
12:33:17 Published 33 bytes to "cli.demo"
12:33:18 Published 33 bytes to "cli.demo"
12:33:18 Published 33 bytes to "cli.demo"
```

We can also publish messages read from STDIN:

```
echo hello|nats pub cli.demo 
```
Output
```
12:34:15 Reading payload from STDIN
12:34:15 Published 6 bytes to "cli.demo"
```

Finally, NATS supports HTTP style headers and the CLI behaves like `curl`:

```
nats pub cli.demo 'hello headers' -H Header1:One -H Header2:Two 
```
Output
```
12:38:44 Published 13 bytes to "cli.demo"
```

The receiver will show:

```
nats sub cli.demo  
```
Output
```
[#47] Received on "cli.demo"
Header1: One
Header2: Two

hello headers
```

### match requests and replies
We can print matching replay-requests together
```
nats sub --match-replies cli.demo
```
Output
```
[#48] Received on "cli.demo" with reply "_INBOX.12345"

[#48] Matched reply on "_INBOX.12345"
```

sub --match-replies --dump subject.name
```
Output
X.json
X_reply.json
```
#### JetStream

When receiving messages from a JetStream Push Consumer messages can be acknowledged when received by passing `--ack`, the
message metadata is also produced:

```
nats sub js.out.testing --ack 
```
Output
```
12:55:23 Subscribing on js.out.testing with acknowledgement of JetStream messages
[#1] Received JetStream message: consumer: TESTING > TAIL / subject: js.in.testing / delivered: 1 / consumer seq: 568 / stream seq: 2638 / ack: true
test JS message
``` 

#### Queue Groups

When subscribers join a Queue Group the messages are randomly load shared within the group. Perform the following
subscribe in 2 or more shells and then publish messages using some of the methods shown above, these messages will
only be received by one of the subscribers at a time.

```
nats sub cli.demo --queue=Q1
```

### Service Requests and Creation

NATS supports a RPC mechanism where a service received Requests and replies with data in response.

```
nats reply 'cli.weather.>' "Weather Service" 
```
Output
```
12:43:28 Listening on "cli.weather.>" in group "NATS-RPLY-22"
```

In another shell we can send a request to this service:

```
nats request "cli.weather.london" '' 
```
Output
```
12:46:34 Sending request on "cli.weather.london"
12:46:35 Received on "_INBOX.BJoZpwsshQM5cKUj8KAkT6.HF9jslpP" rtt 404.76854ms
Weather Service
```

This shows that the service round trip was 404ms, and we can see the response `Weather Service`.

To make this a bit more interesting we can interact with the `wttr.in` web service:

```
nats reply 'cli.weather.>' --command "curl -s wttr.in/{{2}}?format=3" 
```
Output
```
12:47:03 Listening on "cli.weather.>" in group "NATS-RPLY-22"
```

We can perform the same request again:

```
nats request "cli.weather.{london,newyork}" '' --raw 
```
Output
```
london: 🌦 +7°C
newyork: ☀️ +2°C
```

Now the `nats` CLI parses the subject, extracts the `{london,newyork}` from the subjects and calls `curl`, replacing
`{{2}}` with the body of the 2nd subject token - `{london,newyork}`.

## Translating message data using a converter command

Additional to the raw output of messages using `nats sub` and `nats stream view` you can also translate the message data by running it through a command. 

The command receives the message data as raw bytes through stdin and the output of the command will be the shown output for the message. There is the additional possibility to add the filter subject by using `{{Subject}}` as part of the arguments for the tranlation command.

#### Examples for using the translation feature:

Here we use the [jq](https://github.com/stedolan/jq) tool to format our json message paylot into a more readable format:

We subscribe to a subject that will receive json data.
```
nats sub --translate 'jq .' cli.json
```
Now we publish some example data.
```
nats pub cli.json '{"task":"demo","duration":60}'
```

The Output will show the message formatted.
```
23:54:35 Subscribing on cli.json
[#1] Received on "cli.json"
{
  "task": "demo",
  "duration": 60
}
```

Another example is creating hex dumps from any message to avoid terminal corruption.

By changing the subscription into:

```
nats sub --translate 'xxd' cli.json
```

We will get the following output for the same published msg:
```
00:02:56 Subscribing on cli.json
[#1] Received on "cli.json"
00000000: 7b22 7461 736b 223a 2264 656d 6f22 2c22  {"task":"demo","
00000010: 6475 7261 7469 6f6e 223a 3630 7d         duration":60}
```

#### Examples for using the translation feature with template:

A somewhat artificial example using the subject as argument would be:
```
nats sub --translate "sed 's/\(.*\)/{{Subject}}: \1/'" cli.json
```

Output
```
00:22:19 Subscribing on cli.json
[#1] Received on "cli.json"
cli.json: {"task":"demo","duration":60}
```

The translation feature makes it possible to write specialized or universal translators to aid in debugging messages in streams or core nats.

## Benchmarking and Latency Testing

Benchmarking and latency testing is key requirement for evaluating the production preparedness of your NATS network.

### Benchmarking

Here we'll run these benchmarks against a local server instead of `demo.nats.io`.

```
nats context select localhost 
```
Output
```
NATS Configuration Context "localhost"

  Description: Localhost
  Server URLs: nats://127.0.0.1:4222
```

We can benchmark core NATS publishing performance, here we publish 10 million messages from 5 concurrent publishers. By
default messages are published as quick as possible without any acknowledgement or confirmations:

```
nats bench test --msgs=10000000 --pub 5 
```
Output
```                                                                                                                                                                                                                                                                                                                                                                         <13:03:30
01:30:14 Starting benchmark [msgs=10,000,000, msgsize=128 B, pubs=5, subs=0, js=false, stream=benchstream  storage=memory, syncpub=false, pubbatch=100, jstimeout=30s, pull=false, pullbatch=100, request=false, reply=false, noqueue=false, maxackpending=-1, replicas=1, purge=false]
Finished      0s [================================================] 100%
Finished      0s [================================================] 100%
Finished      0s [================================================] 100%
Finished      0s [================================================] 100%
Finished      0s [================================================] 100%

Pub stats: 14,047,987 msgs/sec ~ 1.67 GB/sec
 [1] 3,300,540 msgs/sec ~ 402.90 MB/sec (2000000 msgs)
 [2] 3,306,601 msgs/sec ~ 403.64 MB/sec (2000000 msgs)
 [3] 3,296,538 msgs/sec ~ 402.41 MB/sec (2000000 msgs)
 [4] 2,813,752 msgs/sec ~ 343.48 MB/sec (2000000 msgs)
 [5] 2,811,227 msgs/sec ~ 343.17 MB/sec (2000000 msgs)
 min 2,811,227 | avg 3,105,731 | max 3,306,601 | stddev 239,453 msgs
```

Adding `--sub 2` will start two subscribers on the same subject and measure the rate of messages:

```
nats bench test --msgs=10000000 --pub 5 --sub 2 
```
Output
```
...
01:30:52 Starting benchmark [msgs=10,000,000, msgsize=128 B, pubs=5, subs=2, js=false, stream=benchstream  storage=memory, syncpub=false, pubbatch=100, jstimeout=30s, pull=false, pullbatch=100, request=false, reply=false, noqueue=false, maxackpending=-1, replicas=1, purge=false]
01:30:52 Starting subscriber, expecting 10,000,000 messages
01:30:52 Starting subscriber, expecting 10,000,000 messages
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%
Finished      6s [================================================] 100%

NATS Pub/Sub stats: 4,906,104 msgs/sec ~ 598.89 MB/sec
 Pub stats: 1,635,428 msgs/sec ~ 199.64 MB/sec
  [1] 328,573 msgs/sec ~ 40.11 MB/sec (2000000 msgs)
  [2] 328,147 msgs/sec ~ 40.06 MB/sec (2000000 msgs)
  [3] 327,411 msgs/sec ~ 39.97 MB/sec (2000000 msgs)
  [4] 327,318 msgs/sec ~ 39.96 MB/sec (2000000 msgs)
  [5] 327,283 msgs/sec ~ 39.95 MB/sec (2000000 msgs)
  min 327,283 | avg 327,746 | max 328,573 | stddev 520 msgs
 Sub stats: 3,271,233 msgs/sec ~ 399.32 MB/sec
  [1] 1,635,682 msgs/sec ~ 199.67 MB/sec (10000000 msgs)
  [2] 1,635,616 msgs/sec ~ 199.66 MB/sec (10000000 msgs)
  min 1,635,616 | avg 1,635,649 | max 1,635,682 | stddev 33 msgs
```

JetStream testing can be done by adding the `--js` flag. You can for example measure first the speed of publishing into a stream

```
nats bench js.bench --js --pub 2 --msgs 1000000 --purge 
```
Output
```
01:37:36 Starting benchmark [msgs=1,000,000, msgsize=128 B, pubs=2, subs=0, js=true, stream=benchstream  storage=memory, syncpub=false, pubbatch=100, jstimeout=30s, pull=false, pullbatch=100, request=false, reply=false, noqueue=false, maxackpending=-1, replicas=1, purge=true]
01:37:36 Purging the stream
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%

Pub stats: 415,097 msgs/sec ~ 50.67 MB/sec
 [1] 207,907 msgs/sec ~ 25.38 MB/sec (500000 msgs)
 [2] 207,572 msgs/sec ~ 25.34 MB/sec (500000 msgs)
 min 207,572 | avg 207,739 | max 207,907 | stddev 167 msgs
```
And then you can for example measure the speed of receiving (i.e. replay) the messages from the stream using ordered push consumers
```
nats bench js.bench --js --sub 4 --msgs 1000000 
```
Output
```
01:40:05 Starting benchmark [msgs=1,000,000, msgsize=128 B, pubs=0, subs=4, js=true, stream=benchstream  storage=memory, syncpub=false, pubbatch=100, jstimeout=30s, pull=false, pullbatch=100, request=false, reply=false, noqueue=false, maxackpending=-1, replicas=1, purge=false]
01:40:05 Starting subscriber, expecting 1,000,000 messages
01:40:05 Starting subscriber, expecting 1,000,000 messages
01:40:05 Starting subscriber, expecting 1,000,000 messages
01:40:05 Starting subscriber, expecting 1,000,000 messages
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%

Sub stats: 1,522,920 msgs/sec ~ 185.90 MB/sec
 [1] 382,739 msgs/sec ~ 46.72 MB/sec (1000000 msgs)
 [2] 382,772 msgs/sec ~ 46.73 MB/sec (1000000 msgs)
 [3] 382,407 msgs/sec ~ 46.68 MB/sec (1000000 msgs)
 [4] 381,060 msgs/sec ~ 46.52 MB/sec (1000000 msgs)
 min 381,060 | avg 382,244 | max 382,772 | stddev 698 msgs
```

Similarily you can benchmark synchronous request-reply type of interactions using the `--request` and `--reply` flags. For example you can first start one (or more) replier(s)

```
nats bench test --sub 2 --reply
```

And then run a benchmark with one (or more) synchronous requester(s)

```
nats bench test --pub 10 --request  
```
Output
```
03:04:56 Starting benchmark [msgs=100,000, msgsize=128 B, pubs=10, subs=0, js=false, stream=benchstream  storage=memory, syncpub=false, pubbatch=100, jstimeout=30s, pull=false, pullbatch=100, request=true, reply=false, noqueue=false, maxackpending=-1, replicas=1, purge=false]
03:04:56 Benchmark in request-reply mode
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%
Finished      2s [================================================] 100%

Pub stats: 40,064 msgs/sec ~ 4.89 MB/sec
 [1] 4,045 msgs/sec ~ 505.63 KB/sec (10000 msgs)
 [2] 4,031 msgs/sec ~ 503.93 KB/sec (10000 msgs)
 [3] 4,034 msgs/sec ~ 504.37 KB/sec (10000 msgs)
 [4] 4,031 msgs/sec ~ 503.92 KB/sec (10000 msgs)
 [5] 4,022 msgs/sec ~ 502.85 KB/sec (10000 msgs)
 [6] 4,028 msgs/sec ~ 503.59 KB/sec (10000 msgs)
 [7] 4,025 msgs/sec ~ 503.22 KB/sec (10000 msgs)
 [8] 4,028 msgs/sec ~ 503.59 KB/sec (10000 msgs)
 [9] 4,025 msgs/sec ~ 503.15 KB/sec (10000 msgs)
 [10] 4,018 msgs/sec ~ 502.28 KB/sec (10000 msgs)
 min 4,018 | avg 4,028 | max 4,045 | stddev 7 msgs
```

There are numerous other flags that can be set to configure size of messages, using push or pull JetStream consumers and much more, see `nats bench --help`.

### Latency

Latency is the rate at which messages can cross your network, with the `nats` CLI you can connect a publisher and subscriber
to your NATS network and measure the latency between the publisher and subscriber.

```
nats latency --server-b localhost:4222 --rate 500000  
```
Output
```
==============================
Pub Server RTT : 64µs
Sub Server RTT : 70µs
Message Payload: 8B
Target Duration: 5s
Target Msgs/Sec: 500000
Target Band/Sec: 7.6M
==============================
HDR Percentiles:
10:       57µs
50:       94µs
75:       122µs
90:       162µs
99:       314µs
99.9:     490µs
99.99:    764µs
99.999:   863µs
99.9999:  886µs
99.99999: 1.483ms
100:      1.483ms
==============================
Actual Msgs/Sec: 499990
Actual Band/Sec: 7.6M
Minimum Latency: 25µs
Median Latency : 94µs
Maximum Latency: 1.483ms
1st Sent Wall Time : 3.091ms
Last Sent Wall Time: 5.000098s
Last Recv Wall Time: 5.000168s
```

Various flags exist to adjust message size and target rates, see `nats latency --help`

## Super Cluster observation

NATS publish a number of events and have a Request-Reply API that expose a wealth of internal information about the
state of the network.

For most of these features you will need a [System Account](https://docs.nats.io/nats-server/configuration/sys_accounts)
enabled, most of these commands are run against that account.

I create a `system` context before running these commands and pass that to the commands.

### Lifecycle Events

```
nats event --context system 
```
Output
```
Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT
Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT
Listening for Authentication Errors events on $SYS.SERVER.*.CLIENT.AUTH.ERR

[12:18:35] [puGCIK5UcWUxBXJ52q4Hti] Client Connection

   Server: nc1-c1
  Cluster: c1

   Client:
                 ID: 17
               User: one
               Name: NATS CLI Version development
            Account: one
    Library Version: 1.11.0  Language: go
               Host: 172.21.0.1

[12:18:35] [puGCIK5UcWUxBXJ52q4Hw8] Client Disconnection

   Reason: Client Closed
   Server: nc1-c1
  Cluster: c1

   Client:
                 ID: 17
               User: one
               Name: NATS CLI Version development
            Account: one
    Library Version: 1.11.0  Language: go
               Host: 172.21.0.1

   Stats:
      Received: 0 messages (0 B)
     Published: 1 messages (0 B)
           RTT: 1.551714ms
```

Here one can see a client connected and disconnected shortly after, several other system events are supported.

If an account is running JetStream the `nats event` tool can also be used to look at JetStream advisories by passing
`--js-metric --js-advisory`

These events are JSON messages and can be viewed raw using `--json` or in Cloud Events format with `--cloudevent`,
finally a short version of the messages can be shown:

```
nats event --short 
```
Output
```
Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT
Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT
Listening for Authentication Errors events on $SYS.SERVER.*.CLIENT.AUTH.ERR
12:20:58 [Connection] user: one cid: 19 in account one
12:20:58 [Disconnection] user: one cid: 19 in account one: Client Closed
12:21:00 [Connection] user: one cid: 20 in account one
12:21:00 [Disconnection] user: one cid: 20 in account one: Client Closed
12:21:00 [Connection] user: one cid: 21 in account one
```

### Super Cluster Discovery and Observation

When a cluster or super cluster of NATS servers is configured with a system account a wealth of information is available
via internal APIs, the `nats` tool can interact with these and observe your servers.

A quick view of the available servers and your network RTT to each can be seen with `nats server ping`:

```
nats server ping 
```
Output
```
nc1-c1                                                       rtt=2.30864ms
nc3-c1                                                       rtt=2.396573ms
nc2-c1                                                       rtt=2.484994ms
nc3-c2                                                       rtt=2.549958ms
...

---- ping statistics ----
9 replies max: 3.00 min: 1.00 avg: 2.78
```

A general server overview can be seen with `nats server list`:

```
nats server list 
```
Output
```
+----------------------------------------------------------------------------------------------------------------------------+
|                                                      Server Overview                                                       |
+--------+------------+-----------+---------------+-------+------+--------+-----+---------+-----+------+--------+------------+
| Name   | Cluster    | IP        | Version       | Conns | Subs | Routes | GWs | Mem     | CPU | Slow | Uptime | RTT        |
+--------+------------+-----------+---------------+-------+------+--------+-----+---------+-----+------+--------+------------+
| nc1-c1 | c1         | localhost | 2.2.0-beta.34 | 1     | 97   | 2      | 2   | 13 MiB  | 0.0 | 0    | 5m29s  | 3.371675ms |
| nc2-c1 | c1         | localhost | 2.2.0-beta.34 | 0     | 97   | 2      | 2   | 13 MiB  | 0.0 | 0    | 5m29s  | 3.48287ms  |
| nc3-c1 | c1         | localhost | 2.2.0-beta.34 | 0     | 97   | 2      | 2   | 14 MiB  | 0.0 | 0    | 5m30s  | 3.57123ms  |
| nc1-c3 | c3         | localhost | 2.2.0-beta.34 | 0     | 96   | 2      | 2   | 15 MiB  | 0.0 | 0    | 5m29s  | 3.655548ms |
...
+--------+------------+-----------+---------------+-------+------+--------+-----+---------+-----+------+--------+------------+
|        | 3 Clusters | 9 Servers |               | 1     | 867  |        |     | 125 MiB |     | 0    |        |            |
+--------+------------+-----------+---------------+-------+------+--------+-----+---------+-----+------+--------+------------+

+----------------------------------------------------------------------------+
|                              Cluster Overview                              |
+---------+------------+-------------------+-------------------+-------------+
| Cluster | Node Count | Outgoing Gateways | Incoming Gateways | Connections |
+---------+------------+-------------------+-------------------+-------------+
| c1      | 3          | 6                 | 6                 | 1           |
| c3      | 3          | 6                 | 6                 | 0           |
| c2      | 3          | 6                 | 6                 | 0           |
+---------+------------+-------------------+-------------------+-------------+
|         | 9          | 18                | 18                | 1           |
+---------+------------+-------------------+-------------------+-------------+
```

Data from a specific server can be accessed using it's server name or ID:

```
nats server info nc1-c1 
```
Output
```
Server information for nc1-c1 (NBNIKFCQZ3J6I7JDTUDHAH3Z3HOQYEYGZZ5HOS63BX47PS66NHPT2P72)

Process Details:

         Version: 2.2.0-beta.34
      Git Commit: 2e26d919
      Go Version: go1.14.12
      Start Time: 2020-12-03 12:18:00.423780567 +0000 UTC
          Uptime: 10m1s

Connection Details:

   Auth Required: true
    TLS Required: false
            Host: localhost:10000
     Client URLs: localhost:10000
                  localhost:10002
                  localhost:10001

Limits:

        Max Conn: 65536
        Max Subs: 0
     Max Payload: 1.0 MiB
     TLS Timeout: 2s
  Write Deadline: 10s

Statistics:

       CPU Cores: 2 1.00%
          Memory: 13 MiB
     Connections: 1
   Subscriptions: 0
            Msgs: 240 in 687 out
           Bytes: 151 KiB in 416 KiB out
  Slow Consumers: 0

Cluster:

            Name: c1
            Host: 0.0.0.0:6222
            URLs: nc1:6222
                  nc2:6222
                  nc3:6222

Super Cluster:

            Name: c1
            Host: 0.0.0.0:7222
        Clusters: c1
                  c2
                  c3
```

Additional to this various reports can be generated using `nats server report`, this allows one to list all connections and
subscriptions across the entire cluster with filtering to limit the results by account etc.

Additional raw information in JSON format can be retrieved using the `nats server request` commands.

### Schema Registry

We are adopting JSON Schema to describe the core data formats of events and advisories - as shown by `nats event`. Additionally
all the API interactions with the JetStream API is documented using the same format.

These schemas can be used using tools like [QuickType](https://app.quicktype.io/) to generate stubs for various programming
languages.

The `nats` CLI allows you to view these schemas and validate documents using these schemas.

```
nats schema ls 
```
Output
```
Matched Schemas:

  io.nats.jetstream.advisory.v1.api_audit
  io.nats.jetstream.advisory.v1.consumer_action
  io.nats.jetstream.advisory.v1.max_deliver
...
```

The schemas can be limited using a regular expression, try `nats schema ls request` to see all API requests.

Schemas can be viewed in their raw JSON or YAML formats using `nats schema info io.nats.jetstream.advisory.v1.consumer_action`,
these schemas include descriptions about each field and more.

Finally, if you are interacting with the API using JSON request messages constructed using languages that is not supported
by our own management libraries you can use this tool to validate your messages:

```
nats schema validate io.nats.jetstream.api.v1.stream_create_request request.json 
```
Output
```
Validation errors in request.json:

  retention: retention must be one of the following: "limits", "interest", "workqueue"
  (root): Must validate all the schemas (allOf)

``` 

Here I validate `request.json` against the Schema that describes the API to create Streams, the validation indicates
that I have an incorrect value in the `retention` field.

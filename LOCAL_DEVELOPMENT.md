# Local Development using NATS

We are experimenting with a developer tool to make local development easier.  This tool starts a NATS server in a quick and easy way that does not require knowledge of the NATS Server configuration.

This allows for a quick path to local development and exploration of the NATS eco system.

**NOTE**: This is a preview feature that we are using to experiment ways to improve our user on-boarding.

## Starting a server

Executing `nats server run [name]` will start a local NATS instance ready to work against.  At start it will list it's credentials, url and configuration contextx that can be used to access it

```nohighlight
nats server run

        User Credentials: User: local   Password: Il2OuigEHDJQ7W6aVbQkJZmn4JjFsaDL Context: nats_development
     Service Credentials: User: service Password: SVIkOy7tTZLSOqnveGeSHbunSDa2GxmC Context: nats_development_service
      System Credentials: User: system  Password: 455cwxbiRKf2gcDzbRRp7Kw4zm7pLJlx Context: nats_development_system
                     URL: nats://0.0.0.0:4222
  Extending Demo Network: false
   Extending Remote NATS: false

[32206] [INF] Starting nats-server
[32206] [INF]   Version:  2.6.4-beta.2
[32206] [INF]   Git:      [not set]
[32206] [INF]   Name:     nats_development
[32206] [INF]   ID:       NAFUVLAQIYDRXLGMD7YN22PTZ2QX4I6YVM6UYTGOKQA24PBK7H3QBJ3L
[32206] [INF] Using configuration file: /tmp/nats-server-run-457421452.cfg
[32206] [INF] Listening for client connections on 0.0.0.0:4222
[32206] [INF] Server is ready
```
In a separate terminal run the command below to set up a NATS Service that listens for requests on the `demo` subject:

```nohighlight
$ nats --context nats_development reply demo "[{{Count}}] Response {{Time}}"
```

In another terminal we access the service:

```nohighlight
$ nats --context nats_development request demo ''
```

You can now develop against this NATS instance using the credentials and URL shown using any of our client languages.

## JetStream

[JetStream](https://docs.nats.io/jetstream) can be enabled by passing the `--jetstream` flag, this will create a data directory that matches the instance name.  Future invocations with the same instance name will access the same data.

When extending another network a JetStream Domain is set matching the upper case of the name.

```nohighlight
$ nats server run --jetstream
...
[1584] [INF] Starting JetStream
[1584] [INF]     _ ___ _____ ___ _____ ___ ___   _   __  __
[1584] [INF]  _ | | __|_   _/ __|_   _| _ \ __| /_\ |  \/  |
[1584] [INF] | || | _|  | | \__ \ | | |   / _| / _ \| |\/| |
[1584] [INF]  \__/|___| |_| |___/ |_| |_|_\___/_/ \_\_|  |_|
[1584] [INF]
[1584] [INF]          https://docs.nats.io/jetstream
[1584] [INF]
[1584] [INF] ---------------- JETSTREAM ----------------
[1584] [INF]   Max Memory:      7.21 GB
[1584] [INF]   Max Storage:     8.20 GB
[1584] [INF]   Store Directory: "/home/rip/.local/share/nats/nats_development/jetstream"
[1584] [INF] -------------------------------------------
...
```

Normal JetStream operations can now be performed against this server:

```nohighlight
$ nats account info --context nats_development
Connection Information:

               Client ID: 8
               Client IP: 127.0.0.1
                     RTT: 117.631µs
       Headers Supported: true
         Maximum Payload: 1.0 MiB
           Connected URL: nats://0.0.0.0:4222
       Connected Address: 127.0.0.1:4222
     Connected Server ID: NCV6HAS6B3WLIAOJ4B6VMFJE3HXE3HDWV5OHSXATQWTDHOUKSEWTWYSM
   Connected Server Name: nats_development

JetStream Account Information:

           Memory: 0 B of Unlimited
          Storage: 0 B of Unlimited
          Streams: 0 of Unlimited
        Consumers: 0 of Unlimited
```

## Multiple Accounts

[Accounts](https://docs.nats.io/nats-server/configuration/securing_nats/accounts) is the NATS multi tenancy system that allows different sets of related connections to be isolated from unrelated connections. Accounts can optionally share information or services between them.

The server sets up 3 accounts:

|Account|Description|
|-------|-----------|
|USER   |General access for local development|
|SERVICE|Account for a service with wildcard imports/exports|
|SYSTEM |Account for monitoring purposes|

From the `SERVICE` account we export subjects `service.>` and these are imported into the `USER` account on `imports.SERVICE.>`.

Using this one can experiment with cross account isolation, lets set up a Weather service in the `SERVICE` account:

```nohightlight
$ nats --context nats_development_service reply 'service.weather.>' --command "curl -s wttr.in/{{2}}?format=3"
```

We now have a service listening on `service.weather.>` in the `SERVICE` account.

Let's access the weather across the account boundaries:

```nohighlight
$ nats --context nats_development req 'imports.SERVICE.weather.london' ''
london: ⛅️  +17°C
```

Note we access the service via `imports.SERVICE.weather.london`, we can confirm the isolation provided by subjects by trying to access `service.weather.london` (where the service listens in it's own account) and observe the failure:

```nohighlight
$ nats --context nats_development req 'service.weather.london' ''
nats: error: nats: no responders available for request, try --help
```

We can only access the other account via the import, and we do not have any direct access to it's subjects.

Developers can start their own services on these subject patterns and test cross account behaviors.

## System Account

We enable the [SYSTEM account](https://docs.nats.io/nats-server/configuration/sys_accounts) that is used for monitoring nats, let's see the servers:

```nohighlight
$ nats server list --context nats_development_system
╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│                                                          Server Overview                                                          │
├──────────────────┬────────────┬───────────┬──────────────┬─────┬───────┬──────┬────────┬─────┬────────┬─────┬──────┬────────┬─────┤
│ Name             │ Cluster    │ IP        │ Version      │ JS  │ Conns │ Subs │ Routes │ GWs │ Mem    │ CPU │ Slow │ Uptime │ RTT │
├──────────────────┼────────────┼───────────┼──────────────┼─────┼───────┼──────┼────────┼─────┼────────┼─────┼──────┼────────┼─────┤
│ nats_development │            │ 0.0.0.0   │ 2.6.4-beta.2 │ yes │ 1     │ 51   │ 0      │ 0   │ 13 MiB │ 0.0 │ 0    │ 4m14s  │ 1ms │
├──────────────────┼────────────┼───────────┼──────────────┼─────┼───────┼──────┼────────┼─────┼────────┼─────┼──────┼────────┼─────┤
│                  │ 0 Clusters │ 1 Servers │              │ 1   │ 1     │ 51   │        │     │ 13 MiB │     │ 0    │        │     │
╰──────────────────┴────────────┴───────────┴──────────────┴─────┴───────┴──────┴────────┴─────┴────────┴─────┴──────┴────────┴─────╯
```

A wealther of information is available, try `nats server info nats_development`, `nats server req varz` and more

## Extending other networks

NATS supports extending networks using a technology called [Leaf Nodes](https://docs.nats.io/nats-server/configuration/leafnodes/), we'll show how a remote network can be extended to your Laptop as if it's an edge network.

### Demo Network

We operate a demo NATS network on `demo.nats.io`, we can test the ability of NATS to extend other networks using this tool.

**WARNING**: demo.nats.io is a public network, do not run any services that are of a sensitive nature on it.

Lets extend the `demo.nats.io` network to your local laptop.

```nohighlight
$ nats server run --extend-demo
...
  Extending Demo Network: true
...
```

Now we can run our weather service again, this time in the main account:

```nohightlight
$ nats --context nats_development reply 'myname.weather.>' --command "curl -s wttr.in/{{2}}?format=3"
```

This time we access it on the Demo network:

```nohighlight
$ nats --server demo.nats.io req 'myname.weather.london' ''
london: ⛅️  +15°C 
```

What happened here is that the request for the service went to `demo.nats.io`, while the actual weather service was connected to your laptop. Since our laptop is extending the `demo.nats.io` service communication was possible.

### Synadia NGS

We can extend the Synadia NGS network which would make the above scenario private.

First we need a context that configures access to NGS:

```nohighlight
$ nats context add ngs.leafnode \
   --creds /home/me/.nkeys/creds/synadia/MyAccount/leafnode.creds \
   --server nats://connect.ngs.global:7422 \ 
   --description "NGS Leafnode"
```
We can now run a server using these credentials:

```nohighlight
$ nats server run --extend --context ngs.leafnode
...
   Extending Remote NATS: using ngs.leafnode context
...
```

At this point we've extended the NGS network, (or any other network found in the context), and we can share information, services and more against that network.

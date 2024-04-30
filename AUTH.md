## Evaluation Guide for "nats auth"

The `nats auth` command is a new addition that aims to bring the most common features of `nsc` into
the `nats` command.  The aim is not to have 100% feature parity but rather to provide a easy to use
solution for what most people will use.

It remains compatible with the `nsc` store and so users can switch between these commands.

It is implemented using a new [Go SDK](https://github.com/synadia-io/jwt-auth-builder.go/) allowing
programmatic interaction with the `nsc` store.

This guide is based on the [nsc documentation](https://docs.nats.io/using-nats/nats-tools/nsc) and attempts to 
achieve the same outcome.

## Limitations

 * We only support the `full` resolver type for pushing accounts
 * We do not support import/export activations only account token positions

## Storage

The SDK support both a `nsc` file based storage and a new Key-Value based storage, today in the CLI we only support
the `nsc` compatible storage which for most users means files in `~/.local/share/nats/nsc`.

## Creating an Operator, Account and User

For every operator a system account and signing key is generated. After creation use `nats auth op info MyOperator` to
show the operator settings again: 

```
# nats auth op add MyOperator
Operator MyOperator (OCJDWJ7PYUMG3R7G4RZHWZRLHGMTXDXWNFBVN6TXBRILG5EVGSRW4OUH)

Configuration:

            Name: MyOperator
         Subject: OCJDWJ7PYUMG3R7G4RZHWZRLHGMTXDXWNFBVN6TXBRILG5EVGSRW4OUH
        Accounts: 1
  System Account: SYSTEM (ACSU3XZAMG3HT2OXGXF56YXXPGPCKD3V3RZJYOROIGEKUN2PTPYOEOEY)
    Signing Keys: OC5LYPBJEEYGKPDDF4E4X7DCRS2URYSS77IER3UDXSIY3QUNREBLPALE
```

We add an account, several limits can be set as options, the most common ones are prompted for. After creation use 
`nats auth account info MyAccount` to show the account settings again:

```
# nats auth account add MyAccount
? Maximum Connections -1
? Maximum Subscriptions -1

Account MyAccount (ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF)

Configuration:

                   Name: MyAccount
                 Issuer: OC5LYPBJEEYGKPDDF4E4X7DCRS2URYSS77IER3UDXSIY3QUNREBLPALE
               Operator: MyOperator
         System Account: false
              JetStream: false
                  Users: 0
            Revocations: 0
        Service Exports: 0
         Stream Exports: 0
        Service Imports: 0
         Stream Imports: 0

Limits:

  Bearer Tokens Allowed: false
          Subscriptions: unlimited
            Connections: unlimited
        Maximum Payload: unlimited
              Leafnodes: unlimited
                Imports: unlimited
                Exports: unlimited
```

We add a user, if no account is given you will be presented with a list, again numerous options exist with the most
common ones being prompted. After creation use `nats auth user info MyUser` to show the account settings again:

```
# nats auth user add MyUser
? Select an Account MyAccount
? Maximum Payload -1
? Maximum Subscriptions -1
User MyUser (UD6LMPVXNZOBXHBTWBHWY3LTCXYUCTYDD3GSYCZS6TVERC3NVU6TGVAG)

Configuration:

            Account: MyAccount (ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF)
             Issuer: ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF
             Scoped: false
       Bearer Token: false

Limits:

        Max Payload: unlimited
           Max Data: unlimited
  Max Subscriptions: unlimited

Permissions:

  Publish:

   No permissions defined

  Subscribe:

   No permissions defined
```

## Configuring NATS Server

TODO: Full resolver generate

```
$ nats-server --config nats-server.conf
...
[31] 2024/04/23 09:49:20.430678 [INF] Trusted Operators
[31] 2024/04/23 09:49:20.430681 [INF]   System  : ""
[31] 2024/04/23 09:49:20.430684 [INF]   Operator: "MyOperator"
[31] 2024/04/23 09:49:20.430687 [INF]   Issued  : 2024-04-23 09:33:43 +0000 UTC
[31] 2024/04/23 09:49:20.430708 [INF]   Expires : Never
```

The dynamic account - `MyAccount` in this case - needs to be sent to the NATS Server `SYSTEM` account before you can use it.
To do this we need a `SYSTEM` account user and credential.

Unlike `nsc` that makes credentials on demand to push to the servers we use contexts to authenticate to the system account
which feels more in-line with how the `nats` command works. We need a user and credentials for a system user, if we do 
not supply the username or account they will be prompted for:

```
# nats auth user add admin SYSTEM
# nats auth user cred system.cred admin SYSTEM
Wrote credential for admin to /path/to/system.cred
# nats ctx add system --description "System Account" --server localhost:4222 --creds /path/to/system.cred
```

Next we use the credential we just made to push to the server:

```
# nats auth account push MyAccount --context system
Updating account MyAccount (ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF) on 1 server(s)

✓ Update completed on servertest

Success 1 Failed 0 Expected 1
```

## User Authorization

To do a basic test of publish / subscribe we can use the user we created earlier:

```
# nats auth user cred myuser.cred
? Select an Account MyAccount
? Select a User MyUser
Wrote credential for MyUser to myuser.cred
```

First we setup a responder:
```
# nats reply echo --echo --creds myuser.cred
```

And in a another terminal we communicate with it:

```
# nats req echo x --creds myuser.cred
10:20:41 Sending request on "echo"
10:20:41 Received with rtt 627.799µs
x
```

Now lets lock the requester and responder down, we'll add a user for the service with appropriate permissions.

First the user for the service and in the same command we save its credential:

```
# nats auth user add echosrv --sub-allow echo --pub-allow '_INBOX.>' --credential service.cred 
# nats reply echo --echo  --creds service.cred
10:31:53 Listening on "echo" in group "NATS-RPLY-22"
```

Next we update the `MyUser` so he can only talk to the echo service and we save the credential and then test the access:

```
# nats auth user edit --pub-allow 'echo' --sub-allow '_INBOX.>' --credential myuser.cred
...
Permissions:

  Publish:

                Allow: echo

  Subscribe:

                Allow: _INBOX.>

# nats req echo x --creds myuser.cred
10:36:10 Sending request on "echo"
10:36:10 Received with rtt 767.011µs
10:36:10 NATS-Reply-Counter: 0

x
```

And we confirm the limits are applied by subscribing to an invalid subject:

```
# nats req other x --creds myuser.cred
10:36:36 Sending request on "other"
10:36:36 Unexpected NATS error from server nats://127.0.0.1:4222: nats: Permissions Violation for Publish to "other"
```

## Imports and Exports

For the examples below we'll add an account and some users with their credentials:

```
# nats auth acct add OTHER --defaults
# nats auth user add test MyAccount --credential myaccount.cred --defaults
# nats auth user add test OTHER --credential other.cred --defaults
```

This creates the `OTHER` account and adds credentials files `myaccount.cred` and `other.cred`.

### Sharing NATS Core Streams

Note: A `Stream` in this context describes a one directional flow of nats-core messages, not a Stream stored in JetStream.

To share data cross accounts the origin account has to export it:

```
# nats auth a export add abc "a.b.c.>" 
? Select an Account: MyAccount
Export info for abc exporting a.b.c.>

Configuration:

                    Name: abc
                 Subject: a.b.c.>
     Activation Required: false
  Account Token Position: 0
              Advertised: false

Revocations:

 No revocations found
```

The other side needs to import it:

```
# nats auth a import add "MyAccount abc" "a.b.c.>" OTHER
? Select the Source account ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF
Import info for MyAccount abc importing a.b.c.>

Configuration:

                     Name: MyAccount abc
             From Account: (MyAccount) ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF
            Local Subject: a.b.c.>
           Remote Subject: a.b.c.>
  Sharing Connection Info: false
   Allows Message Tracing: false
```

We have to push these accounts to the servers now since we changed the account JWTs:

```
# nats auth account push MyAccount --context system
# nats auth account push OTHER --context system
```

In one terminal we can publish messages using the `MyAccount` user:

```
# nats pub a.b.c.d '{{.Count}}' --count 100 --sleep 1s --creds myaccount.cred
```

And in another terminal we can receive them on the `OTHER` account user:

```
# nats sub a.b.c.d --creds other.cred
12:38:02 Subscribing on a.b.c.d
[#1] Received on "a.b.c.d"
14
...
```

### Sharing NATS Core Services

To share data cross accounts the origin account has to export it, the example will be a weather service:

```
# nats auth account export add weather.v1 'weather.v1.>' --service
? Select an Account MyAccount
Export info for weather.v1 exporting weather.v1.>

Configuration:

                    Name: weather.v1
                 Subject: weather.v1.>
     Activation Required: false
  Account Token Position: 0
              Advertised: false

Revocations:

 No revocations found
```

The other account needs to import it, we import the `weather.v1.>` subjects to `weather.>` in the `OTHER` account:

```
# nats auth account import add weather 'weather.v1.>' --local 'weather.>' --service
? Select an Account OTHER
? Select the Source account ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF
Import info for import "weather" importing "weather.>"

Configuration:

                     Name: weather
             From Account: MyAccount (ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF)
            Local Subject: weather.>
           Remote Subject: weather.v1.>
  Sharing Connection Info: false
```

We have to push the accounts since we edited the JWTs:

```
# nats auth account push MyAccount --context system
# nats auth account push OTHER --context system
```

To test it we set up a weather service in the `MyAccount`, note we listen on `weather.v1.>`:

```
# nats reply 'weather.v1.>' --command "curl -s wttr.in/{{2}}?format=3" --creds myaccount.cred 
```

On the importing account we access the service via `weather.>`:

```
# nats req weather.malta '' --creds other.cred
08:53:45 Sending request on "weather.malta"
08:53:45 Received with rtt 228.858489ms
malta: ⛅️  +17°C
```

## Account token authentication for imports and exports

`nats auth` supports securing Exports by forcing account tokens in subjects.  Let's create a weather export that will
require the account ID in imports:

```
$ nats auth account export add weatherAPI 'weather.*.>' MyAccount --token-position 2 --service
Export info for weatherAPI exporting weather.*.>

Configuration:

                    Name: weatherAPI
                    Kind: Service
                 Subject: weather.*.>
     Activation Required: false
  Account Token Position: 2
              Advertised: false

Revocations:

 No revocations found
 
$ nats auth account push USERS  --creds admin.cred
```

Now we create an account and import the service:

```
$ nats auth account add CLIENT --defaults
Account CLIENT (ABAFX7CGEL5Z63ZYSR6VZX67VBXCK5BFXAGKLRTE4KEDDLX6BSTYM3ET)

Configuration:

                   Name: CLIENT
                 Issuer: OCBIIO6GFBV4C3O66WNYOALWLQKEW3RNLXYRDFXLISNLNUK6BIYA6HGP
               Operator: DEMO
         System Account: false
              JetStream: false
                  Users: 1
            Revocations: 0
        Service Exports: 0
         Stream Exports: 0
        Service Imports: 0
         Stream Imports: 1

Limits:

  Bearer Tokens Allowed: false
          Subscriptions: unlimited
            Connections: unlimited
        Maximum Payload: unlimited
              Leafnodes: unlimited
                Imports: unlimited
                Exports: unlimited
```

Note the account ID here is `ABAFX7CGEL5Z63ZYSR6VZX67VBXCK5BFXAGKLRTE4KEDDLX6BSTYM3ET`, this means when importing the
service we must add this token in the subject:

```
$ nats auth account import add weatherAPI 'weather.ABAFX7CGEL5Z63ZYSR6VZX67VBXCK5BFXAGKLRTE4KEDDLX6BSTYM3ET.>' --local 'weather.>' CLIENT --service
$ nats auth user add user --credential client.cred CLIENT --defaults
$ nats auth account push CLIENT --creds admin.cred

? Select the Source account ACJFET46AFRLXBHFTHRTCB3NQR2KZAWUH66HXUYVEUU4I2425SQBZKOU
Import info for import "weatherAPI" importing "weather.>"

Configuration:

                     Name: weatherAPI
             From Account: MyAccount (ADNJMEHSIAQAE3X5EKGABVVOFX3GECHZKG6M4DK72ZDHEPQ5YW4CA3OF)
            Local Subject: weather.>
                     Kind: Service
           Remote Subject: weather.ABAFX7CGEL5Z63ZYSR6VZX67VBXCK5BFXAGKLRTE4KEDDLX6BSTYM3ET.>
  Sharing Connection Info: false
```

We can now run the weather service in the one account:

```
$ nats reply 'weather.*.>' --command "curl -s wttr.in/{{3}}?format=3" --creds myaccount.cred
```

And in another we can access the service:

```
$ nats req weather.lon '' --creds client.cred
12:26:18 Sending request on "weather.lon"
12:26:19 Received with rtt 686.31243ms
3: ⛅️  +13°C
```

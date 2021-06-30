package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/nats-io/jsm.go/kv"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type kvCommand struct {
	bucket   string
	key      string
	val      string
	raw      bool
	asJson   bool
	share    bool
	history  uint
	ttl      time.Duration
	replicas uint
	force    bool
	cluster  string
}

func configureKVCommand(app *kingpin.Application) {
	c := &kvCommand{}

	help := `Interacts with a JetStream based Key-Value store

The JetStream Key-Value store uses streams to store key-value pairs
for an indefinite period or a per-bucket configured TTL.

The Key-Value store supports read-after-write safety when not using
any caches or read replicas.

NOTE: This is an experimental feature.
`

	kv := app.Command("kv", help)

	get := kv.Command("get", "Gets a value for a key").Action(c.getAction)
	get.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	get.Arg("key", "The key to act on").Required().StringVar(&c.key)
	get.Flag("raw", "Show only the value string").BoolVar(&c.raw)

	put := kv.Command("put", "Puts a value into a key").Action(c.putAction)
	put.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	put.Arg("key", "The key to act on").Required().StringVar(&c.key)
	put.Arg("value", "The value to store, when empty reads STDIN").StringVar(&c.val)
	put.Flag("share", "Store client details in the value").Default("true").BoolVar(&c.share)

	del := kv.Command("del", "Deletes a key from the bucket").Action(c.deleteAction)
	del.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	del.Arg("key", "The key to act on").Required().StringVar(&c.key)
	del.Flag("force", "Act without confirmation").BoolVar(&c.force)

	history := kv.Command("history", "Shows the full history for a key").Action(c.historyAction)
	history.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	history.Arg("key", "The key to act on").Required().StringVar(&c.key)

	add := kv.Command("add", "Adds a new KV store").Alias("new").Action(c.addAction)
	add.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	add.Flag("history", "How many historic values to keep per key").Default("1").UintVar(&c.history)
	add.Flag("ttl", "How long to keep values for").DurationVar(&c.ttl)
	add.Flag("replicas", "How many replicas of the data to store").Default("1").UintVar(&c.replicas)
	add.Flag("cluster", "Place the bucket in a specific cluster").StringVar(&c.cluster)

	status := kv.Command("status", "View the status of a KV store").Alias("view").Action(c.statusAction)
	status.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	status.Flag("json", "Shows the status in JSON format").BoolVar(&c.asJson)

	watch := kv.Command("watch", "Watch the bucket or a specific key for updated").Action(c.watchAction)
	watch.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	watch.Arg("key", "The key to act on").StringVar(&c.key)

	dump := kv.Command("dump", "Dumps the contents of the bucket as JSON").Action(c.dumpAction)
	dump.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)

	purge := kv.Command("purge", "Removes values from the bucket").Action(c.purgeAction)
	purge.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	purge.Flag("force", "Act without confirmation").BoolVar(&c.force)

	rm := kv.Command("rm", "Removes a bucket").Action(c.rmAction)
	rm.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	rm.Flag("force", "Act without confirmation").BoolVar(&c.force)

	cheats["kv"] = `# to create a replicated KV bucket
nats kv add CONFIG --replicas 3

# to store a value in the bucket
nats kv put CONFIG username bob

# to read just the value with no additional details
nats kv get CONFIG username --raw

# to see all values in the bucket
nats kv dump CONFIG

# to see the bucket status
nats kv status CONFIG
`
}

func (c *kvCommand) historyAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	history, err := store.History(context.Background(), c.key)
	if err != nil {
		return err
	}

	table := newTableWriter(fmt.Sprintf("History for %s.%s", c.bucket, c.key))
	table.AddHeaders("Seq / Op", "Created", "Value")
	for _, r := range history {
		table.AddRow(fmt.Sprintf("%d / %s", r.Sequence(), r.Operation()), r.Created().Format(time.RFC822), r.Value())
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) deleteAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Delete key %s from bucket %s", c.key, c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	return store.Delete(c.key)
}

func (c *kvCommand) addAction(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	store, err := kv.NewBucket(nc, c.bucket, kv.WithTTL(c.ttl), kv.WithHistory(c.history), kv.WithReplicas(c.replicas), kv.WithPlacementCluster(c.cluster))
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) getAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	res, err := store.Get(c.key)
	if err != nil {
		return err
	}

	if c.raw {
		fmt.Println(res.Value())
		return nil
	}

	fmt.Printf("%s.%s created @ %s\n", res.Bucket(), res.Key(), res.Created().Format(time.RFC822))
	fmt.Println()
	fmt.Println(res.Value())

	return nil
}

func (c *kvCommand) putAction(_ *kingpin.ParseContext) error {
	if c.val == "" {
		val, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		c.val = string(val)
	}

	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	_, err = store.Put(c.key, c.val)

	fmt.Println(c.val)
	return err
}

func (c *kvCommand) loadBucket() (*nats.Conn, kv.KV, error) {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return nil, nil, err
	}

	store, err := kv.NewBucket(nc, c.bucket)
	if err != nil {
		return nil, nil, err
	}

	return nc, store, err
}

func (c *kvCommand) statusAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) watchAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	var watch kv.Watch
	if c.key != "" {
		watch, err = store.Watch(context.Background(), c.key)
	} else {
		watch, err = store.WatchBucket(context.Background())
	}
	if err != nil {
		return err
	}
	defer watch.Close()

	for res := range watch.Channel() {
		if res != nil {
			if res.Operation() == kv.DeleteOperation {
				fmt.Printf("[%s] %s %s.%s\n", res.Created().Format("2006-01-02 15:04:05"), color.RedString("DEL"), res.Bucket(), res.Key())
			} else {
				fmt.Printf("[%s] %s %s.%s: %s\n", res.Created().Format("2006-01-02 15:04:05"), color.GreenString("PUT"), res.Bucket(), res.Key(), res.Value())
			}
		}
	}

	return nil
}

func (c *kvCommand) dumpAction(_ *kingpin.ParseContext) error {
	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	j, err := store.JSON(context.Background())
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func (c *kvCommand) purgeAction(_ *kingpin.ParseContext) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Purge bucket %s", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping purge")
			return nil
		}
	}

	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return store.Purge()
}

func (c *kvCommand) rmAction(_ *kingpin.ParseContext) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Deleted bucket %s", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	_, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return store.Destroy()
}

func (c *kvCommand) showStatus(store kv.KV) error {
	status, err := store.Status()
	if err != nil {
		return err
	}

	fmt.Println("Key-Value Store Status")
	fmt.Println()
	fmt.Printf("        Bucket Name: %s\n", c.bucket)
	fmt.Printf("       History Kept: %d\n", status.History())

	ok, failed := status.Replicas()
	fmt.Printf("      Data Replicas: ok %d failed: %d\n", ok, failed)
	if status.Cluster() != "" {
		fmt.Printf("            Cluster: %s\n", status.Cluster())
	}
	fmt.Printf("      Values Stored: %d\n", status.Values())
	fmt.Printf(" Backing Store Name: %s\n", status.BackingStore())

	return nil
}

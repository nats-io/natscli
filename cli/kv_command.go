// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type kvCommand struct {
	bucket        string
	key           string
	val           string
	raw           bool
	history       uint64
	ttl           time.Duration
	replicas      uint
	force         bool
	maxValueSize  int32
	maxBucketSize int64
	revision      uint64
	description   string
	listNames     bool
	storage       string
}

func configureKVCommand(app commandHost) {
	c := &kvCommand{}

	help := `Interacts with a JetStream based Key-Value store

The JetStream Key-Value store uses streams to store key-value pairs
for an indefinite period or a per-bucket configured TTL.

The Key-Value store supports read-after-write safety.

NOTE: This is an experimental feature.
`

	kv := app.Command("kv", help)

	add := kv.Command("add", "Adds a new KV Store Bucket").Alias("new").Action(c.addAction)
	add.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	add.Flag("history", "How many historic values to keep per key").Default("1").Uint64Var(&c.history)
	add.Flag("ttl", "How long to keep values for").DurationVar(&c.ttl)
	add.Flag("replicas", "How many replicas of the data to store").Default("1").UintVar(&c.replicas)
	add.Flag("max-value-size", "Maximum size for any single value").Int32Var(&c.maxValueSize)
	add.Flag("max-bucket-size", "Maximum size for the bucket").Int64Var(&c.maxBucketSize)
	add.Flag("description", "A description for the bucket").StringVar(&c.description)
	add.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")

	put := kv.Command("put", "Puts a value into a key").Action(c.putAction)
	put.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	put.Arg("key", "The key to act on").Required().StringVar(&c.key)
	put.Arg("value", "The value to store, when empty reads STDIN").StringVar(&c.val)

	get := kv.Command("get", "Gets a value for a key").Action(c.getAction)
	get.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	get.Arg("key", "The key to act on").Required().StringVar(&c.key)
	get.Flag("raw", "Show only the value string").BoolVar(&c.raw)

	create := kv.Command("create", "Puts a value into a key only if the key is new or it's last operation was a delete").Action(c.createAction)
	create.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	create.Arg("key", "The key to act on").Required().StringVar(&c.key)
	create.Arg("value", "The value to store, when empty reads STDIN").StringVar(&c.val)

	update := kv.Command("update", "Updates a key with a new value if the previous value matches the given revision").Action(c.updateAction)
	update.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	update.Arg("key", "The key to act on").Required().StringVar(&c.key)
	update.Arg("value", "The value to store, when empty reads STDIN").StringVar(&c.val)
	update.Arg("revision", "The revision of the previous value in the bucket").Uint64Var(&c.revision)

	del := kv.Command("del", "Deletes a key or the entire bucket").Alias("rm").Action(c.deleteAction)
	del.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	del.Arg("key", "The key to act on").StringVar(&c.key)
	del.Flag("force", "Act without confirmation").Short('f').BoolVar(&c.force)

	purge := kv.Command("purge", "Deletes a key from the bucket, clearing history before creating a delete marker").Action(c.purgeAction)
	purge.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	purge.Arg("key", "The key to act on").Required().StringVar(&c.key)
	purge.Flag("force", "Act without confirmation").Short('f').BoolVar(&c.force)

	history := kv.Command("history", "Shows the full history for a key").Action(c.historyAction)
	history.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	history.Arg("key", "The key to act on").Required().StringVar(&c.key)

	status := kv.Command("status", "View the status of a KV store").Alias("view").Alias("info").Action(c.statusAction)
	status.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)

	watch := kv.Command("watch", "Watch the bucket or a specific key for updated").Action(c.watchAction)
	watch.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	watch.Arg("key", "The key to act on").StringVar(&c.key)

	ls := kv.Command("ls", "List available Buckets").Alias("list").Action(c.lsAction)
	ls.Flag("names", "Show just the bucket names").Short('n').BoolVar(&c.listNames)

	rmHistory := kv.Command("compact", "Removes all historic values from the store where the last value is a delete").Action(c.compactAction)
	rmHistory.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	rmHistory.Flag("force", "Act without confirmation").Short('f').BoolVar(&c.force)

	upgrade := kv.Command("upgrade", "Upgrades a early tech-preview bucket to current format").Action(c.upgradeAction)
	upgrade.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)

	cheats["kv"] = `# to create a replicated KV bucket
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
`

}

func init() {
	registerCommand("kv", 9, configureKVCommand)
}

func (c *kvCommand) strForOp(op nats.KeyValueOp) string {
	switch op {
	case nats.KeyValuePut:
		return "PUT"
	case nats.KeyValuePurge:
		return "PURGE"
	case nats.KeyValueDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func (c *kvCommand) lsAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	var found []*jsm.Stream
	err = mgr.EachStream(func(s *jsm.Stream) {
		if s.IsKVBucket() {
			found = append(found, s)
		}
	})
	if err != nil {
		return err
	}

	if len(found) == 0 {
		fmt.Println("No Key-Value buckets found")
		return nil
	}

	if c.listNames {
		for _, s := range found {
			fmt.Println(strings.TrimPrefix(s.Name(), "KV_"))
		}
		return nil
	}

	sort.Slice(found, func(i, j int) bool {
		info, _ := found[i].LatestInformation()
		jnfo, _ := found[j].LatestInformation()

		return info.State.Bytes < jnfo.State.Bytes
	})

	table := newTableWriter("Key-Value Buckets")
	table.AddHeaders("Bucket", "Description", "Created", "Size", "Values", "Last Update")
	for _, s := range found {
		nfo, _ := s.LatestInformation()

		table.AddRow(strings.TrimPrefix(s.Name(), "KV_"), s.Description(), nfo.Created.Format("2006-01-02 15:01:05"), nfo.State.Bytes, humanize.IBytes(nfo.State.Msgs), humanizeDuration(time.Since(nfo.State.LastTime)))
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) upgradeAction(_ *kingpin.ParseContext) error {
	_, js, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	status, err := store.Status()
	if err != nil {
		return err
	}

	nfo := status.(*nats.KeyValueBucketStatus).StreamInfo()
	if nfo.Config.AllowRollup && nfo.Config.Discard == nats.DiscardNew {
		fmt.Println("Bucket is already using the correct configuration")
		os.Exit(1)
	}

	nfo.Config.AllowRollup = true
	nfo.Config.Discard = nats.DiscardNew
	nfo, err = js.UpdateStream(&nfo.Config)
	if err != nil {
		return err
	}

	if !nfo.Config.AllowRollup || nfo.Config.Discard != nats.DiscardNew {
		fmt.Printf("Configuration upgrade failed, please edit stream %s to allow RollUps and have Discard Policy of New", nfo.Config.Name)
		os.Exit(1)
	}

	fmt.Printf("Bucket %s has been upgraded\n\n", status.Bucket())

	return c.showStatus(store)
}

func (c *kvCommand) historyAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	history, err := store.History(c.key)
	if err != nil {
		return err
	}

	table := newTableWriter(fmt.Sprintf("History for %s > %s", c.bucket, c.key))
	table.AddHeaders("Key", "Revision", "Op", "Created", "Length", "Value")
	for _, r := range history {
		val := base64IfNotPrintable(r.Value())
		if len(val) > 40 {
			val = fmt.Sprintf("%s...%s", val[0:15], val[len(val)-15:])
		}

		table.AddRow(r.Key(), r.Revision(), c.strForOp(r.Operation()), r.Created().Format(time.RFC822), humanize.Comma(int64(len(r.Value()))), val)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) compactAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Purge all historic values and audit trails for deleted keys in bucket %s?", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	return store.PurgeDeletes()
}

func (c *kvCommand) deleteAction(pc *kingpin.ParseContext) error {
	if c.key == "" {
		return c.rmBucketAction(pc)
	}

	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Delete key %s > %s?", c.bucket, c.key), false)
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
	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	storage := nats.FileStorage
	if strings.HasPrefix(c.storage, "m") {
		storage = nats.MemoryStorage
	}

	store, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       c.bucket,
		Description:  c.description,
		MaxValueSize: c.maxValueSize,
		History:      uint8(c.history),
		TTL:          c.ttl,
		MaxBytes:     c.maxBucketSize,
		Storage:      storage,
		Replicas:     int(c.replicas),
	})
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) getAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	res, err := store.Get(c.key)
	if err != nil {
		return err
	}

	if c.raw {
		os.Stdout.Write(res.Value())
		return nil
	}

	fmt.Printf("%s > %s created @ %s\n", res.Bucket(), res.Key(), res.Created().Format(time.RFC822))
	fmt.Println()
	pv := base64IfNotPrintable(res.Value())
	lpv := len(pv)
	if len(pv) > 120 {
		fmt.Printf("Showing first 120 bytes of %s, use --raw for full data\n\n", humanize.Comma(int64(lpv)))
		fmt.Println(pv[:120])
	} else {
		fmt.Println(base64IfNotPrintable(res.Value()))
	}

	fmt.Println()

	return nil
}

func (c *kvCommand) putAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	_, err = store.Put(c.key, val)
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) createAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	_, err = store.Create(c.key, val)
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) updateAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	val, err := c.valOrReadVal()
	if err != nil {
		return err
	}

	_, err = store.Update(c.key, val, c.revision)
	if err != nil {
		return err
	}

	fmt.Println(c.val)

	return err
}

func (c *kvCommand) valOrReadVal() ([]byte, error) {
	if c.val != "" {
		return []byte(c.val), nil
	}

	return ioutil.ReadAll(os.Stdin)
}

func (c *kvCommand) loadBucket() (*nats.Conn, nats.JetStreamContext, nats.KeyValue, error) {
	nc, js, err := prepareJSHelper()
	if err != nil {
		return nil, nil, nil, err
	}

	store, err := js.KeyValue(c.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	return nc, js, store, err
}

func (c *kvCommand) statusAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) watchAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	watch, err := store.Watch(c.key)
	if err != nil {
		return err
	}
	defer watch.Stop()

	for res := range watch.Updates() {
		if res == nil {
			continue
		}

		switch res.Operation() {
		case nats.KeyValueDelete:
			fmt.Printf("[%s] %s %s > %s\n", res.Created().Format("2006-01-02 15:04:05"), color.RedString(c.strForOp(res.Operation())), res.Bucket(), res.Key())
		case nats.KeyValuePurge:
			fmt.Printf("[%s] %s %s > %s\n", res.Created().Format("2006-01-02 15:04:05"), color.RedString(c.strForOp(res.Operation())), res.Bucket(), res.Key())
		case nats.KeyValuePut:
			fmt.Printf("[%s] %s %s > %s: %s\n", res.Created().Format("2006-01-02 15:04:05"), color.GreenString(c.strForOp(res.Operation())), res.Bucket(), res.Key(), res.Value())
		}
	}

	return nil
}

func (c *kvCommand) purgeAction(_ *kingpin.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Purge key %s > %s?", c.bucket, c.key), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping purge")
			return nil
		}
	}

	return store.Purge(c.key)
}

func (c *kvCommand) rmBucketAction(_ *kingpin.ParseContext) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Deleted bucket %s?", c.bucket), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}
	}

	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	return js.DeleteKeyValue(c.bucket)
}

func (c *kvCommand) showStatus(store nats.KeyValue) error {
	status, err := store.Status()
	if err != nil {
		return err
	}

	fmt.Printf("%s Key-Value Store Status\n", status.Bucket())
	fmt.Println()
	fmt.Printf("         Bucket Name: %s\n", status.Bucket())
	fmt.Printf("        History Kept: %d\n", status.History())
	fmt.Printf("       Values Stored: %d\n", status.Values())
	fmt.Printf("  Backing Store Kind: %s\n", status.BackingStore())

	if status.BackingStore() == "JetStream" {
		nfo := status.(*nats.KeyValueBucketStatus).StreamInfo()
		if nfo.Config.Description != "" {
			fmt.Printf("         Description: %s\n", nfo.Config.Description)
		}
		if nfo.Config.MaxBytes == -1 {
			fmt.Printf(" Maximum Bucket Size: unlimited\n")
		} else {
			fmt.Printf(" Maximum Bucket Size: %d\n", nfo.Config.MaxBytes)
		}
		if nfo.Config.MaxBytes == -1 {
			fmt.Printf("  Maximum Value Size: unlimited\n")
		} else {
			fmt.Printf("  Maximum Value Size: %d\n", nfo.Config.MaxMsgSize)
		}
		fmt.Printf("    JetStream Stream: %s\n", nfo.Config.Name)
		if nfo.Cluster != nil {
			fmt.Printf("    Cluster Location: %s\n", nfo.Cluster.Name)
		}
		fmt.Printf("            Storage: %s\n", nfo.Config.Storage.String())

		if !nfo.Config.AllowRollup {
			fmt.Println()
			fmt.Println("Warning the bucket does not support roll-ups")
			fmt.Println("and needs a configuration upgrade.")
			fmt.Println()
			fmt.Printf("Please run: nats kv upgrade %s\n\n", status.Bucket())
		}
	}

	return nil
}

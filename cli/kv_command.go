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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
)

type kvCommand struct {
	bucket                string
	key                   string
	val                   string
	raw                   bool
	history               uint64
	ttl                   time.Duration
	replicas              uint
	force                 bool
	maxValueSize          int64
	maxValueSizeString    string
	maxBucketSize         int64
	maxBucketSizeString   string
	revision              uint64
	description           string
	listNames             bool
	lsVerbose             bool
	lsVerboseDisplayValue bool
	storage               string
	placementCluster      string
	placementTags         []string
	repubSource           string
	repubDest             string
	repubHeadersOnly      bool
	mirror                string
	mirrorDomain          string
}

func configureKVCommand(app commandHost) {
	c := &kvCommand{}

	help := `Interacts with a JetStream based Key-Value store

The JetStream Key-Value store uses streams to store key-value pairs
for an indefinite period or a per-bucket configured TTL.
`

	kv := app.Command("kv", help)
	addCheat("kv", kv)

	add := kv.Command("add", "Adds a new KV Store Bucket").Alias("new").Action(c.addAction)
	add.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	add.Flag("history", "How many historic values to keep per key").Default("1").Uint64Var(&c.history)
	add.Flag("ttl", "How long to keep values for").DurationVar(&c.ttl)
	add.Flag("replicas", "How many replicas of the data to store").Default("1").UintVar(&c.replicas)
	add.Flag("max-value-size", "Maximum size for any single value").PlaceHolder("BYTES").StringVar(&c.maxValueSizeString)
	add.Flag("max-bucket-size", "Maximum size for the bucket").PlaceHolder("BYTES").StringVar(&c.maxBucketSizeString)
	add.Flag("description", "A description for the bucket").StringVar(&c.description)
	add.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
	add.Flag("tags", "Place the bucket on servers that has specific tags").StringsVar(&c.placementTags)
	add.Flag("cluster", "Place the bucket on a specific cluster").StringVar(&c.placementCluster)
	add.Flag("republish-source", "Republish messages to --republish-destination").PlaceHolder("SRC").StringVar(&c.repubSource)
	add.Flag("republish-destination", "Republish destination for messages in --republish-source").PlaceHolder("DEST").StringVar(&c.repubDest)
	add.Flag("republish-headers", "Republish only message headers, no bodies").UnNegatableBoolVar(&c.repubHeadersOnly)
	add.Flag("mirror", "Creates a mirror of a different bucket").StringVar(&c.mirror)
	add.Flag("mirror-domain", "When mirroring find the bucket in a different domain").StringVar(&c.mirrorDomain)

	add.PreAction(c.parseLimitStrings)

	put := kv.Command("put", "Puts a value into a key").Action(c.putAction)
	put.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	put.Arg("key", "The key to act on").Required().StringVar(&c.key)
	put.Arg("value", "The value to store, when empty reads STDIN").StringVar(&c.val)

	get := kv.Command("get", "Gets a value for a key").Action(c.getAction)
	get.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	get.Arg("key", "The key to act on").Required().StringVar(&c.key)
	get.Flag("revision", "Gets a specific revision").Uint64Var(&c.revision)
	get.Flag("raw", "Show only the value string").UnNegatableBoolVar(&c.raw)

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
	del.Flag("force", "Act without confirmation").Short('f').UnNegatableBoolVar(&c.force)

	purge := kv.Command("purge", "Deletes a key from the bucket, clearing history before creating a delete marker").Action(c.purgeAction)
	purge.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	purge.Arg("key", "The key to act on").Required().StringVar(&c.key)
	purge.Flag("force", "Act without confirmation").Short('f').UnNegatableBoolVar(&c.force)

	history := kv.Command("history", "Shows the full history for a key").Action(c.historyAction)
	history.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	history.Arg("key", "The key to act on").Required().StringVar(&c.key)

	revert := kv.Command("revert", "Reverts a value to a previous revision using put").Action(c.revertAction)
	revert.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	revert.Arg("key", "The key to act on").Required().StringVar(&c.key)
	revert.Arg("revision", "The revision to revert to").Required().Uint64Var(&c.revision)
	revert.Flag("force", "Force reverting without prompting").BoolVar(&c.force)

	status := kv.Command("info", "View the status of a KV store").Alias("view").Alias("status").Action(c.infoAction)
	status.Arg("bucket", "The bucket to act on").StringVar(&c.bucket)

	watch := kv.Command("watch", "Watch the bucket or a specific key for updated").Action(c.watchAction)
	watch.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	watch.Arg("key", "The key to act on").Default(">").StringVar(&c.key)

	ls := kv.Command("ls", "List available buckets or the keys in a bucket").Alias("list").Action(c.lsAction)
	ls.Arg("bucket", "The bucket to list the keys").StringVar(&c.bucket)
	ls.Flag("names", "Show just the bucket names").Short('n').UnNegatableBoolVar(&c.listNames)
	ls.Flag("verbose", "Show detailed info about the key").Short('v').UnNegatableBoolVar(&c.lsVerbose)
	ls.Flag("display-value", "Display value in verbose output (has no effect without 'verbose')").UnNegatableBoolVar(&c.lsVerboseDisplayValue)

	rmHistory := kv.Command("compact", "Reclaim space used by deleted keys").Action(c.compactAction)
	rmHistory.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	rmHistory.Flag("force", "Act without confirmation").Short('f').UnNegatableBoolVar(&c.force)
}

func init() {
	registerCommand("kv", 9, configureKVCommand)
}

func (c *kvCommand) parseLimitStrings(_ *fisk.ParseContext) (err error) {
	if c.maxValueSizeString != "" {
		c.maxValueSize, err = parseStringAsBytes(c.maxValueSizeString)
		if err != nil {
			return err
		}
	}

	if c.maxBucketSizeString != "" {
		c.maxBucketSize, err = parseStringAsBytes(c.maxBucketSizeString)
		if err != nil {
			return err
		}
	}

	return nil
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

func (c *kvCommand) lsAction(_ *fisk.ParseContext) error {
	if c.bucket != "" {
		return c.lsBucketKeys()
	}

	return c.lsBuckets()
}

func (c *kvCommand) lsBucketKeys() error {
	_, js, err := prepareJSHelper()
	if err != nil {
		return fmt.Errorf("unable to prepare js helper: %s", err)
	}

	kv, err := js.KeyValue(c.bucket)
	if err != nil {
		return fmt.Errorf("unable to load bucket: %s", err)
	}

	keys, err := kv.Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			fmt.Println("No keys found in bucket")
			return nil
		}

		return fmt.Errorf("unable to fetch keys in bucket: %s", err)
	}

	if c.lsVerbose {
		if err := c.displayKeyInfo(kv, keys); err != nil {
			return fmt.Errorf("unable to display key info: %s", err)
		}
	} else {
		for _, v := range keys {
			fmt.Println(v)
		}
	}

	return nil
}

func (c *kvCommand) displayKeyInfo(kv nats.KeyValue, keys []string) error {
	if kv == nil {
		return errors.New("key value cannot be nil")
	}

	table := newTableWriter(fmt.Sprintf("Contents for bucket '%s'", c.bucket))

	if c.lsVerboseDisplayValue {
		table.AddHeaders("Key", "Created", "Delta", "Revision", "Value")
	} else {
		table.AddHeaders("Key", "Created", "Delta", "Revision")
	}

	for _, keyName := range keys {
		kve, err := kv.Get(keyName)
		if err != nil {
			return fmt.Errorf("unable to fetch key %s: %s", keyName, err)
		}

		row := []interface{}{
			kve.Key(),
			kve.Created().Format("2006-01-02 15:04:05"),
			kve.Delta(),
			kve.Revision(),
		}

		if c.lsVerboseDisplayValue {
			row = append(row, string(kve.Value()))
		}

		table.AddRow(row...)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) lsBuckets() error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	var found []*jsm.Stream

	_, err = mgr.EachStream(nil, func(s *jsm.Stream) {
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

		table.AddRow(strings.TrimPrefix(s.Name(), "KV_"), s.Description(), nfo.Created.Format("2006-01-02 15:04:05"), humanize.IBytes(nfo.State.Bytes), humanize.Comma(int64(nfo.State.Msgs)), humanizeDuration(time.Since(nfo.State.LastTime)))
	}

	fmt.Println(table.Render())

	return nil
}

func (c *kvCommand) revertAction(pc *fisk.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	rev, err := store.GetRevision(c.key, c.revision)
	if err != nil {
		return err
	}

	if !c.force {
		val := base64IfNotPrintable(rev.Value())
		if len(val) > 40 {
			val = fmt.Sprintf("%s...%s", val[0:15], val[len(val)-15:])
		}

		fmt.Printf("Revision: %d\n\n%v\n\n", rev.Revision(), val)
		ok, err := askConfirmation(fmt.Sprintf("Really revert to revision %d", c.revision), false)
		fisk.FatalIfError(err, "could not obtain confirmation")
		if !ok {
			return nil
		}
	}

	_, err = store.Put(c.key, rev.Value())
	if err != nil {
		return err
	}

	return c.historyAction(pc)
}

func (c *kvCommand) historyAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) compactAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) deleteAction(pc *fisk.ParseContext) error {
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

func (c *kvCommand) addAction(_ *fisk.ParseContext) error {
	_, js, err := prepareJSHelper()
	if err != nil {
		return err
	}

	storage := nats.FileStorage
	if strings.HasPrefix(c.storage, "m") {
		storage = nats.MemoryStorage
	}

	placement := &nats.Placement{Cluster: c.placementCluster}
	if len(c.placementTags) > 0 {
		placement.Tags = c.placementTags
	}

	cfg := &nats.KeyValueConfig{
		Bucket:       c.bucket,
		Description:  c.description,
		MaxValueSize: int32(c.maxValueSize),
		History:      uint8(c.history),
		TTL:          c.ttl,
		MaxBytes:     c.maxBucketSize,
		Storage:      storage,
		Replicas:     int(c.replicas),
		Placement:    placement,
	}

	if c.repubSource != "" && c.repubDest != "" {
		cfg.RePublish = &nats.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.mirror != "" {
		cfg.Mirror = &nats.StreamSource{
			Name:   c.mirror,
			Domain: c.mirrorDomain,
		}
	}

	store, err := js.CreateKeyValue(cfg)
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) getAction(_ *fisk.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	var res nats.KeyValueEntry
	if c.revision > 0 {
		res, err = store.GetRevision(c.key, c.revision)
	} else {
		res, err = store.Get(c.key)
	}
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

func (c *kvCommand) putAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) createAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) updateAction(_ *fisk.ParseContext) error {
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

	return io.ReadAll(os.Stdin)
}

func (c *kvCommand) loadBucket() (*nats.Conn, nats.JetStreamContext, nats.KeyValue, error) {
	nc, js, err := prepareJSHelper()
	if err != nil {
		return nil, nil, nil, err
	}

	if c.bucket == "" {
		known, err := c.knownBuckets(nc)
		if err != nil {
			return nil, nil, nil, err
		}

		if len(known) == 0 {
			return nil, nil, nil, fmt.Errorf("no KV buckets found")
		}

		err = askOne(&survey.Select{
			Message:  "Select a Bucket",
			Options:  known,
			PageSize: selectPageSize(len(known)),
		}, &c.bucket)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	store, err := js.KeyValue(c.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	return nc, js, store, err
}

func (c *kvCommand) knownBuckets(nc *nats.Conn) ([]string, error) {
	mgr, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return nil, err
	}

	var found []string
	for _, stream := range streams {
		if jsm.IsKVBucketStream(stream) {
			found = append(found, strings.TrimPrefix(stream, "KV_"))
		}
	}

	return found, nil
}

func (c *kvCommand) infoAction(_ *fisk.ParseContext) error {
	_, _, store, err := c.loadBucket()
	if err != nil {
		return err
	}

	return c.showStatus(store)
}

func (c *kvCommand) watchAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) purgeAction(_ *fisk.ParseContext) error {
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

func (c *kvCommand) rmBucketAction(_ *fisk.ParseContext) error {
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

	var nfo *nats.StreamInfo
	if status.BackingStore() == "JetStream" {
		nfo = status.(*nats.KeyValueBucketStatus).StreamInfo()
	}

	if nfo == nil {
		fmt.Printf("Information for Key-Value Store Bucket %s\n", status.Bucket())
	} else {
		fmt.Printf("Information for Key-Value Store Bucket %s created %s\n", status.Bucket(), nfo.Created.Local().Format(time.RFC3339))
	}

	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	fmt.Printf("          Bucket Name: %s\n", status.Bucket())
	fmt.Printf("         History Kept: %s\n", humanize.Comma(status.History()))
	fmt.Printf("        Values Stored: %s\n", humanize.Comma(int64(status.Values())))
	fmt.Printf("   Backing Store Kind: %s\n", status.BackingStore())

	if nfo != nil {
		if nfo.Config.Description != "" {
			fmt.Printf("          Description: %s\n", nfo.Config.Description)
		}
		fmt.Printf("          Bucket Size: %s\n", humanize.IBytes(nfo.State.Bytes))
		if nfo.Config.MaxBytes == -1 {
			fmt.Printf("  Maximum Bucket Size: unlimited\n")
		} else {
			fmt.Printf("  Maximum Bucket Size: %s\n", humanize.IBytes(uint64(nfo.Config.MaxBytes)))
		}
		if nfo.Config.MaxMsgSize == -1 {
			fmt.Printf("   Maximum Value Size: unlimited\n")
		} else {
			fmt.Printf("   Maximum Value Size: %s\n", humanize.IBytes(uint64(nfo.Config.MaxMsgSize)))
		}
		if nfo.Config.MaxAge <= 0 {
			fmt.Printf("          Maximum Age: unlimited\n")
		} else {
			fmt.Printf("          Maximum Age: %s\n", humanizeDuration(nfo.Config.MaxAge))
		}
		fmt.Printf("     JetStream Stream: %s\n", nfo.Config.Name)
		fmt.Printf("              Storage: %s\n", nfo.Config.Storage.String())
		if nfo.Config.RePublish != nil {
			if nfo.Config.RePublish.HeadersOnly {
				fmt.Printf(" Republishing Headers: %s to %s", nfo.Config.RePublish.Source, nfo.Config.RePublish.Destination)
			} else {
				fmt.Printf("         Republishing: %s to %s", nfo.Config.RePublish.Source, nfo.Config.RePublish.Destination)
			}
		}

		if nfo.Mirror != nil {
			s := nfo.Mirror
			fmt.Println("\n  Mirror Information:")
			fmt.Println()
			fmt.Printf("        Origin Bucket: %s\n", strings.TrimPrefix(s.Name, "KV_"))
			if s.External != nil {
				fmt.Printf("         External API: %v\n", s.External.APIPrefix)
			}
			if s.Active > 0 && s.Active < math.MaxInt64 {
				fmt.Printf("            Last Seen: %v\n", humanizeDuration(s.Active))
			} else {
				fmt.Printf("     	   Last Seen: never\n")
			}
			fmt.Printf("                  Lag: %s\n", humanize.Comma(int64(s.Lag)))

			fmt.Println()
		}
		if nfo.Cluster != nil {
			fmt.Println("\n  Cluster Information:")
			fmt.Println()
			renderNatsGoClusterInfo(nfo)
			fmt.Println()
		}

		if !nfo.Config.AllowRollup || nfo.Config.Discard != nats.DiscardNew {
			fmt.Println("Warning the bucket if not compatible with the latest")
			fmt.Println("configuration format and needs a configuration upgrade.")
			fmt.Println()
			fmt.Printf("Please run: nats kv upgrade %s\n\n", status.Bucket())
		}
	}

	return nil
}

func renderNatsGoClusterInfo(info *nats.StreamInfo) {
	fmt.Printf("                Name: %s\n", info.Cluster.Name)
	fmt.Printf("              Leader: %s\n", info.Cluster.Leader)
	for _, r := range info.Cluster.Replicas {
		state := []string{r.Name}

		if r.Current {
			state = append(state, "current")
		} else {
			state = append(state, "outdated")
		}

		if r.Offline {
			state = append(state, "OFFLINE")
		}

		if r.Active > 0 && r.Active < math.MaxInt64 {
			state = append(state, fmt.Sprintf("seen %s ago", humanizeDuration(r.Active)))
		} else {
			state = append(state, "not seen")
		}

		switch {
		case r.Lag > 1:
			state = append(state, fmt.Sprintf("%s operations behind", humanize.Comma(int64(r.Lag))))
		case r.Lag == 1:
			state = append(state, fmt.Sprintf("%d operation behind", r.Lag))
		}

		fmt.Printf("             Replica: %s\n", strings.Join(state, ", "))
	}
}

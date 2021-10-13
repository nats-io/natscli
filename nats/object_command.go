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

package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type objCommand struct {
	bucket       string
	file         string
	overrideName string
	hdrs         []string
	force        bool

	description string
	replicas    uint
	ttl         time.Duration
}

func configureObjectCommand(app *kingpin.Application) {
	c := &objCommand{}

	help := `Interacts with a JetStream based Object store

The JetStream Object store uses streams to store large objects
for an indefinite period or a per-bucket configured TTL.

NOTE: This is an experimental feature.
`

	obj := app.Command("object", help).Alias("obj")

	add := obj.Command("add", "Adds a new Object Store Bucket").Action(c.addAction)
	add.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	add.Flag("ttl", "How long to keep objects for").DurationVar(&c.ttl)
	add.Flag("replicas", "How many replicas of the data to store").Default("1").UintVar(&c.replicas)
	add.Flag("description", "A description for the bucket").StringVar(&c.description)

	put := obj.Command("put", "Puts a file into the store").Action(c.putAction)
	put.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	put.Arg("file", "The file to put").Required().ExistingFileVar(&c.file)
	put.Flag("name", "Override the name supplied to the object store").StringVar(&c.overrideName)
	put.Flag("description", "Sets an optional description for the object").StringVar(&c.description)
	put.Flag("header", "Adds headers to the object").Short('H').StringsVar(&c.hdrs)

	rm := obj.Command("rm", "Removes a file from the store").Action(c.rmAction)
	rm.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	rm.Arg("file", "The file to retrieve").Required().StringVar(&c.file)
	rm.Flag("force", "Act without confirmation").Short('f').BoolVar(&c.force)

	get := obj.Command("get", "Retrieves a file from the store").Action(c.getAction)
	get.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	get.Arg("file", "The file to retrieve").Required().StringVar(&c.file)
	get.Flag("output", "Override the output file name").Short('O').StringVar(&c.overrideName)

	info := obj.Command("info", "Get information about a bucket or object").Action(c.infoAction)
	info.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	info.Arg("file", "The file to retrieve").StringVar(&c.file)

	ls := obj.Command("ls", "List contents of the bucket").Action(c.lsAction)
	ls.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)

	seal := obj.Command("seal", "Seals a bucket preventing further updates").Action(c.sealAction)
	seal.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
	seal.Flag("force", "Force sealing without prompting").Short('f').BoolVar(&c.force)

	watch := obj.Command("watch", "Watch a bucket for changes").Action(c.watchAction)
	watch.Arg("bucket", "The bucket to act on").Required().StringVar(&c.bucket)
}

func (c *objCommand) watchAction(_ *kingpin.ParseContext) error {
	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	w, err := obj.Watch(nats.IncludeHistory())
	if err != nil {
		return err
	}
	defer w.Stop()

	for i := range w.Updates() {
		if i == nil {
			continue
		}

		if i.Deleted {
			fmt.Printf("[%s] %s %s > %s\n", i.ModTime.Format("2006-01-02 15:04:05"), color.RedString("DEL"), i.Bucket, i.Name)
		} else {
			fmt.Printf("[%s] %s %s > %s: %s bytes in %s chunks\n", i.ModTime.Format("2006-01-02 15:04:05"), color.GreenString("PUT"), i.Bucket, i.Name, humanize.IBytes(i.Size), humanize.Comma(int64(i.Chunks)))
		}
	}

	return nil
}

func (c *objCommand) sealAction(_ *kingpin.ParseContext) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really seal Bucket %s, sealed buckets can not be unsealed or modified", c.bucket), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	err = obj.Seal()
	if err != nil {
		return err
	}

	fmt.Printf("%s has been sealed\n", c.bucket)

	return c.showBucketInfo(obj)
}

func (c *objCommand) rmAction(_ *kingpin.ParseContext) error {
	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	if !c.force {
		nfo, err := obj.GetInfo(c.file)
		if err != nil {
			return err
		}

		ok, err := askConfirmation(fmt.Sprintf("Delete %s byte file %s > %s?", humanize.IBytes(nfo.Size), c.bucket, c.file), false)
		if err != nil {
			return err
		}

		if !ok {
			fmt.Println("Skipping delete")
			return nil
		}

	}

	err = obj.Delete(c.file)
	if err != nil {
		return err
	}

	fmt.Printf("Removed %s > %s\n", c.bucket, c.file)

	return c.showBucketInfo(obj)
}

func (c *objCommand) infoAction(_ *kingpin.ParseContext) error {
	if c.file == "" {
		return fmt.Errorf("only object info is supported")
	}

	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	nfo, err := obj.GetInfo(c.file)
	if err != nil {
		return err
	}

	c.showObjectInfo(nfo)

	return nil
}

func (c *objCommand) showBucketInfo(store nats.ObjectStore) error {
	status, err := store.Status()
	if err != nil {
		return err
	}

	fmt.Printf("%s Object Store Status\n", status.Bucket())
	fmt.Println()
	fmt.Printf("         Bucket Name: %s\n", status.Bucket())
	fmt.Printf("         Description: %s\n", status.Description())
	fmt.Printf("            Replicas: %d\n", status.Replicas())
	fmt.Printf("                 TTL: %s\n", humanizeDuration(status.TTL()))
	fmt.Printf("              Sealed: %v\n", status.Sealed())
	fmt.Printf("                Size: %s\n", humanize.IBytes(status.Size()))
	fmt.Printf("  Backing Store Kind: %s\n", status.BackingStore())
	if status.BackingStore() == "JetStream" {
		nfo := status.(*nats.ObjectBucketStatus).StreamInfo()
		fmt.Printf("    JetStream Stream: %s\n", nfo.Config.Name)
		if nfo.Cluster != nil {
			fmt.Printf("    Cluster Location: %s\n", nfo.Cluster.Name)
		}
	}

	return nil
}

func (c *objCommand) showObjectInfo(nfo *nats.ObjectInfo) {
	digest := strings.Split(nfo.Digest, "=")
	digestBytes, _ := base64.StdEncoding.DecodeString(digest[1])

	fmt.Printf("Object information for %s > %s\n\n", nfo.Bucket, nfo.Name)
	if nfo.Description != "" {
		fmt.Printf("      Description: %s\n", nfo.Description)
	}
	fmt.Printf("               Size: %s\n", humanize.IBytes(nfo.Size))
	fmt.Printf("  Modification Time: %s\n", nfo.ModTime.Format(time.RFC822Z))
	fmt.Printf("             Chunks: %s\n", humanize.Comma(int64(nfo.Chunks)))
	fmt.Printf("             Digest: %s %x\n", digest[0], digestBytes)
	fmt.Printf("                 ID: %s\n", nfo.NUID)
	if nfo.Deleted {
		fmt.Printf("            Deleted: %v\n", nfo.Deleted)
	}
	if len(nfo.Headers) > 0 {
		fmt.Printf("            Headers:\n")
		for k, v := range nfo.Headers {
			fmt.Printf("                     %s = %s\n", k, v)
		}
	}
}

func (c *objCommand) lsAction(_ *kingpin.ParseContext) error {
	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	contents, err := obj.List()
	if err != nil {
		return err
	}

	if len(contents) == 0 {
		fmt.Println("No entries found")
		return nil
	}

	table := newTableWriter("Bucket Contents")
	table.AddHeaders("Name", "Size", "Time", "ID")

	for _, i := range contents {
		table.AddRow(i.Name, humanize.IBytes(i.Size), i.ModTime.Format(time.RFC3339), i.NUID)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *objCommand) putAction(_ *kingpin.ParseContext) error {
	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	name := c.file
	if c.overrideName != "" {
		name = c.overrideName
	}

	hdr, err := parseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return err
	}

	meta := &nats.ObjectMeta{
		Name:        filepath.Clean(name),
		Description: c.description,
		Headers:     hdr,
	}

	r, err := os.Open(c.file)
	if err != nil {
		return err
	}

	nfo, err := obj.Put(meta, r)
	if err != nil {
		return err
	}

	c.showObjectInfo(nfo)

	return nil
}

func (c *objCommand) getAction(_ *kingpin.ParseContext) error {
	_, _, obj, err := c.loadBucket()
	if err != nil {
		return err
	}

	res, err := obj.Get(c.file)
	if err != nil {
		return err
	}

	nfo, err := res.Info()
	if err != nil {
		return err
	}

	if nfo.Deleted {
		return fmt.Errorf("file has been deleted")
	}

	out := filepath.Base(nfo.Name)
	if c.overrideName != "" {
		out = c.overrideName
	}

	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}

	of, err := os.Create(out)
	if err != nil {
		return err
	}
	defer of.Close()

	// TODO: progress
	wc, err := io.Copy(of, res)
	if err != nil {
		of.Close()
		os.Remove(of.Name())
		return err
	}

	if wc > 0 && uint64(wc) != nfo.Size {
		return fmt.Errorf("wrote %s, expected %s", humanize.IBytes(uint64(wc)), humanize.IBytes(nfo.Size))
	}

	of.Close()
	err = os.Chtimes(of.Name(), time.Now(), nfo.ModTime)
	if err != nil {
		log.Printf("Could not set modification time: %s", err)
	}

	// TODO: render, verify
	fmt.Printf("wrote: %s to %s\n", humanize.IBytes(uint64(wc)), of.Name())

	return nil
}

func (c *objCommand) addAction(_ *kingpin.ParseContext) error {
	_, js, err := prepareJSHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	obj, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:      c.bucket,
		Description: c.description,
		TTL:         c.ttl,
		Storage:     nats.FileStorage,
		Replicas:    int(c.replicas),
	})
	if err != nil {
		return err
	}

	return c.showBucketInfo(obj)
}

func (c *objCommand) loadBucket() (*nats.Conn, nats.JetStreamContext, nats.ObjectStore, error) {
	nc, js, err := prepareJSHelper("", natsOpts()...)
	if err != nil {
		return nil, nil, nil, err
	}

	store, err := js.ObjectStore(c.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	return nc, js, store, err
}

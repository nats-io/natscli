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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/google/go-cmp/cmp"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/jsm.go/api"
	"github.com/xlab/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go"
)

type streamCmd struct {
	stream           string
	force            bool
	json             bool
	msgID            int64
	retentionPolicyS string
	inputFile        string

	destination         string
	subjects            []string
	ack                 bool
	storage             string
	maxMsgLimit         int64
	maxBytesLimit       int64
	maxAgeLimit         string
	maxMsgSize          int32
	reportSortConsumers bool
	reportSortMsgs      bool
	reportSortName      bool
	reportSortStorage   bool
	reportRaw           bool
	maxStreams          int
	discardPolicy       string
	validateOnly        bool
	backupFile          string
	showProgress        bool
	healthCheck         bool
	snapshotChunk       int
}

func configureStreamCommand(app *kingpin.Application) {
	c := &streamCmd{msgID: -1}

	str := app.Command("stream", "JetStream Stream management").Alias("str").Alias("st").Alias("ms").Alias("s")

	strInfo := str.Command("info", "Stream information").Alias("nfo").Alias("i").Action(c.infoAction)
	strInfo.Arg("stream", "Stream to retrieve information for").StringVar(&c.stream)
	strInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("subjects", "Subjects that are consumed by the Stream").Default().StringsVar(&c.subjects)
		f.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
		f.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
		f.Flag("max-bytes", "Maximum bytes to keep").Int64Var(&c.maxBytesLimit)
		f.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
		f.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
		f.Flag("retention", "Defines a retention policy (limits, interest, work)").EnumVar(&c.retentionPolicyS, "limits", "interest", "workq", "work")
		f.Flag("discard", "Defines the discard policy (new, old)").EnumVar(&c.discardPolicy, "new", "old")
		f.Flag("max-msg-size", "Maximum size any 1 message may be").Int32Var(&c.maxMsgSize)
		f.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	}

	strAdd := str.Command("create", "Create a new Stream").Alias("add").Alias("new").Action(c.addAction)
	strAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	strAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strAdd.Flag("validate", "Only validates the configuration against the official Schema").BoolVar(&c.validateOnly)
	addCreateFlags(strAdd)

	strEdit := str.Command("edit", "Edits an existing stream").Action(c.editAction)
	strEdit.Arg("stream", "Stream to retrieve edit").StringVar(&c.stream)
	strEdit.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strEdit.Flag("force", "Force edit without prompting").Short('f').BoolVar(&c.force)

	addCreateFlags(strEdit)

	strCopy := str.Command("copy", "Creates a new Stream based on the configuration of another").Alias("cp").Action(c.cpAction)
	strCopy.Arg("source", "Source Stream to copy").Required().StringVar(&c.stream)
	strCopy.Arg("destination", "New Stream to create").Required().StringVar(&c.destination)
	addCreateFlags(strCopy)

	strRm := str.Command("rm", "Removes a Stream").Alias("delete").Alias("del").Action(c.rmAction)
	strRm.Arg("stream", "Stream name").StringVar(&c.stream)
	strRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strLs := str.Command("ls", "List all known Streams").Alias("list").Alias("l").Action(c.lsAction)
	strLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strPurge := str.Command("purge", "Purge a Stream without deleting it").Action(c.purgeAction)
	strPurge.Arg("stream", "Stream name").StringVar(&c.stream)
	strPurge.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	strPurge.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strRmMsg := str.Command("rmm", "Securely removes an individual message from a Stream").Action(c.rmMsgAction)
	strRmMsg.Arg("stream", "Stream name").StringVar(&c.stream)
	strRmMsg.Arg("id", "Message ID to remove").Int64Var(&c.msgID)
	strRmMsg.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strGet := str.Command("get", "Retrieves a specific message from a Stream").Action(c.getAction)
	strGet.Arg("stream", "Stream name").StringVar(&c.stream)
	strGet.Arg("id", "Message ID to retrieve").Int64Var(&c.msgID)
	strGet.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strReport := str.Command("report", "Reports on Stream statistics").Action(c.reportAction)
	strReport.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	strReport.Flag("consumers", "Sort by number of Consumers").Short('o').BoolVar(&c.reportSortConsumers)
	strReport.Flag("messages", "Sort by number of Messages").Short('m').BoolVar(&c.reportSortMsgs)
	strReport.Flag("name", "Sort by Stream name").Short('n').BoolVar(&c.reportSortName)
	strReport.Flag("storage", "Sort by Storage type").Short('t').BoolVar(&c.reportSortStorage)
	strReport.Flag("raw", "Show un-formatted numbers").Short('r').BoolVar(&c.reportRaw)

	strBackup := str.Command("backup", "Backs up a Stream over the NATS network").Action(c.backupAction)
	strBackup.Arg("stream", "Stream to backup").Required().StringVar(&c.stream)
	strBackup.Arg("target", "File to create the backup in").Required().StringVar(&c.backupFile)
	strBackup.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strBackup.Flag("check", "Checks the Stream for health prior to backup").Default("false").BoolVar(&c.healthCheck)
	strBackup.Flag("chunk-size", "The size of individual chunks to send").Default("16777216").IntVar(&c.snapshotChunk)

	strRestore := str.Command("restore", "Restore a Stream over the NATS network").Action(c.restoreAction)
	strRestore.Arg("stream", "The name of the Stream to restore").Required().StringVar(&c.stream)
	strRestore.Arg("file", "The file holding the backup to restore").Required().ExistingFileVar(&c.backupFile)
	strRestore.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strRestore.Flag("chunk-size", "The size of individual chunks to send").Default("16777216").IntVar(&c.snapshotChunk)

	strTemplate := str.Command("template", "Manages Stream Templates").Alias("templ").Alias("t")

	strTAdd := strTemplate.Command("create", "Creates a new Stream Template").Alias("add").Alias("new").Action(c.streamTemplateAdd)
	strTAdd.Arg("stream", "Template name").StringVar(&c.stream)
	strTAdd.Flag("max-streams", "Maximum amount of streams that this template can generate").Default("-1").IntVar(&c.maxStreams)
	addCreateFlags(strTAdd)

	strTLs := strTemplate.Command("ls", "List all known Stream Templates").Alias("list").Alias("l").Action(c.streamTemplateLs)
	strTLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strTRm := strTemplate.Command("rm", "Removes a Stream Template").Alias("delete").Alias("del").Action(c.streamTemplateRm)
	strTRm.Arg("template", "Stream Template name").StringVar(&c.stream)
	strTRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strTInfo := strTemplate.Command("info", "Stream Template information").Alias("nfo").Alias("i").Action(c.streamTemplateInfo)
	strTInfo.Arg("template", "Stream Template to retrieve information for").StringVar(&c.stream)
	strTInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
}

func (c *streamCmd) restoreAction(pc *kingpin.ParseContext) (err error) {
	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	known, err := jsm.IsKnownStream(c.stream)
	kingpin.FatalIfError(err, "Could not check if the stream already exist")
	if known {
		kingpin.Fatalf("Stream %q already exist", c.stream)
	}

	var progress *uiprogress.Bar
	var bps uint64

	cb := func(p jsm.RestoreProgress) {
		bps = p.BytesPerSecond()

		if progress == nil {
			progress = uiprogress.AddBar(p.ChunksToSend()).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {
				return humanize.IBytes(bps) + "/s"
			})
		}

		progress.Set(int(p.ChunksSent()))
	}

	var opts = []jsm.SnapshotOption{
		jsm.SnapshotChunkSize(c.snapshotChunk),
	}

	if c.showProgress {
		uiprogress.Start()
		opts = append(opts, jsm.RestoreNotify(cb))
	} else {
		opts = append(opts, jsm.SnapshotDebug())
	}

	fmt.Printf("Starting restore of Stream %q from file %q\n\n", c.stream, c.backupFile)

	fp, _, err := jsm.RestoreSnapshotFromFile(context.Background(), c.stream, c.backupFile, opts...)
	kingpin.FatalIfError(err, "restore failed")
	if c.showProgress {
		progress.Set(int(fp.ChunksSent()))
		uiprogress.Stop()
	}

	fmt.Println()
	fmt.Printf("Restored stream %q in %v\n", c.stream, fp.EndTime().Sub(fp.StartTime()))
	fmt.Println()

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	kingpin.FatalIfError(err, "could not show stream")

	return nil
}

func (c *streamCmd) backupAction(_ *kingpin.ParseContext) (err error) {
	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not load stream")

	first := true
	inprogress := true
	pmu := sync.Mutex{}
	var progress *uiprogress.Bar
	var bps uint64

	cb := func(p jsm.SnapshotProgress) {
		if first {
			fmt.Printf("Starting backup of Stream %q with %d data blocks\n\n", c.stream, p.BlocksExpected())

			if p.HealthCheck() {
				fmt.Printf("Health Check was requested, this can take a long time without progress reports\n\n")
			}

			first = false
		}

		if !p.HasMetadata() {
			return
		}

		bps = p.BytesPerSecond()

		if progress == nil {
			progress = uiprogress.AddBar(p.BlocksExpected() * p.BlockSize()).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {
				return humanize.IBytes(bps) + "/s"
			})
		}

		if !p.HasData() {
			progress.Set(int(p.BlockBytesReceived()))
		} else {
			pmu.Lock()
			if inprogress {
				progress.Set(p.BlocksExpected() * p.BlockSize())
				uiprogress.Stop()
				inprogress = false
			}
			pmu.Unlock()
		}
	}

	opts := []jsm.SnapshotOption{
		jsm.SnapshotConsumers(),
		jsm.SnapshotChunkSize(c.snapshotChunk),
	}

	if c.showProgress {
		uiprogress.Start()
		opts = append(opts, jsm.SnapshotNotify(cb))
	} else {
		opts = append(opts, jsm.SnapshotDebug())
	}

	if c.healthCheck {
		opts = append(opts, jsm.SnapshotHealthCheck())
	}

	fp, err := stream.SnapshotToFile(context.Background(), c.backupFile, opts...)
	pmu.Lock()
	if c.showProgress && inprogress {
		progress.Set(fp.BlocksExpected() * fp.BlockSize())
		uiprogress.Stop()
		inprogress = false
	}
	pmu.Unlock()
	kingpin.FatalIfError(err, "snapshot failed")

	fmt.Println()
	fmt.Printf("Received %s compressed data in %d chunks for stream %q in %v, %s uncompressed \n", humanize.IBytes(fp.BytesReceived()), fp.ChunksReceived(), c.stream, fp.EndTime().Sub(fp.StartTime()), humanize.IBytes(fp.BlockBytesReceived()))

	return nil
}

func (c *streamCmd) streamTemplateRm(_ *kingpin.ParseContext) (err error) {
	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, err = selectStreamTemplate(c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	template, err := jsm.LoadStreamTemplate(c.stream)
	kingpin.FatalIfError(err, "could not load Stream Template")

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete Stream Template %q, this will remove all managed Streams this template created as well", c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = template.Delete()
	kingpin.FatalIfError(err, "could not delete Stream Template")

	return nil
}

func (c *streamCmd) streamTemplateAdd(pc *kingpin.ParseContext) (err error) {
	cfg := c.prepareConfig()

	if c.maxStreams == -1 {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum Streams",
		}, &c.maxStreams, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxStreams < 0 {
		kingpin.Fatalf("Maximum Streams can not be negative")
	}

	cfg.Name = ""

	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "could not create Stream")

	_, err = jsm.NewStreamTemplate(c.stream, uint32(c.maxStreams), cfg)
	kingpin.FatalIfError(err, "could not create Stream Template")

	fmt.Printf("Stream Template %s was created\n\n", c.stream)

	return c.streamTemplateInfo(pc)
}

func (c *streamCmd) streamTemplateInfo(_ *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, err = selectStreamTemplate(c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	info, err := jsm.LoadStreamTemplate(c.stream)
	kingpin.FatalIfError(err, "could not load Stream Template %q", c.stream)

	if c.json {
		err = printJSON(info.Configuration())
		kingpin.FatalIfError(err, "could not display info")
		return nil
	}

	fmt.Printf("Information for Stream Template %s\n", c.stream)
	fmt.Println()
	c.showStreamConfig(info.StreamConfiguration())
	fmt.Printf("      Maximum Streams: %d\n", info.MaxStreams())
	fmt.Println()
	fmt.Println("Managed Streams:")
	fmt.Println()
	if len(info.Streams()) == 0 {
		fmt.Println("  No Streams have been defined by this template")
	} else {
		managed := info.Streams()
		sort.Strings(managed)
		for _, n := range managed {
			fmt.Printf("    %s\n", n)
		}
	}
	fmt.Println()

	return nil
}

func (c *streamCmd) streamTemplateLs(_ *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	names, err := jsm.StreamTemplateNames()
	kingpin.FatalIfError(err, "could not list Stream Templates")

	if c.json {
		err = printJSON(names)
		kingpin.FatalIfError(err, "could not display Stream Templates")
		return nil
	}

	if len(names) == 0 {
		fmt.Println("No Streams Templates defined")
		return nil
	}

	fmt.Println("Stream Templates:")
	fmt.Println()
	for _, t := range names {
		fmt.Printf("\t%s\n", t)
	}
	fmt.Println()

	return nil
}

func (c *streamCmd) reportAction(pc *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	type stat struct {
		Name      string
		Consumers int
		Msgs      int64
		Bytes     uint64
		Storage   string
		Template  string
	}

	if !c.json {
		fmt.Print("Obtaining Stream stats\n\n")
	}

	stats := []stat{}
	jsm.EachStream(func(stream *jsm.Stream) {
		info, err := stream.LatestInformation()
		kingpin.FatalIfError(err, "could not get stream info for %s", stream.Name())
		stats = append(stats, stat{info.Config.Name, info.State.Consumers, int64(info.State.Msgs), info.State.Bytes, info.Config.Storage.String(), info.Config.Template})
	})

	if len(stats) == 0 {
		if !c.json {
			fmt.Println("No Streams defined")
		}
		return nil
	}

	if c.json {
		printJSON(stats)
		return nil
	}

	if c.reportSortConsumers {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Consumers < stats[j].Consumers })
	} else if c.reportSortMsgs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Msgs < stats[j].Msgs })
	} else if c.reportSortName {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	} else if c.reportSortStorage {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Storage < stats[j].Storage })
	} else {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Bytes < stats[j].Bytes })
	}

	table := tablewriter.CreateTable()
	table.AddHeaders("Stream", "Consumers", "Messages", "Bytes", "Storage", "Template")

	for _, s := range stats {
		if c.reportRaw {
			table.AddRow(s.Name, s.Consumers, s.Msgs, s.Bytes, s.Storage, s.Template)
		} else {
			table.AddRow(s.Name, s.Consumers, humanize.Comma(s.Msgs), humanize.IBytes(s.Bytes), s.Storage, s.Template)
		}
	}

	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) copyAndEditStream(cfg api.StreamConfig) (api.StreamConfig, error) {
	var err error

	if c.inputFile != "" {
		var cfg api.StreamConfig
		f, err := ioutil.ReadFile(c.inputFile)
		if err != nil {
			return api.StreamConfig{}, err
		}

		err = json.Unmarshal(f, &cfg)
		if err != nil {
			return api.StreamConfig{}, err
		}

		if cfg.Name == "" {
			cfg.Name = c.stream
		}

		return cfg, nil
	}

	cfg.NoAck = !c.ack

	if c.discardPolicy != "" {
		cfg.Discard = c.discardPolicyFromString()
	}

	if len(c.subjects) > 0 {
		cfg.Subjects = c.splitCLISubjects()
	}

	if c.storage != "" {
		cfg.Storage = c.storeTypeFromString(c.storage)
	}

	if c.retentionPolicyS != "" {
		cfg.Retention = c.retentionPolicyFromString()
	}

	if c.maxBytesLimit != -1 {
		cfg.MaxBytes = c.maxBytesLimit
	}

	if c.maxMsgLimit != 0 {
		cfg.MaxMsgs = c.maxMsgLimit
	}

	if c.maxAgeLimit != "" {
		cfg.MaxAge, err = parseDurationString(c.maxAgeLimit)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid maximum age limit format: %v", err)
		}
	}

	if c.maxMsgSize != 0 {
		cfg.MaxMsgSize = c.maxMsgSize
	}

	return cfg, nil
}

func (c *streamCmd) editAction(pc *kingpin.ParseContext) error {
	c.connectAndAskStream()

	sourceStream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	cfg, err := c.copyAndEditStream(sourceStream.Configuration())
	kingpin.FatalIfError(err, "could not create new configuration for Stream %s", c.stream)

	// sorts strings to subject lists that only differ in ordering is considered equal
	sorter := cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	})

	diff := cmp.Diff(sourceStream.Configuration(), cfg, sorter)
	if diff == "" {
		fmt.Printf("No difference in configuration")
		return nil
	}

	fmt.Printf("Differences (-old +new):\n%s", diff)
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit Stream %s", c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = sourceStream.UpdateConfiguration(cfg)
	kingpin.FatalIfError(err, "could not edit Stream %s", c.stream)

	if !c.json {
		fmt.Printf("Stream %s was updated\n\n", c.stream)
	}

	c.showStream(sourceStream)

	return nil
}

func (c *streamCmd) cpAction(pc *kingpin.ParseContext) error {
	if c.stream == c.destination {
		kingpin.Fatalf("source and destination Stream names cannot be the same")
	}

	c.connectAndAskStream()

	sourceStream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	cfg, err := c.copyAndEditStream(sourceStream.Configuration())
	kingpin.FatalIfError(err, "could not copy Stream %s", c.stream)

	cfg.Name = c.destination

	new, err := jsm.NewStreamFromDefault(cfg.Name, cfg)
	kingpin.FatalIfError(err, "could not create Stream")

	if !c.json {
		fmt.Printf("Stream %s was created\n\n", c.stream)
	}

	c.showStream(new)

	return nil
}

func (c *streamCmd) showStreamConfig(cfg api.StreamConfig) {
	fmt.Println("Configuration:")
	fmt.Println()
	fmt.Printf("             Subjects: %s\n", strings.Join(cfg.Subjects, ", "))
	fmt.Printf("     Acknowledgements: %v\n", !cfg.NoAck)
	fmt.Printf("            Retention: %s - %s\n", cfg.Storage.String(), cfg.Retention.String())
	fmt.Printf("             Replicas: %d\n", cfg.Replicas)
	fmt.Printf("       Discard Policy: %s\n", cfg.Discard.String())
	if cfg.MaxMsgs == -1 {
		fmt.Println("     Maximum Messages: unlimited")
	} else {
		fmt.Printf("     Maximum Messages: %s\n", humanize.Comma(cfg.MaxMsgs))
	}
	if cfg.MaxBytes == -1 {
		fmt.Println("        Maximum Bytes: unlimited")
	} else {
		fmt.Printf("        Maximum Bytes: %s\n", humanize.IBytes(uint64(cfg.MaxBytes)))
	}
	if cfg.MaxAge == -1 {
		fmt.Println("          Maximum Age: unlimited")
	} else {
		fmt.Printf("          Maximum Age: %s\n", humanizeDuration(cfg.MaxAge))
	}
	if cfg.MaxMsgSize == -1 {
		fmt.Println(" Maximum Message Size: unlimited")
	} else {
		fmt.Printf(" Maximum Message Size: %s\n", humanize.IBytes(uint64(cfg.MaxMsgSize)))
	}
	if cfg.MaxConsumers == -1 {
		fmt.Println("    Maximum Consumers: unlimited")
	} else {
		fmt.Printf("    Maximum Consumers: %d\n", cfg.MaxConsumers)
	}
	if cfg.Template != "" {
		fmt.Printf("  Managed by Template: %s\n", cfg.Template)
	}
}

func (c *streamCmd) showStream(stream *jsm.Stream) error {
	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	c.showStreamInfo(info)

	return nil
}

func (c *streamCmd) showStreamInfo(info *api.StreamInfo) {
	if c.json {
		err := printJSON(info)
		kingpin.FatalIfError(err, "could not display info")
		return
	}

	fmt.Printf("Information for Stream %s\n", c.stream)
	fmt.Println()
	c.showStreamConfig(info.Config)
	fmt.Println()
	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("            Messages: %s\n", humanize.Comma(int64(info.State.Msgs)))
	fmt.Printf("               Bytes: %s\n", humanize.IBytes(info.State.Bytes))

	if info.State.FirstTime.IsZero() {
		fmt.Printf("            FirstSeq: %s\n", humanize.Comma(int64(info.State.FirstSeq)))
	} else {
		fmt.Printf("            FirstSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.FirstSeq)), info.State.FirstTime.Format("2006-01-02T15:04:05"))
	}

	if info.State.LastTime.IsZero() {
		fmt.Printf("             LastSeq: %s\n", humanize.Comma(int64(info.State.LastSeq)))
	} else {
		fmt.Printf("             LastSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.LastSeq)), info.State.LastTime.Format("2006-01-02T15:04:05"))
	}

	fmt.Printf("    Active Consumers: %d\n", info.State.Consumers)
}

func (c *streamCmd) infoAction(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	kingpin.FatalIfError(err, "could not show stream")

	fmt.Println()

	return nil
}

func (c *streamCmd) splitCLISubjects() []string {
	new := []string{}

	re := regexp.MustCompile(`,|\t|\s`)
	for _, s := range c.subjects {
		if re.MatchString(s) {
			new = append(new, splitString(s)...)
		} else {
			new = append(new, s)
		}
	}

	return new
}

func (c *streamCmd) discardPolicyFromString() api.DiscardPolicy {
	switch strings.ToLower(c.discardPolicy) {
	case "new":
		return api.DiscardNew
	case "old":
		return api.DiscardOld
	default:
		kingpin.Fatalf("invalid discard policy %s", c.discardPolicy)
		return api.DiscardOld // unreachable
	}
}

func (c *streamCmd) storeTypeFromString(s string) api.StorageType {
	switch s {
	case "file", "f":
		return api.FileStorage
	case "memory", "m":
		return api.MemoryStorage
	default:
		kingpin.Fatalf("invalid storage type %s", c.storage)
		return api.MemoryStorage // unreachable
	}
}

func (c *streamCmd) retentionPolicyFromString() api.RetentionPolicy {
	switch strings.ToLower(c.retentionPolicyS) {
	case "limits":
		return api.LimitsPolicy
	case "interest":
		return api.InterestPolicy
	case "work queue", "workq", "work":
		return api.WorkQueuePolicy
	default:
		kingpin.Fatalf("invalid retention policy %s", c.retentionPolicyS)
		return api.LimitsPolicy // unreachable
	}
}

func (c *streamCmd) prepareConfig() (cfg api.StreamConfig) {
	var err error

	if c.inputFile != "" {
		f, err := ioutil.ReadFile(c.inputFile)
		kingpin.FatalIfError(err, "invalid input")

		err = json.Unmarshal(f, &cfg)
		kingpin.FatalIfError(err, "invalid input")

		if c.stream != "" {
			cfg.Name = c.stream
		}

		if c.stream == "" {
			c.stream = cfg.Name
		}

		return cfg
	}

	if c.stream == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Stream Name",
		}, &c.stream, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	if len(c.subjects) == 0 {
		subjects := ""
		err = survey.AskOne(&survey.Input{
			Message: "Subjects to consume",
			Help:    "Streams consume messages from subjects, this is a space or comma separated list that can include wildcards. Settable using --subjects",
		}, &subjects, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		c.subjects = splitString(subjects)
	}

	c.subjects = c.splitCLISubjects()

	if c.storage == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Storage backend",
			Options: []string{"file", "memory"},
			Help:    "Streams are stored on the server, this can be one of many backends and all are usable in clustering mode. Settable using --storage",
		}, &c.storage, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	storage := c.storeTypeFromString(c.storage)

	if c.retentionPolicyS == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Retention Policy",
			Options: []string{"Limits", "Interest", "Work Queue"},
			Help:    "Messages are retained either based on limits like size and age (Limits), as long as there are Consumers (Interest) or until any worker processed them (Work Queue)",
			Default: "Limits",
		}, &c.retentionPolicyS, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.discardPolicy == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Discard Policy",
			Options: []string{"New", "Old"},
			Help:    "Once the Stream reach it's limits of size or messages the New policy will prevent further messages from being added while Old will delete old messages.",
			Default: "Old",
		}, &c.discardPolicy, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	var maxAge time.Duration
	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Message count limit", "-1", "Defines the amount of messages to keep in the store for this Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit == 0 {
		c.maxBytesLimit, err = askOneBytes("Message size limit", "-1", "Defines the combined size of all messages in a Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-bytes")
		kingpin.FatalIfError(err, "invalid input")

		if c.maxBytesLimit <= 0 {
			c.maxBytesLimit = -1
		}
	}

	if c.maxAgeLimit == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum message age limit",
			Default: "-1",
			Help:    "Defines the oldest messages that can be stored in the Stream, any messages older than this period will be removed, -1 for unlimited. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --max-age",
		}, &c.maxAgeLimit)
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxAgeLimit != "-1" {
		maxAge, err = parseDurationString(c.maxAgeLimit)
		kingpin.FatalIfError(err, "invalid maximum age limit format")
	}

	if c.maxMsgSize == 0 {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum individual message size",
			Default: "-1",
			Help:    "Defines the maximum size any single message may be to be accepted by the Stream. Settable using --max-msg-size",
		}, &c.maxMsgSize)
		kingpin.FatalIfError(err, "invalid input")
	}

	cfg = api.StreamConfig{
		Name:         c.stream,
		Subjects:     c.subjects,
		MaxMsgs:      c.maxMsgLimit,
		MaxBytes:     c.maxBytesLimit,
		MaxMsgSize:   c.maxMsgSize,
		MaxAge:       maxAge,
		Storage:      storage,
		NoAck:        !c.ack,
		Retention:    c.retentionPolicyFromString(),
		Discard:      c.discardPolicyFromString(),
		MaxConsumers: -1,
		Replicas:     1,
	}

	return cfg
}

func (c *streamCmd) addAction(pc *kingpin.ParseContext) (err error) {
	cfg := c.prepareConfig()

	if c.validateOnly {
		j, err := json.MarshalIndent(cfg, "", "  ")
		kingpin.FatalIfError(err, "Could not marshal configuration")
		fmt.Println(string(j))
		fmt.Println()
		valid, errs := cfg.Validate()
		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Println("Configuration is a valid Stream")
		return nil
	}

	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "could not create Stream")

	str, err := jsm.NewStreamFromDefault(c.stream, cfg)
	kingpin.FatalIfError(err, "could not create Stream")

	fmt.Printf("Stream %s was created\n\n", c.stream)

	c.showStream(str)

	return nil
}

func (c *streamCmd) rmAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete Stream %s", c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not remove Stream")

	err = stream.Delete()
	kingpin.FatalIfError(err, "could not remove Stream")

	return nil
}

func (c *streamCmd) purgeAction(pc *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge Stream %s", c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not purge Stream")

	err = stream.Purge()
	kingpin.FatalIfError(err, "could not purge Stream")

	c.showStream(stream)

	return nil
}

func (c *streamCmd) lsAction(_ *kingpin.ParseContext) (err error) {
	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	streams, err := jsm.StreamNames()
	kingpin.FatalIfError(err, "could not list Streams")

	if c.json {
		err = printJSON(streams)
		kingpin.FatalIfError(err, "could not display Streams")
		return nil
	}

	if len(streams) == 0 {
		fmt.Println("No Streams defined")
		return nil
	}

	fmt.Println("Streams:")
	fmt.Println()
	for _, s := range streams {
		fmt.Printf("\t%s\n", s)
	}
	fmt.Println()

	return nil
}

func (c *streamCmd) rmMsgAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if c.msgID == -1 {
		id := ""
		err = survey.AskOne(&survey.Input{
			Message: "Message ID to remove",
		}, &id, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		kingpin.FatalIfError(err, "invalid number")

		c.msgID = int64(idint)
	}

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Stream %s", c.stream)

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove message %d from Stream %s", c.msgID, c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	return stream.DeleteMessage(int(c.msgID))
}

func (c *streamCmd) getAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if c.msgID == -1 {
		id := ""
		err = survey.AskOne(&survey.Input{
			Message: "Message ID to retrieve",
		}, &id, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		kingpin.FatalIfError(err, "invalid number")

		c.msgID = int64(idint)
	}

	stream, err := jsm.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Stream %s", c.stream)

	item, err := stream.ReadMessage(int(c.msgID))
	kingpin.FatalIfError(err, "could not retrieve %s#%d", c.stream, c.msgID)

	if c.json {
		printJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v on Subject %s\n\n", c.stream, c.msgID, item.Time, item.Subject)

	if len(item.Header) > 0 {
		fmt.Println("Headers:")
		hdrs, err := decodeHeadersMsg(item.Header)
		if err == nil {
			for k, vals := range hdrs {
				for _, val := range vals {
					fmt.Printf("  %s: %s\n", k, val)
				}
			}
		}
		fmt.Println()
	}

	fmt.Println(string(item.Data))
	fmt.Println()
	return nil
}

func (c *streamCmd) connectAndAskStream() {
	_, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, err = selectStream(c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream to operate on")
}

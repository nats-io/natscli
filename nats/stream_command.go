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
	"log"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/emicklei/dot"
	"github.com/google/go-cmp/cmp"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
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
	outFile          string
	filterSubject    string

	destination           string
	subjects              []string
	ack                   bool
	storage               string
	maxMsgLimit           int64
	maxMsgPerSubjectLimit int64
	maxBytesLimit         int64
	maxAgeLimit           string
	maxMsgSize            int64
	maxConsumers          int
	reportSortConsumers   bool
	reportSortMsgs        bool
	reportSortName        bool
	reportSortStorage     bool
	reportRaw             bool
	reportLimitCluster    string
	reportLeaderDistrib   bool
	maxStreams            int
	discardPolicy         string
	validateOnly          bool
	backupDirectory       string
	showProgress          bool
	healthCheck           bool
	snapShotConsumers     bool
	dupeWindow            string
	replicas              int64
	placementCluster      string
	placementTags         []string
	peerName              string
	sources               []string
	mirror                string
	interactive           bool
	purgeKeep             uint64
	purgeSubject          string
	purgeSequence         uint64
	description           string

	vwStartId    int
	vwStartDelta time.Duration
	vwPageSize   int
	vwRaw        bool
	vwSubject    string

	selectedStream *jsm.Stream
	nc             *nats.Conn
	mgr            *jsm.Manager
}

type streamStat struct {
	Name      string
	Consumers int
	Msgs      int64
	Bytes     uint64
	Storage   string
	Template  string
	Cluster   *api.ClusterInfo
	LostBytes uint64
	LostMsgs  int
	Deleted   int
	Mirror    *api.StreamSourceInfo
	Sources   []*api.StreamSourceInfo
}

func configureStreamCommand(app *kingpin.Application) {
	c := &streamCmd{msgID: -1}

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
		f.Flag("cluster", "Place the stream on a specific cluster").StringVar(&c.placementCluster)
		f.Flag("description", "Sets a contextual description for the stream").StringVar(&c.description)
		f.Flag("discard", "Defines the discard policy (new, old)").EnumVar(&c.discardPolicy, "new", "old")
		f.Flag("dupe-window", "Window size for duplicate tracking").Default("").StringVar(&c.dupeWindow)
		f.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
		f.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
		f.Flag("max-bytes", "Maximum bytes to keep").Int64Var(&c.maxBytesLimit)
		f.Flag("max-consumers", "Maximum number of consumers to allow").Default("-1").IntVar(&c.maxConsumers)
		f.Flag("max-msg-size", "Maximum size any 1 message may be").Int64Var(&c.maxMsgSize)
		f.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
		f.Flag("max-msgs-per-subject", "Maximum amount of messages to keep per subject").Default("0").Int64Var(&c.maxMsgPerSubjectLimit)
		f.Flag("mirror", "Completely mirror another stream").StringVar(&c.mirror)
		f.Flag("replicas", "When clustered, how many replicas of the data to create").Int64Var(&c.replicas)
		f.Flag("retention", "Defines a retention policy (limits, interest, work)").EnumVar(&c.retentionPolicyS, "limits", "interest", "workq", "work")
		f.Flag("source", "Source data from other Streams, merging into this one").PlaceHolder("STREAM").StringsVar(&c.sources)
		f.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
		f.Flag("subjects", "Subjects that are consumed by the Stream").Default().StringsVar(&c.subjects)
		f.Flag("tags", "Place the stream on servers that has specific tags").StringsVar(&c.placementTags)
	}

	str := app.Command("stream", "JetStream Stream management").Alias("str").Alias("st").Alias("ms").Alias("s")

	strAdd := str.Command("add", "Create a new Stream").Alias("create").Alias("new").Action(c.addAction)
	strAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	strAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strAdd.Flag("validate", "Only validates the configuration against the official Schema").BoolVar(&c.validateOnly)
	strAdd.Flag("output", "Save configuration instead of creating").PlaceHolder("FILE").StringVar(&c.outFile)
	addCreateFlags(strAdd)

	strEdit := str.Command("edit", "Edits an existing stream").Action(c.editAction)
	strEdit.Arg("stream", "Stream to retrieve edit").StringVar(&c.stream)
	strEdit.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strEdit.Flag("force", "Force edit without prompting").Short('f').BoolVar(&c.force)
	strEdit.Flag("interactive", "Edit the configuring using your editor").Short('i').BoolVar(&c.interactive)
	addCreateFlags(strEdit)

	strInfo := str.Command("info", "Stream information").Alias("nfo").Alias("i").Action(c.infoAction)
	strInfo.Arg("stream", "Stream to retrieve information for").StringVar(&c.stream)
	strInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strLs := str.Command("ls", "List all known Streams").Alias("list").Alias("l").Action(c.lsAction)
	strLs.Flag("subject", "Filters Streams by those with interest matching a subject or wildcard").StringVar(&c.filterSubject)
	strLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strRm := str.Command("rm", "Removes a Stream").Alias("delete").Alias("del").Action(c.rmAction)
	strRm.Arg("stream", "Stream name").StringVar(&c.stream)
	strRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strPurge := str.Command("purge", "Purge a Stream without deleting it").Action(c.purgeAction)
	strPurge.Arg("stream", "Stream name").StringVar(&c.stream)
	strPurge.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	strPurge.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)
	strPurge.Flag("subject", "Limits the purge to a specific subject").PlaceHolder("SUBJECT").StringVar(&c.purgeSubject)
	strPurge.Flag("seq", "Purge up to but not including a specific message sequence").PlaceHolder("SEQUENCE").Uint64Var(&c.purgeSequence)
	strPurge.Flag("keep", "Keeps a certain number of messages after the purge").PlaceHolder("MESSAGES").Uint64Var(&c.purgeKeep)

	strCopy := str.Command("copy", "Creates a new Stream based on the configuration of another").Alias("cp").Action(c.cpAction)
	strCopy.Arg("source", "Source Stream to copy").Required().StringVar(&c.stream)
	strCopy.Arg("destination", "New Stream to create").Required().StringVar(&c.destination)
	addCreateFlags(strCopy)

	strGet := str.Command("get", "Retrieves a specific message from a Stream").Action(c.getAction)
	strGet.Arg("stream", "Stream name").StringVar(&c.stream)
	strGet.Arg("id", "Message Sequence to retrieve").Int64Var(&c.msgID)
	strGet.Flag("last-for", "Retrieves the message for a specific subject").Short('S').PlaceHolder("SUBJECT").StringVar(&c.filterSubject)
	strGet.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strRmMsg := str.Command("rmm", "Securely removes an individual message from a Stream").Action(c.rmMsgAction)
	strRmMsg.Arg("stream", "Stream name").StringVar(&c.stream)
	strRmMsg.Arg("id", "Message Sequence to remove").Int64Var(&c.msgID)
	strRmMsg.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	strView := str.Command("view", "View messages in a stream").Action(c.viewAction)
	strView.Arg("stream", "Stream name").StringVar(&c.stream)
	strView.Arg("size", "Page size").Default("10").IntVar(&c.vwPageSize)
	strView.Flag("id", "Start at a specific message Sequence").IntVar(&c.vwStartId)
	strView.Flag("since", "Start at a time delta").DurationVar(&c.vwStartDelta)
	strView.Flag("raw", "Show the raw data received").BoolVar(&c.vwRaw)
	strView.Flag("subject", "Filter the stream using a subject").StringVar(&c.vwSubject)

	strReport := str.Command("report", "Reports on Stream statistics").Action(c.reportAction)
	strReport.Flag("cluster", "Limit report to streams within a specific cluster").StringVar(&c.reportLimitCluster)
	strReport.Flag("consumers", "Sort by number of Consumers").Short('o').BoolVar(&c.reportSortConsumers)
	strReport.Flag("messages", "Sort by number of Messages").Short('m').BoolVar(&c.reportSortMsgs)
	strReport.Flag("name", "Sort by Stream name").Short('n').BoolVar(&c.reportSortName)
	strReport.Flag("storage", "Sort by Storage type").Short('t').BoolVar(&c.reportSortStorage)
	strReport.Flag("raw", "Show un-formatted numbers").Short('r').BoolVar(&c.reportRaw)
	strReport.Flag("dot", "Produce a GraphViz graph of replication topology").StringVar(&c.outFile)
	strReport.Flag("leaders", "Show details about RAFT leaders").Short('l').BoolVar(&c.reportLeaderDistrib)

	strBackup := str.Command("backup", "Creates a backup of a Stream over the NATS network").Alias("snapshot").Action(c.backupAction)
	strBackup.Arg("stream", "Stream to backup").Required().StringVar(&c.stream)
	strBackup.Arg("target", "Directory to create the backup in").Required().StringVar(&c.backupDirectory)
	strBackup.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strBackup.Flag("check", "Checks the Stream for health prior to backup").Default("false").BoolVar(&c.healthCheck)
	strBackup.Flag("consumers", "Enable or disable consumer backups").Default("true").BoolVar(&c.snapShotConsumers)

	strRestore := str.Command("restore", "Restore a Stream over the NATS network").Action(c.restoreAction)
	strRestore.Arg("stream", "The name of the Stream to restore").Required().StringVar(&c.stream)
	strRestore.Arg("file", "The directory holding the backup to restore").Required().ExistingDirVar(&c.backupDirectory)
	strRestore.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strRestore.Flag("config", "Load a different configuration when restoring the stream").ExistingFileVar(&c.inputFile)

	strCluster := str.Command("cluster", "Manages a clustered Stream").Alias("c")
	strClusterDown := strCluster.Command("step-down", "Force a new leader election by standing down the current leader").Alias("stepdown").Alias("sd").Alias("elect").Alias("down").Alias("d").Action(c.leaderStandDown)
	strClusterDown.Arg("stream", "Stream to act on").StringVar(&c.stream)

	strClusterRemovePeer := strCluster.Command("peer-remove", "Removes a peer from the Stream cluster").Alias("pr").Action(c.removePeer)
	strClusterRemovePeer.Arg("stream", "The stream to act on").StringVar(&c.stream)
	strClusterRemovePeer.Arg("peer", "The name of the peer to remove").StringVar(&c.peerName)

	strTemplate := str.Command("template", "Manages Stream Templates").Alias("templ").Alias("t")

	strTAdd := strTemplate.Command("create", "Creates a new Stream Template").Alias("add").Alias("new").Action(c.streamTemplateAdd)
	strTAdd.Arg("stream", "Template name").StringVar(&c.stream)
	strTAdd.Flag("max-streams", "Maximum amount of streams that this template can generate").Default("-1").IntVar(&c.maxStreams)
	addCreateFlags(strTAdd)

	strTInfo := strTemplate.Command("info", "Stream Template information").Alias("nfo").Alias("i").Action(c.streamTemplateInfo)
	strTInfo.Arg("template", "Stream Template to retrieve information for").StringVar(&c.stream)
	strTInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strTLs := strTemplate.Command("ls", "List all known Stream Templates").Alias("list").Alias("l").Action(c.streamTemplateLs)
	strTLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strTRm := strTemplate.Command("rm", "Removes a Stream Template").Alias("delete").Alias("del").Action(c.streamTemplateRm)
	strTRm.Arg("template", "Stream Template name").StringVar(&c.stream)
	strTRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)
}

func (c *streamCmd) loadStream(stream string) (*jsm.Stream, error) {
	if c.selectedStream != nil && c.selectedStream.Name() == stream {
		return c.selectedStream, nil
	}

	return c.mgr.LoadStream(stream)
}

func (c *streamCmd) leaderStandDown(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	leader := info.Cluster.Leader
	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(info.Cluster.Replicas)+1)
	err = stream.LeaderStepDown()
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTimer(500 * time.Millisecond).C {
		if ctr == 5 {
			return fmt.Errorf("stream did not elect a new leader in time")
		}
		ctr++

		info, err = stream.Information()
		if err != nil {
			log.Printf("Failed to retrieve Stream State: %s", err)
			continue
		}

		if info.Cluster.Leader != leader {
			log.Printf("New leader elected %q", info.Cluster.Leader)
			break
		}
	}

	if info.Cluster.Leader == leader {
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
	}

	fmt.Println()
	return c.showStream(stream)
}

func (c *streamCmd) removePeer(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.Information()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	if c.peerName == "" {
		peerNames := []string{info.Cluster.Leader}
		for _, r := range info.Cluster.Replicas {
			peerNames = append(peerNames, r.Name)
		}

		err = survey.AskOne(&survey.Select{
			Message: "Select a Peer",
			Options: peerNames,
		}, &c.peerName)
		if err != nil {
			return err
		}
	}

	log.Printf("Removing peer %q", c.peerName)

	err = stream.RemoveRAFTPeer(c.peerName)
	if err != nil {
		return err
	}

	log.Printf("Requested removal of peer %q", c.peerName)

	return nil
}

func (c *streamCmd) viewAction(_ *kingpin.ParseContext) error {
	if c.vwPageSize > 25 {
		c.vwPageSize = 25
	}

	c.connectAndAskStream()

	str, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	pops := []jsm.PagerOption{
		jsm.PagerSize(c.vwPageSize),
	}

	switch {
	case c.vwStartDelta > 0:
		pops = append(pops, jsm.PagerStartDelta(c.vwStartDelta))
	case c.vwStartId > 0:
		pops = append(pops, jsm.PagerStartId(c.vwStartId))
	}

	if c.vwSubject != "" {
		pops = append(pops, jsm.PagerFilterSubject(c.vwSubject))
	}

	pgr, err := str.PageContents(pops...)
	if err != nil {
		return err
	}
	defer pgr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			cancel()
		}
	}()

	for {
		msg, last, err := pgr.NextMsg(ctx)
		if err != nil && last {
			log.Println("Reached apparent end of data")
			return nil
		}
		if err != nil {
			return err
		}

		switch {
		case c.vwRaw:
			fmt.Println(string(msg.Data))
		default:
			meta, err := jsm.ParseJSMsgMetadata(msg)
			if err == nil {
				fmt.Printf("[%d] Subject: %s Received: %s\n", meta.StreamSequence(), msg.Subject, meta.TimeStamp().Format(time.RFC3339))
			} else {
				fmt.Printf("Subject: %s Reply: %s\n", msg.Subject, msg.Reply)
			}

			if len(msg.Header) > 0 {
				fmt.Println()
				for k, vs := range msg.Header {
					for _, v := range vs {
						fmt.Printf("  %s: %s\n", k, v)
					}
				}
			}

			fmt.Println()
			if len(msg.Data) == 0 {
				fmt.Println("nil body")
			} else {
				fmt.Println(string(msg.Data))
				if !strings.HasSuffix(string(msg.Data), "\n") {
					fmt.Println()
				}
			}

		}

		if last {
			next := false
			survey.AskOne(&survey.Confirm{Message: "Next Page?", Default: true}, &next)
			if !next {
				return nil
			}
		}
	}
}

func (c *streamCmd) restoreAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	known, err := mgr.IsKnownStream(c.stream)
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
			progress.Width = progressWidth()
		}

		progress.Set(int(p.ChunksSent()))
	}

	var opts []jsm.SnapshotOption

	if c.showProgress {
		uiprogress.Start()
		opts = append(opts, jsm.RestoreNotify(cb))
	} else {
		opts = append(opts, jsm.SnapshotDebug())
	}

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		if err != nil {
			return err
		}

		opts = append(opts, jsm.RestoreConfiguration(*cfg))
	}

	fmt.Printf("Starting restore of Stream %q from file %q\n\n", c.stream, c.backupDirectory)

	fp, _, err := mgr.RestoreSnapshotFromDirectory(context.Background(), c.stream, c.backupDirectory, opts...)
	kingpin.FatalIfError(err, "restore failed")
	if c.showProgress {
		progress.Set(int(fp.ChunksSent()))
		uiprogress.Stop()
	}

	fmt.Println()
	fmt.Printf("Restored stream %q in %v\n", c.stream, fp.EndTime().Sub(fp.StartTime()).Round(time.Second))
	fmt.Println()

	stream, err := mgr.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	kingpin.FatalIfError(err, "could not show stream")

	return nil
}

func (c *streamCmd) backupAction(_ *kingpin.ParseContext) error {
	var err error

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	first := true
	inprogress := true
	pmu := sync.Mutex{}
	var progress *uiprogress.Bar
	var bps uint64
	expected := 1

	cb := func(p jsm.SnapshotProgress) {
		if progress == nil {
			if p.BytesExpected() > 0 {
				expected = int(p.BytesExpected())
			}
			progress = uiprogress.AddBar(expected).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {
				return humanize.IBytes(bps) + "/s"
			})
			progress.Width = progressWidth()
		}

		if first {
			fmt.Printf("Starting backup of Stream %q with %s\n\n", c.stream, humanize.IBytes(p.BytesExpected()))

			if p.HealthCheck() {
				fmt.Printf("Health Check was requested, this can take a long time without progress reports\n\n")
			}

			first = false
		}

		bps = p.BytesPerSecond()
		progress.Set(int(p.BytesReceived()))

		if p.Finished() {
			pmu.Lock()
			if inprogress {
				uiprogress.Stop()
				inprogress = false
			}
			pmu.Unlock()
		}
	}

	var opts []jsm.SnapshotOption

	if c.snapShotConsumers {
		opts = append(opts, jsm.SnapshotConsumers())
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

	fp, err := stream.SnapshotToDirectory(context.Background(), c.backupDirectory, opts...)
	kingpin.FatalIfError(err, "snapshot failed")

	pmu.Lock()
	if c.showProgress && inprogress {
		progress.Set(int(fp.BytesReceived()))
		uiprogress.Stop()
		inprogress = false
	}
	pmu.Unlock()

	fmt.Println()
	fmt.Printf("Received %s compressed data in %d chunks for stream %q in %v, %s uncompressed \n", humanize.IBytes(fp.BytesReceived()), fp.ChunksReceived(), c.stream, fp.EndTime().Sub(fp.StartTime()).Round(time.Millisecond), humanize.IBytes(fp.UncompressedBytesReceived()))

	return nil
}

func (c *streamCmd) streamTemplateRm(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, err = selectStreamTemplate(mgr, c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	template, err := mgr.LoadStreamTemplate(c.stream)
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

	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "could not create Stream")

	_, err = mgr.NewStreamTemplate(c.stream, uint32(c.maxStreams), cfg)
	kingpin.FatalIfError(err, "could not create Stream Template")

	fmt.Printf("Stream Template %s was created\n\n", c.stream)

	return c.streamTemplateInfo(pc)
}

func (c *streamCmd) streamTemplateInfo(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, err = selectStreamTemplate(mgr, c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	info, err := mgr.LoadStreamTemplate(c.stream)
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
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	names, err := mgr.StreamTemplateNames()
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

func (c *streamCmd) reportAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	if !c.json {
		fmt.Print("Obtaining Stream stats\n\n")
	}

	stats := []streamStat{}
	leaders := make(map[string]*raftLeader)
	showReplication := false

	dg := dot.NewGraph(dot.Directed)
	dg.Label("Stream Replication Structure")

	err = mgr.EachStream(func(stream *jsm.Stream) {
		info, err := stream.LatestInformation()
		kingpin.FatalIfError(err, "could not get stream info for %s", stream.Name())

		if info.Cluster != nil {
			if c.reportLimitCluster != "" && info.Cluster.Name != c.reportLimitCluster {
				return
			}

			if info.Cluster.Leader != "" {
				_, ok := leaders[info.Cluster.Leader]
				if !ok {
					leaders[info.Cluster.Leader] = &raftLeader{name: info.Cluster.Leader, cluster: info.Cluster.Name}
				}
				leaders[info.Cluster.Leader].groups++
			}
		}

		deleted := info.State.NumDeleted
		// backward compat with servers that predate the num_deleted response
		if len(info.State.Deleted) > 0 {
			deleted = len(info.State.Deleted)
		}
		s := streamStat{Name: info.Config.Name, Consumers: info.State.Consumers, Msgs: int64(info.State.Msgs), Bytes: info.State.Bytes, Storage: info.Config.Storage.String(), Template: info.Config.Template, Cluster: info.Cluster, Deleted: deleted, Mirror: info.Mirror, Sources: info.Sources}
		if info.State.Lost != nil {
			s.LostBytes = info.State.Lost.Bytes
			s.LostMsgs = len(info.State.Lost.Msgs)
		}

		if len(info.Config.Sources) > 0 {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			for _, source := range info.Config.Sources {
				snode, ok := dg.FindNodeById(source.Name)
				if !ok {
					snode = dg.Node(source.Name)
				}
				edge := dg.Edge(snode, node).Attr("color", "green")
				if source.FilterSubject != "" {
					edge.Label(source.FilterSubject)
				}
			}
		}

		if info.Config.Mirror != nil {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			mnode, ok := dg.FindNodeById(info.Config.Mirror.Name)
			if !ok {
				mnode = dg.Node(info.Config.Mirror.Name)
			}
			dg.Edge(mnode, node).Attr("color", "blue").Label("Mirror")
		}

		stats = append(stats, s)
	})
	if err != nil {
		return err
	}

	if len(stats) == 0 {
		if !c.json {
			fmt.Println("No Streams defined")
		}
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

	c.renderStreams(stats)

	if showReplication {
		c.renderReplication(stats)

		if c.outFile != "" {
			ioutil.WriteFile(c.outFile, []byte(dg.String()), 0644)
		}
	}

	if c.reportLeaderDistrib && len(leaders) > 0 {
		renderRaftLeaders(leaders, "Streams")
	}

	return nil
}

func (c *streamCmd) renderReplication(stats []streamStat) {
	table := newTableWriter("Replication Report")
	table.AddHeaders("Stream", "Kind", "API Prefix", "Source Stream", "Active", "Lag", "Error")

	for _, s := range stats {
		if len(s.Sources) == 0 && s.Mirror == nil {
			continue
		}

		if s.Mirror != nil {
			apierr := ""
			if s.Mirror.Error != nil {
				apierr = s.Mirror.Error.Error()
			}

			eApiPrefix := ""
			if s.Mirror.External != nil {
				eApiPrefix = s.Mirror.External.ApiPrefix
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, s.Mirror.Active, s.Mirror.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, humanizeDuration(s.Mirror.Active), humanize.Comma(int64(s.Mirror.Lag)), apierr)
			}
		}

		for _, source := range s.Sources {
			apierr := ""
			if source != nil && source.Error != nil {
				apierr = source.Error.Error()
			}

			eApiPrefix := ""
			if source.External != nil {
				eApiPrefix = source.External.ApiPrefix
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, source.Active, source.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, humanizeDuration(source.Active), humanize.Comma(int64(source.Lag)), apierr)
			}

		}
	}
	fmt.Println(table.Render())
}

func (c *streamCmd) renderStreams(stats []streamStat) {
	table := newTableWriter("Stream Report")
	table.AddHeaders("Stream", "Storage", "Consumers", "Messages", "Bytes", "Lost", "Deleted", "Replicas")

	for _, s := range stats {
		lost := "0"
		if c.reportRaw {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%d (%d)", s.LostMsgs, s.LostBytes)
			}
			table.AddRow(s.Name, s.Storage, s.Consumers, s.Msgs, s.Bytes, lost, s.Deleted, renderCluster(s.Cluster))
		} else {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%s (%s)", humanize.Comma(int64(s.LostMsgs)), humanize.IBytes(s.LostBytes))
			}
			table.AddRow(s.Name, s.Storage, s.Consumers, humanize.Comma(s.Msgs), humanize.IBytes(s.Bytes), lost, s.Deleted, renderCluster(s.Cluster))
		}
	}

	fmt.Println(table.Render())
}

func (c *streamCmd) loadConfigFile(file string) (*api.StreamConfig, error) {
	var cfg api.StreamConfig
	f, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(f, &cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Name == "" {
		cfg.Name = c.stream
	}

	return &cfg, nil
}

func (c *streamCmd) copyAndEditStream(cfg api.StreamConfig) (api.StreamConfig, error) {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		if err != nil {
			return api.StreamConfig{}, err
		}

		if cfg.Name == "" {
			cfg.Name = c.stream
		}

		return *cfg, nil
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

	if c.maxBytesLimit != 0 {
		cfg.MaxBytes = c.maxBytesLimit
	}

	if c.maxMsgLimit != 0 {
		cfg.MaxMsgs = c.maxMsgLimit
	}

	if c.maxMsgPerSubjectLimit != 0 {
		cfg.MaxMsgsPer = c.maxMsgPerSubjectLimit
	}

	if c.maxAgeLimit != "" {
		cfg.MaxAge, err = parseDurationString(c.maxAgeLimit)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid maximum age limit format: %v", err)
		}
	}

	if c.maxMsgSize != 0 {
		cfg.MaxMsgSize = int32(c.maxMsgSize)
	}

	if c.maxConsumers != -1 {
		cfg.MaxConsumers = c.maxConsumers
	}

	if c.dupeWindow != "" {
		dw, err := parseDurationString(c.dupeWindow)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid duplicate window: %v", err)
		}
		cfg.Duplicates = dw
	}

	if c.replicas != 0 {
		cfg.Replicas = int(c.replicas)
	}

	if c.placementCluster != "" {
		if cfg.Placement == nil {
			cfg.Placement = &api.Placement{}
		}
		cfg.Placement.Cluster = c.placementCluster
	}

	if len(c.placementTags) == 0 {
		if cfg.Placement == nil {
			cfg.Placement = &api.Placement{}
		}
		cfg.Placement.Tags = c.placementTags
	}

	if cfg.Placement.Cluster == "" {
		cfg.Placement = nil
	}

	if len(c.sources) > 0 || c.mirror != "" {
		return cfg, fmt.Errorf("cannot edit mirrors or sources using the CLI, use --config instead")
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	return cfg, nil
}

func (c *streamCmd) interactiveEdit(cfg api.StreamConfig) (api.StreamConfig, error) {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		return api.StreamConfig{}, fmt.Errorf("set EDITOR environment variable to your chosen editor")
	}

	cj, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile, err := ioutil.TempFile("", "")
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}
	defer os.Remove(tfile.Name())

	_, err = fmt.Fprint(tfile, string(cj))
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile.Close()

	cmd := exec.Command(editor, tfile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	ncfg, err := c.loadConfigFile(tfile.Name())
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	return *ncfg, nil
}

func (c *streamCmd) editAction(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	var cfg api.StreamConfig
	if c.interactive {
		cfg, err = c.interactiveEdit(sourceStream.Configuration())
		kingpin.FatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	} else {
		cfg, err = c.copyAndEditStream(sourceStream.Configuration())
		kingpin.FatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	}

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

func (c *streamCmd) cpAction(_ *kingpin.ParseContext) error {
	if c.stream == c.destination {
		kingpin.Fatalf("source and destination Stream names cannot be the same")
	}

	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	cfg, err := c.copyAndEditStream(sourceStream.Configuration())
	kingpin.FatalIfError(err, "could not copy Stream %s", c.stream)

	cfg.Name = c.destination

	new, err := c.mgr.NewStreamFromDefault(cfg.Name, cfg)
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
	if cfg.Description != "" {
		fmt.Printf("      Description: %s\n", cfg.Description)
	}
	if len(cfg.Subjects) > 0 {
		fmt.Printf("             Subjects: %s\n", strings.Join(cfg.Subjects, ", "))
	}
	fmt.Printf("     Acknowledgements: %v\n", !cfg.NoAck)
	fmt.Printf("            Retention: %s - %s\n", cfg.Storage.String(), cfg.Retention.String())
	fmt.Printf("             Replicas: %d\n", cfg.Replicas)
	fmt.Printf("       Discard Policy: %s\n", cfg.Discard.String())
	fmt.Printf("     Duplicate Window: %v\n", cfg.Duplicates)
	if cfg.MaxMsgs == -1 {
		fmt.Println("     Maximum Messages: unlimited")
	} else {
		fmt.Printf("     Maximum Messages: %s\n", humanize.Comma(cfg.MaxMsgs))
	}
	if cfg.MaxMsgsPer > 0 {
		fmt.Printf("  Maximum Per Subject: %s\n", humanize.Comma(cfg.MaxMsgsPer))
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
	if cfg.Placement != nil {
		if cfg.Placement.Cluster != "" {
			fmt.Printf("    Placement Cluster: %s\n", cfg.Placement.Cluster)
		}
		if len(cfg.Placement.Tags) > 0 {
			fmt.Printf("       Placement Tags: %s\n", strings.Join(cfg.Placement.Tags, ", "))
		}
	}
	if cfg.Mirror != nil {
		fmt.Printf("               Mirror: %s\n", c.renderSource(cfg.Mirror))
	}
	if len(cfg.Sources) > 0 {
		fmt.Printf("              Sources: ")
		sort.Slice(cfg.Sources, func(i, j int) bool {
			return cfg.Sources[i].Name < cfg.Sources[j].Name
		})

		for i, source := range cfg.Sources {
			if i == 0 {
				fmt.Println(c.renderSource(source))
			} else {
				fmt.Printf("                       %s\n", c.renderSource(source))
			}
		}
	}

	fmt.Println()
}

func (c *streamCmd) renderSource(s *api.StreamSource) string {
	parts := []string{s.Name}
	if s.OptStartSeq > 0 {
		parts = append(parts, fmt.Sprintf("Start Seq: %s", humanize.Comma(int64(s.OptStartSeq))))
	}

	if s.OptStartTime != nil {
		parts = append(parts, fmt.Sprintf("Start Time: %v", s.OptStartTime))
	}
	if s.FilterSubject != "" {
		parts = append(parts, fmt.Sprintf("Subject: %s", s.FilterSubject))
	}
	if s.External != nil {
		parts = append(parts, fmt.Sprintf("API Prefix: %s", s.External.ApiPrefix))
		parts = append(parts, fmt.Sprintf("Delivery Prefix: %s", s.External.DeliverPrefix))
	}

	return strings.Join(parts, ", ")
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

	fmt.Printf("Information for Stream %s created %s\n", c.stream, info.Created.Local().Format(time.RFC3339))
	fmt.Println()
	c.showStreamConfig(info.Config)
	fmt.Println()

	if info.Cluster != nil && info.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                 Name: %s\n", info.Cluster.Name)
		fmt.Printf("               Leader: %s\n", info.Cluster.Leader)
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

			fmt.Printf("              Replica: %s\n", strings.Join(state, ", "))

		}
		fmt.Println()
	}
	showSource := func(s *api.StreamSourceInfo) {
		fmt.Printf("          Stream Name: %s\n", s.Name)
		fmt.Printf("                  Lag: %s\n", humanize.Comma(int64(s.Lag)))
		if s.Active > 0 && s.Active < math.MaxInt64 {
			fmt.Printf("            Last Seen: %v\n", humanizeDuration(s.Active))
		} else {
			fmt.Printf("            Last Seen: never\n")
		}
		if s.External != nil {
			fmt.Printf("      Ext. API Prefix: %s\n", s.External.ApiPrefix)
			if s.External.DeliverPrefix != "" {
				fmt.Printf(" Ext. Delivery Prefix: %s\n", s.External.DeliverPrefix)
			}
		}
		if s.Error != nil {
			fmt.Printf("                Error: %s\n", s.Error.Description)
		}
	}
	if info.Mirror != nil {
		fmt.Println("Mirror Information:")
		fmt.Println()
		showSource(info.Mirror)
		fmt.Println()
	}

	if len(info.Sources) > 0 {
		fmt.Println("Source Information:")
		fmt.Println()
		for _, s := range info.Sources {
			showSource(s)
			fmt.Println()
		}
	}

	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("             Messages: %s\n", humanize.Comma(int64(info.State.Msgs)))
	fmt.Printf("                Bytes: %s\n", humanize.IBytes(info.State.Bytes))
	if info.State.Lost != nil && len(info.State.Lost.Msgs) > 0 {
		fmt.Printf("        Lost Messages: %s (%s)\n", humanize.Comma(int64(len(info.State.Lost.Msgs))), humanize.IBytes(info.State.Lost.Bytes))
	}

	if info.State.FirstTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("             FirstSeq: %s\n", humanize.Comma(int64(info.State.FirstSeq)))
	} else {
		fmt.Printf("             FirstSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.FirstSeq)), info.State.FirstTime.Format("2006-01-02T15:04:05"))
	}

	if info.State.LastTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("              LastSeq: %s\n", humanize.Comma(int64(info.State.LastSeq)))
	} else {
		fmt.Printf("              LastSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.LastSeq)), info.State.LastTime.Format("2006-01-02T15:04:05"))
	}

	if len(info.State.Deleted) > 0 { // backwards compat with older servers
		fmt.Printf("     Deleted Messages: %d\n", len(info.State.Deleted))
	} else if info.State.NumDeleted > 0 {
		fmt.Printf("     Deleted Messages: %d\n", info.State.NumDeleted)
	}

	fmt.Printf("     Active Consumers: %d\n", info.State.Consumers)
}

func (c *streamCmd) infoAction(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
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

func (c *streamCmd) prepareConfig() api.StreamConfig {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		kingpin.FatalIfError(err, "invalid input")

		if c.stream != "" {
			cfg.Name = c.stream
		}

		if c.stream == "" {
			c.stream = cfg.Name
		}

		if len(c.subjects) > 0 {
			cfg.Subjects = c.subjects
		}

		return *cfg
	}

	if c.stream == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Stream Name",
		}, &c.stream, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.mirror == "" && len(c.sources) == 0 {
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
	}

	if c.mirror != "" && len(c.subjects) > 0 {
		kingpin.Fatalf("mirrors cannot listen for messages on subjects")
	}

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

	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Stream Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		kingpin.FatalIfError(err, "invalid input")
		if c.maxMsgLimit <= 0 {
			c.maxMsgLimit = -1
		}
	}

	if c.maxMsgPerSubjectLimit == 0 && len(c.subjects) > 0 && (len(c.subjects) > 0 || strings.Contains(c.subjects[0], "*") || strings.Contains(c.subjects[0], ">")) {
		c.maxMsgPerSubjectLimit, err = askOneInt("Per Subject Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream per unique subject, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs-per-subject")
		kingpin.FatalIfError(err, "invalid input")
		if c.maxMsgPerSubjectLimit <= 0 {
			c.maxMsgPerSubjectLimit = -1
		}
	}

	var maxAge time.Duration
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
		c.maxMsgSize, err = askOneBytes("Maximum individual message size", "-1", "Defines the maximum size any single message may be to be accepted by the Stream. Settable using --max-msg-size")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxMsgSize == 0 {
		c.maxMsgSize = -1
	}

	var dupeWindow time.Duration
	if c.dupeWindow == "" && c.mirror == "" {
		defaultDW := (2 * time.Minute).String()
		if maxAge > 0 && maxAge < 2*time.Minute {
			defaultDW = maxAge.String()
		}
		err = survey.AskOne(&survey.Input{
			Message: "Duplicate tracking time window",
			Default: defaultDW,
			Help:    "Duplicate messages are identified by the Msg-Id headers and tracked within a window of this size. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --dupe-window.",
		}, &c.dupeWindow)
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.dupeWindow != "" {
		dupeWindow, err = parseDurationString(c.dupeWindow)
		kingpin.FatalIfError(err, "invalid duplicate window format")
	}

	if c.replicas == 0 {
		c.replicas, err = askOneInt("Replicas", "1", "When clustered, defines how many replicas of the data to store.  Settable using --replicas.")
		kingpin.FatalIfError(err, "invalid input")
	}
	if c.replicas <= 0 {
		kingpin.Fatalf("replicas should be >= 1")
	}

	cfg := api.StreamConfig{
		Name:         c.stream,
		Description:  c.description,
		Subjects:     c.subjects,
		MaxMsgs:      c.maxMsgLimit,
		MaxMsgsPer:   c.maxMsgPerSubjectLimit,
		MaxBytes:     c.maxBytesLimit,
		MaxMsgSize:   int32(c.maxMsgSize),
		Duplicates:   dupeWindow,
		MaxAge:       maxAge,
		Storage:      storage,
		NoAck:        !c.ack,
		Retention:    c.retentionPolicyFromString(),
		Discard:      c.discardPolicyFromString(),
		MaxConsumers: c.maxConsumers,
		Replicas:     int(c.replicas),
	}

	if c.placementCluster != "" || len(c.placementTags) > 0 {
		cfg.Placement = &api.Placement{
			Cluster: c.placementCluster,
			Tags:    c.placementTags,
		}
	}

	if c.mirror != "" {
		if isJsonString(c.mirror) {
			cfg.Mirror, err = c.parseStreamSource(c.mirror)
			kingpin.FatalIfError(err, "invalid mirror")
		} else {
			cfg.Mirror = c.askMirror()
		}
	}

	for _, source := range c.sources {
		if isJsonString(source) {
			ss, err := c.parseStreamSource(source)
			kingpin.FatalIfError(err, "invalid source")
			cfg.Sources = append(cfg.Sources, ss)
		} else {
			ss := c.askSource(source, fmt.Sprintf("%s Source", source))
			cfg.Sources = append(cfg.Sources, ss)
		}
	}

	return cfg
}

func (c *streamCmd) askMirror() *api.StreamSource {
	mirror := &api.StreamSource{Name: c.mirror}
	ok, err := askConfirmation("Adjust mirror start", false)
	kingpin.FatalIfError(err, "Could not request mirror details")
	if ok {
		a, err := askOneInt("Mirror Start Sequence", "0", "Start mirroring at a specific sequence")
		kingpin.FatalIfError(err, "Invalid sequence")
		mirror.OptStartSeq = uint64(a)

		if mirror.OptStartSeq == 0 {
			ts := ""
			err = survey.AskOne(&survey.Input{
				Message: "Mirror Start Time (YYYY:MM:DD HH:MM:SS)",
				Help:    "Start replicating as a specific time stamp in UTC time",
			}, &ts)
			kingpin.FatalIfError(err, "could not request start time")
			if ts != "" {
				t, err := time.Parse("2006:01:02 15:04:05", ts)
				kingpin.FatalIfError(err, "invalid time format")
				mirror.OptStartTime = &t
			}
		}
	}

	ok, err = askConfirmation("Import mirror from a different JetStream domain", false)
	kingpin.FatalIfError(err, "Could not request mirror details")
	if ok {
		mirror.External = &api.ExternalStream{}
		domainName := ""
		err = survey.AskOne(&survey.Input{
			Message: "Foreign JetStream domain name",
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "Could not request mirror details")
		mirror.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		err = survey.AskOne(&survey.Input{
			Message: "Delivery prefix",
			Help:    "Optional prefix of the delivery subject",
		}, &mirror.External.DeliverPrefix)
		kingpin.FatalIfError(err, "Could not request mirror details")
		return mirror
	}

	ok, err = askConfirmation("Import mirror from a different account", false)
	kingpin.FatalIfError(err, "Could not request mirror details")
	if !ok {
		return mirror
	}

	mirror.External = &api.ExternalStream{}
	err = survey.AskOne(&survey.Input{
		Message: "Foreign account API prefix",
		Help:    "The prefix where the foreign account JetStream API has been imported",
	}, &mirror.External.ApiPrefix, survey.WithValidator(survey.Required))
	kingpin.FatalIfError(err, "Could not request mirror details")

	err = survey.AskOne(&survey.Input{
		Message: "Foreign account delivery prefix",
		Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
	}, &mirror.External.DeliverPrefix, survey.WithValidator(survey.Required))
	kingpin.FatalIfError(err, "Could not request mirror details")

	return mirror
}

func (c *streamCmd) askSource(name string, prefix string) *api.StreamSource {
	cfg := &api.StreamSource{Name: name}

	ok, err := askConfirmation(fmt.Sprintf("Adjust source %q start", name), false)
	kingpin.FatalIfError(err, "Could not request source details")
	if ok {
		a, err := askOneInt(fmt.Sprintf("%s Start Sequence", prefix), "0", "Start mirroring at a specific sequence")
		kingpin.FatalIfError(err, "Invalid sequence")
		cfg.OptStartSeq = uint64(a)

		ts := ""
		err = survey.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s UTC Time Stamp (YYYY:MM:DD HH:MM:SS)", prefix),
			Help:    "Start replicating as a specific time stamp",
		}, &ts)
		kingpin.FatalIfError(err, "could not request start time")
		if ts != "" {
			t, err := time.Parse("2006:01:02 15:04:05", ts)
			kingpin.FatalIfError(err, "invalid time format")
			cfg.OptStartTime = &t
		}

		err = survey.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s Filter source by subject", prefix),
			Help:    "Only replicate data matching this subject",
		}, &cfg.FilterSubject)
		kingpin.FatalIfError(err, "could not request filter")
	}

	ok, err = askConfirmation(fmt.Sprintf("Import %q from a different JetStream domain", name), false)
	kingpin.FatalIfError(err, "Could not request source details")
	if ok {
		cfg.External = &api.ExternalStream{}
		domainName := ""
		err = survey.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign JetStream domain name", prefix),
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "Could not request source details")
		cfg.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		err = survey.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign JetStream domain delivery prefix", prefix),
			Help:    "Optional prefix of the delivery subject",
		}, &cfg.External.DeliverPrefix)
		kingpin.FatalIfError(err, "Could not request source details")
		return cfg
	}

	ok, err = askConfirmation(fmt.Sprintf("Import %q from a different account", name), false)
	kingpin.FatalIfError(err, "Could not request source details")
	if !ok {
		return cfg
	}

	cfg.External = &api.ExternalStream{}
	err = survey.AskOne(&survey.Input{
		Message: fmt.Sprintf("%s foreign account API prefix", prefix),
		Help:    "The prefix where the foreign account JetStream API has been imported",
	}, &cfg.External.ApiPrefix, survey.WithValidator(survey.Required))
	kingpin.FatalIfError(err, "Could not request source details")

	err = survey.AskOne(&survey.Input{
		Message: fmt.Sprintf("%s foreign account delivery prefix", prefix),
		Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
	}, &cfg.External.DeliverPrefix, survey.WithValidator(survey.Required))
	kingpin.FatalIfError(err, "Could not request source details")

	return cfg
}

func (c *streamCmd) parseStreamSource(source string) (*api.StreamSource, error) {
	ss := &api.StreamSource{}
	if isJsonString(source) {
		err := json.Unmarshal([]byte(source), ss)
		if err != nil {
			return nil, err
		}

		if ss.Name == "" {
			return nil, fmt.Errorf("name is required")
		}
	} else {
		ss.Name = source
	}

	return ss, nil
}

func (c *streamCmd) validateCfg(cfg *api.StreamConfig) (bool, []byte, []string, error) {
	if os.Getenv("NOVALIDATE") != "" {
		return true, nil, nil, nil
	}

	j, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, nil, nil, err
	}

	if !cfg.NoAck {
		for _, subject := range cfg.Subjects {
			if subject == ">" {
				return false, j, []string{"subjects cannot be '>' when acknowledgement is enabled"}, nil
			}
		}
	}

	valid, errs := cfg.Validate(new(SchemaValidator))

	return valid, j, errs, nil
}

func (c *streamCmd) addAction(_ *kingpin.ParseContext) (err error) {
	cfg := c.prepareConfig()

	switch {
	case c.validateOnly:
		valid, j, errs, err := c.validateCfg(&cfg)
		if err != nil {
			return err
		}

		fmt.Println(string(j))
		fmt.Println()
		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Printf("Configuration is a valid Stream matching %s\n", cfg.SchemaType())
		return nil

	case c.outFile != "":
		valid, j, errs, err := c.validateCfg(&cfg)
		kingpin.FatalIfError(err, "Could not validate configuration")

		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		return ioutil.WriteFile(c.outFile, j, 0644)
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "could not create Stream")

	str, err := mgr.NewStreamFromDefault(c.stream, cfg)
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

	stream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not remove Stream")

	err = stream.Delete()
	kingpin.FatalIfError(err, "could not remove Stream")

	return nil
}

func (c *streamCmd) purgeAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge Stream %s", c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not purge Stream")

	var req *api.JSApiStreamPurgeRequest
	if c.purgeKeep > 0 || c.purgeSubject != "" || c.purgeSequence > 0 {
		if c.purgeSequence > 0 && c.purgeKeep > 0 {
			return fmt.Errorf("sequence and keep cannot be combined when purghing")
		}

		req = &api.JSApiStreamPurgeRequest{
			Sequence: c.purgeSequence,
			Subject:  c.purgeSubject,
			Keep:     c.purgeKeep,
		}
	}

	err = stream.Purge(req)
	kingpin.FatalIfError(err, "could not purge Stream")

	stream.Reset()

	c.showStream(stream)

	return nil
}

func (c *streamCmd) lsAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	streams, err := mgr.StreamNames(&jsm.StreamNamesFilter{Subject: c.filterSubject})
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

	if c.filterSubject == "" {
		fmt.Println("Streams:")
	} else {
		fmt.Printf("Streams matching %q:\n", c.filterSubject)
	}

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
			Message: "Message Sequence to remove",
		}, &id, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		kingpin.FatalIfError(err, "invalid number")

		if idint <= 0 {
			return fmt.Errorf("positive message ID required")
		}
		c.msgID = int64(idint)
	}

	stream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Stream %s", c.stream)

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove message %d from Stream %s", c.msgID, c.stream), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	return stream.DeleteMessage(uint64(c.msgID))
}

func (c *streamCmd) getAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskStream()

	if c.msgID == -1 && c.filterSubject == "" {
		id := ""
		err = survey.AskOne(&survey.Input{
			Message: "Message Sequence to retrieve",
			Default: "-1",
		}, &id, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		kingpin.FatalIfError(err, "invalid number")

		c.msgID = int64(idint)

		if c.msgID == -1 {
			err = survey.AskOne(&survey.Input{
				Message: "Subject to retrieve last message for",
			}, &c.filterSubject)
			kingpin.FatalIfError(err, "invalid subject")
		}
	}

	stream, err := c.loadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Stream %s", c.stream)

	var item *api.StoredMsg
	if c.msgID > -1 {
		item, err = stream.ReadMessage(uint64(c.msgID))
	} else if c.filterSubject != "" {
		item, err = stream.ReadLastMessageForSubject(c.filterSubject)
	} else {
		return fmt.Errorf("no ID or subject specified")
	}
	kingpin.FatalIfError(err, "could not retrieve %s#%d", c.stream, c.msgID)

	if c.json {
		printJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v on Subject %s\n\n", c.stream, item.Sequence, item.Time, item.Subject)

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
	var err error

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.stream, c.selectedStream, err = selectStream(c.mgr, c.stream, c.force)
	kingpin.FatalIfError(err, "could not pick a Stream to operate on")
}

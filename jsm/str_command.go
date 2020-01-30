// Copyright 2019 The NATS Authors
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
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	api "github.com/nats-io/nats-server/v2/server"
	"github.com/xlab/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jetstream/internal/jsch"
)

type streamCmd struct {
	stream           string
	force            bool
	json             bool
	msgID            int64
	retentionPolicyS string

	destination         string
	subjects            []string
	ack                 bool
	storage             string
	maxMsgLimit         int64
	maxBytesLimit       int64
	maxAgeLimit         string
	maxMsgSize          int32
	rPolicy             api.RetentionPolicy
	reportSortConsumers bool
	reportSortMsgs      bool
	reportSortName      bool
	reportRaw           bool
	maxStreams          int
}

func configureStreamCommand(app *kingpin.Application) {
	c := &streamCmd{msgID: -1}

	str := app.Command("stream", "Stream management").Alias("str").Alias("st").Alias("ms").Alias("s")

	strInfo := str.Command("info", "Stream information").Alias("nfo").Alias("i").Action(c.infoAction)
	strInfo.Arg("stream", "Stream to retrieve information for").StringVar(&c.stream)
	strInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("subjects", "Subjects that are consumed by the Stream").Default().StringsVar(&c.subjects)
		f.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
		f.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
		f.Flag("max-bytes", "Maximum bytes to keep").Default("0").Int64Var(&c.maxBytesLimit)
		f.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
		f.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
		f.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
		f.Flag("retention", "Defines a retention policy (limits, interest, work)").EnumVar(&c.retentionPolicyS, "limits", "interest", "workq", "work")
		f.Flag("max-msg-size", "Maximum size any 1 message may be").Int32Var(&c.maxMsgSize)
	}

	strAdd := str.Command("create", "Create a new Stream").Alias("add").Alias("new").Action(c.addAction)
	strAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	addCreateFlags(strAdd)

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

	strGet := str.Command("get", "Retrieves a specific message from a Stream").Action(c.getAction)
	strGet.Arg("stream", "Stream name").StringVar(&c.stream)
	strGet.Arg("id", "Message ID to retrieve").Int64Var(&c.msgID)
	strGet.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	strReport := str.Command("report", "Reports on Stream statistics").Action(c.reportAction)
	strReport.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	strReport.Flag("consumers", "Sort by number of Consumers").Short('o').BoolVar(&c.reportSortConsumers)
	strReport.Flag("messages", "Sort by number of Messages").Short('m').BoolVar(&c.reportSortMsgs)
	strReport.Flag("name", "Sort by Stream name").Short('n').BoolVar(&c.reportSortName)
	strReport.Flag("raw", "Show un-formatted numbers").Short('r').BoolVar(&c.reportRaw)

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

func (c *streamCmd) streamTemplateRm(_ *kingpin.ParseContext) (err error) {
	nc, err := newNatsConn(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")
	jsch.SetConnection(nc)

	c.stream, err = selectStreamTemplate(c.stream)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	template, err := jsch.LoadStreamTemplate(c.stream)
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

	_, err = jsch.NewStreamTemplate(c.stream, uint32(c.maxStreams), cfg)
	kingpin.FatalIfError(err, "could not create Stream Template")

	fmt.Printf("Stream Template %s was created\n\n", c.stream)

	return c.streamTemplateInfo(pc)
}

func (c *streamCmd) streamTemplateInfo(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")
	jsch.SetConnection(nc)

	c.stream, err = selectStreamTemplate(c.stream)
	kingpin.FatalIfError(err, "could not pick a Stream Template to operate on")

	info, err := jsch.LoadStreamTemplate(c.stream)
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

	names, err := jsch.StreamTemplateNames()
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
	}

	if !c.json {
		fmt.Print("Obtaining Stream stats\n\n")
	}

	stats := []stat{}
	jsch.EachStream(func(stream *jsch.Stream) {
		info, err := stream.Information()
		kingpin.FatalIfError(err, "could not get stream info for %s", stream.Name())
		stats = append(stats, stat{info.Config.Name, info.State.Consumers, int64(info.State.Msgs), info.State.Bytes})
	})

	if len(stats) == 0 {
		if !c.json {
			fmt.Println("No Streams defined")
		}
		return nil
	}

	if c.json {
		j, err := json.MarshalIndent(stats, "", "  ")
		kingpin.FatalIfError(err, "could not JSON marshal stats")
		fmt.Println(string(j))
		return nil
	}

	if c.reportSortConsumers {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Consumers < stats[j].Consumers })
	} else if c.reportSortMsgs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Msgs < stats[j].Msgs })
	} else if c.reportSortName {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	} else {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Bytes < stats[j].Bytes })
	}

	table := tablewriter.CreateTable()
	table.AddHeaders("Stream", "Consumers", "Messages", "Bytes")

	for _, s := range stats {
		if c.reportRaw {
			table.AddRow(s.Name, s.Consumers, s.Msgs, s.Bytes)
		} else {
			table.AddRow(s.Name, s.Consumers, humanize.Comma(s.Msgs), humanize.IBytes(s.Bytes))
		}
	}

	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) cpAction(pc *kingpin.ParseContext) error {
	if c.stream == c.destination {
		kingpin.Fatalf("source and destination Stream names cannot be the same")
	}

	c.connectAndAskStream()

	sourceStream, err := jsch.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	source, err := sourceStream.Information()
	kingpin.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	cfg := source.Config
	cfg.NoAck = !c.ack
	cfg.Name = c.destination

	if len(c.subjects) > 0 {
		cfg.Subjects = c.splitCLISubjects()
	}

	if c.storage != "" {
		cfg.Storage = c.storeTypeFromString(c.storage)
	}

	if c.retentionPolicyS != "" {
		cfg.Retention = c.retentionPolicyFromString(strings.ToLower(c.storage))
	}

	if c.maxBytesLimit != 0 {
		cfg.MaxBytes = c.maxBytesLimit
	}

	if c.maxMsgLimit != 0 {
		cfg.MaxMsgs = c.maxMsgLimit
	}

	if c.maxAgeLimit != "" {
		cfg.MaxAge, err = parseDurationString(c.maxAgeLimit)
		kingpin.FatalIfError(err, "invalid maximum age limit format")
	}

	if c.maxMsgSize != 0 {
		cfg.MaxMsgSize = c.maxMsgSize
	}

	_, err = jsch.NewStreamFromDefault(cfg.Name, cfg)
	kingpin.FatalIfError(err, "could not create Stream")

	fmt.Printf("Stream %s was created\n\n", c.stream)

	c.stream = c.destination
	return c.infoAction(pc)
}

func (c *streamCmd) showStreamConfig(cfg api.StreamConfig) {
	fmt.Println("Configuration:")
	fmt.Println()
	fmt.Printf("             Subjects: %s\n", strings.Join(cfg.Subjects, ", "))
	fmt.Printf("     Acknowledgements: %v\n", !cfg.NoAck)
	fmt.Printf("            Retention: %s - %s\n", cfg.Storage.String(), cfg.Retention.String())
	fmt.Printf("             Replicas: %d\n", cfg.Replicas)
	fmt.Printf("     Maximum Messages: %d\n", cfg.MaxMsgs)
	fmt.Printf("        Maximum Bytes: %d\n", cfg.MaxBytes)
	fmt.Printf("          Maximum Age: %s\n", cfg.MaxAge.String())
	fmt.Printf(" Maximum Message Size: %d\n", cfg.MaxMsgSize)
	fmt.Printf("    Maximum Consumers: %d\n", cfg.MaxConsumers)
	if cfg.Template != "" {
		fmt.Printf("  Managed by Template: %s\n", cfg.Template)
	}
}

func (c *streamCmd) infoAction(_ *kingpin.ParseContext) error {
	c.connectAndAskStream()

	stream, err := jsch.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not request Stream info")
	mstats, err := stream.Information()
	kingpin.FatalIfError(err, "could not request Stream info")

	if c.json {
		err = printJSON(mstats)
		kingpin.FatalIfError(err, "could not display info")
		return nil
	}

	fmt.Printf("Information for Stream %s\n", c.stream)
	fmt.Println()
	c.showStreamConfig(mstats.Config)
	fmt.Println()
	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("            Messages: %s\n", humanize.Comma(int64(mstats.State.Msgs)))
	fmt.Printf("               Bytes: %s\n", humanize.IBytes(mstats.State.Bytes))
	fmt.Printf("            FirstSeq: %s\n", humanize.Comma(int64(mstats.State.FirstSeq)))
	fmt.Printf("             LastSeq: %s\n", humanize.Comma(int64(mstats.State.LastSeq)))
	fmt.Printf("    Active Consumers: %d\n", mstats.State.Consumers)

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

func (c *streamCmd) storeTypeFromString(s string) api.StorageType {
	switch s {
	case "file", "f":
		return api.FileStorage
	case "memory", "m":
		return api.MemoryStorage
	default:
		kingpin.Fatalf("invalid storage type %s", c.storage)
		return 0 // unreachable
	}
}

func (c *streamCmd) retentionPolicyFromString(s string) api.RetentionPolicy {
	switch strings.ToLower(c.retentionPolicyS) {
	case "limits":
		return api.LimitsPolicy
	case "interest":
		return api.InterestPolicy
	case "work queue", "workq", "work":
		return api.WorkQueuePolicy
	default:
		kingpin.Fatalf("invalid retention policy %s", c.retentionPolicyS)
		return 0 // unreachable
	}
}

func (c *streamCmd) prepareConfig() (cfg api.StreamConfig) {
	var err error

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

	c.rPolicy = c.retentionPolicyFromString(strings.ToLower(c.retentionPolicyS))

	var maxAge time.Duration
	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Message count limit", "-1", "Defines the amount of messages to keep in the store for this Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit == 0 {
		c.maxBytesLimit, err = askOneInt("Message size limit", "-1", "Defines the combined size of all messages in a Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-bytes")
		kingpin.FatalIfError(err, "invalid input")
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

	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "could not create Stream")

	cfg = api.StreamConfig{
		Name:         c.stream,
		Subjects:     c.subjects,
		MaxMsgs:      c.maxMsgLimit,
		MaxBytes:     c.maxBytesLimit,
		MaxMsgSize:   c.maxMsgSize,
		MaxAge:       maxAge,
		Storage:      storage,
		NoAck:        !c.ack,
		Retention:    c.rPolicy,
		MaxConsumers: -1,
		Replicas:     0,
	}

	return cfg
}

func (c *streamCmd) addAction(pc *kingpin.ParseContext) (err error) {
	cfg := c.prepareConfig()

	_, err = jsch.NewStreamFromDefault(c.stream, cfg)
	kingpin.FatalIfError(err, "could not create Stream")

	fmt.Printf("Stream %s was created\n\n", c.stream)

	return c.infoAction(pc)
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

	stream, err := jsch.LoadStream(c.stream)
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

	stream, err := jsch.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not purge Stream")

	err = stream.Purge()
	kingpin.FatalIfError(err, "could not purge Stream")

	return c.infoAction(pc)
}

func (c *streamCmd) lsAction(_ *kingpin.ParseContext) (err error) {
	prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	streams, err := jsch.StreamNames()
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

func (c *streamCmd) getAction(_ *kingpin.ParseContext) (err error) {
	prepareHelper(servers, natsOpts()...)

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

	stream, err := jsch.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Stream %s", c.stream)

	item, err := stream.LoadMessage(int(c.msgID))
	kingpin.FatalIfError(err, "could not retrieve %s#%d", c.stream, c.msgID)

	if c.json {
		printJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v on Subject %s\n\n", c.stream, c.msgID, item.Time, item.Subject)
	fmt.Println(string(item.Data))
	fmt.Println()
	return nil
}

func (c *streamCmd) connectAndAskStream() {
	nc, err := newNatsConn(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")
	jsch.SetConnection(nc)

	c.stream, err = selectStream(c.stream)
	kingpin.FatalIfError(err, "could not pick a Stream to operate on")
}

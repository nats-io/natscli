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

	"github.com/nats-io/jetstream/jsch"
)

type msCmd struct {
	set              string
	force            bool
	json             bool
	msgID            int64
	retentionPolicyS string

	destination    string
	subjects       []string
	ack            bool
	storage        string
	maxMsgLimit    int64
	maxBytesLimit  int64
	maxAgeLimit    string
	maxMsgSize     int32
	rPolicy        api.RetentionPolicy
	reportSortObs  bool
	reportSortMsgs bool
	reportSortName bool
	reportRaw      bool
}

func configureMSCommand(app *kingpin.Application) {
	c := &msCmd{msgID: -1}

	ms := app.Command("messageset", "Message Set management").Alias("ms")

	msInfo := ms.Command("info", "Message Set information").Alias("nfo").Action(c.infoAction)
	msInfo.Arg("set", "Message Set to retrieve information for").StringVar(&c.set)
	msInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("subjects", "Subjects that belong to the Message Set").Default().StringsVar(&c.subjects)
		f.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
		f.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
		f.Flag("max-bytes", "Maximum bytes to keep").Default("0").Int64Var(&c.maxBytesLimit)
		f.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
		f.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
		f.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
		f.Flag("retention", "Defines a retention policy (stream, interest, workq)").EnumVar(&c.retentionPolicyS, "stream", "interest", "workq", "work")
		f.Flag("max-msg-size", "Maximum size any 1 message may be").Int32Var(&c.maxMsgSize)
	}

	msAdd := ms.Command("create", "Create a new Message Set").Alias("add").Alias("new").Action(c.addAction)
	msAdd.Arg("name", "Message Set name").StringVar(&c.set)
	addCreateFlags(msAdd)

	msCopy := ms.Command("copy", "Creates a new Message Set based on the configuration of another").Alias("cp").Action(c.cpAction)
	msCopy.Arg("source", "Source Message Set to copy").Required().StringVar(&c.set)
	msCopy.Arg("destination", "New Message Set to create").Required().StringVar(&c.destination)
	addCreateFlags(msCopy)

	msRm := ms.Command("rm", "Removes a Message Set").Alias("delete").Alias("del").Action(c.rmAction)
	msRm.Arg("name", "Message Set name").StringVar(&c.set)
	msRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	msLs := ms.Command("ls", "List all known Message Sets").Alias("list").Alias("l").Action(c.lsAction)
	msLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	msPurge := ms.Command("purge", "Purge a Message Set witout deleting it").Action(c.purgeAction)
	msPurge.Arg("name", "Message Set name").StringVar(&c.set)
	msPurge.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	msPurge.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	msGet := ms.Command("get", "Retrieves a specific message from a Message Set").Action(c.getAction)
	msGet.Arg("name", "Message Set name").StringVar(&c.set)
	msGet.Arg("id", "Message ID to retrieve").Int64Var(&c.msgID)
	msGet.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	msReport := ms.Command("report", "Reports on Message Set statistics").Action(c.reportAction)
	msReport.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	msReport.Flag("observables", "Sort by number of Observables").Short('o').BoolVar(&c.reportSortObs)
	msReport.Flag("messages", "Sort by number of Messages").Short('m').BoolVar(&c.reportSortMsgs)
	msReport.Flag("name", "Sort by Message Set name").Short('n').BoolVar(&c.reportSortName)
	msReport.Flag("raw", "Show un-formatted numbers").Short('r').BoolVar(&c.reportRaw)
}

func (c *msCmd) reportAction(pc *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	type stat struct {
		Name        string
		Observables int
		Msgs        int64
		Bytes       uint64
	}

	if !c.json {
		fmt.Print("Obtaining Message Set stats\n\n")
	}

	stats := []stat{}
	jsch.EachStream(func(stream *jsch.Stream) {
		info, err := stream.Information()
		kingpin.FatalIfError(err, "could not get set info for %s", stream.Name())
		stats = append(stats, stat{info.Config.Name, info.Stats.Observables, int64(info.Stats.Msgs), info.Stats.Bytes})
	})

	if len(stats) == 0 {
		if !c.json {
			fmt.Println("No sets defined")
		}
		return nil
	}

	if c.json {
		j, err := json.MarshalIndent(stats, "", "  ")
		kingpin.FatalIfError(err, "could not JSON marshal stats")
		fmt.Println(string(j))
		return nil
	}

	if c.reportSortObs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Observables < stats[j].Observables })
	} else if c.reportSortMsgs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Msgs < stats[j].Msgs })
	} else if c.reportSortName {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	} else {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Bytes < stats[j].Bytes })
	}

	table := tablewriter.CreateTable()
	table.AddHeaders("Message Set", "Observables", "Messages", "Bytes")

	for _, s := range stats {
		if c.reportRaw {
			table.AddRow(s.Name, s.Observables, s.Msgs, s.Bytes)
		} else {
			table.AddRow(s.Name, s.Observables, humanize.Comma(s.Msgs), humanize.IBytes(s.Bytes))
		}
	}

	fmt.Println(table.Render())

	return nil
}

func (c *msCmd) cpAction(pc *kingpin.ParseContext) error {
	if c.set == c.destination {
		kingpin.Fatalf("source and destination set names cannot be the same")
	}

	c.connectAndAskSet()

	sourceStream, err := jsch.LoadStream(c.set)
	kingpin.FatalIfError(err, "could not request Message Set %s configuration", c.set)

	source, err := sourceStream.Information()
	kingpin.FatalIfError(err, "could not request Message Set %s configuration", c.set)

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

	_, err = jsch.NewStreamFromTemplate(cfg.Name, cfg)
	kingpin.FatalIfError(err, "could not create Message Set")

	fmt.Printf("Message Set %s was created\n\n", c.set)

	c.set = c.destination
	return c.infoAction(pc)
}

func (c *msCmd) infoAction(_ *kingpin.ParseContext) error {
	c.connectAndAskSet()

	stream, err := jsch.LoadStream(c.set)
	kingpin.FatalIfError(err, "could not request Message Set info")
	mstats, err := stream.Information()
	kingpin.FatalIfError(err, "could not request Message Set info")

	if c.json {
		err = printJSON(mstats)
		kingpin.FatalIfError(err, "could not display info")
		return nil
	}

	fmt.Printf("Information for Message Set %s\n", c.set)
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	fmt.Printf("             Subjects: %s\n", strings.Join(mstats.Config.Subjects, ", "))
	fmt.Printf("     Acknowledgements: %v\n", !mstats.Config.NoAck)
	fmt.Printf("            Retention: %s - %s\n", mstats.Config.Storage.String(), mstats.Config.Retention.String())
	fmt.Printf("             Replicas: %d\n", mstats.Config.Replicas)
	fmt.Printf("     Maximum Messages: %d\n", mstats.Config.MaxMsgs)
	fmt.Printf("        Maximum Bytes: %d\n", mstats.Config.MaxBytes)
	fmt.Printf("          Maximum Age: %s\n", mstats.Config.MaxAge.String())
	fmt.Printf(" Maximum Message Size: %d\n", mstats.Config.MaxMsgSize)
	fmt.Printf("  Maximum Observables: %d\n", mstats.Config.MaxObservables)
	fmt.Println()
	fmt.Println("Statistics:")
	fmt.Println()
	fmt.Printf("            Messages: %s\n", humanize.Comma(int64(mstats.Stats.Msgs)))
	fmt.Printf("               Bytes: %s\n", humanize.IBytes(mstats.Stats.Bytes))
	fmt.Printf("            FirstSeq: %s\n", humanize.Comma(int64(mstats.Stats.FirstSeq)))
	fmt.Printf("             LastSeq: %s\n", humanize.Comma(int64(mstats.Stats.LastSeq)))
	fmt.Printf("  Active Observables: %d\n", mstats.Stats.Observables)

	fmt.Println()

	return nil
}

func (c *msCmd) splitCLISubjects() []string {
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

func (c *msCmd) storeTypeFromString(s string) api.StorageType {
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

func (c *msCmd) retentionPolicyFromString(s string) api.RetentionPolicy {
	switch strings.ToLower(c.retentionPolicyS) {
	case "stream":
		return api.StreamPolicy
	case "interest":
		return api.InterestPolicy
	case "work queue", "workq", "work":
		return api.WorkQueuePolicy
	default:
		kingpin.Fatalf("invalid retention policy %s", c.retentionPolicyS)
		return 0 // unreachable
	}
}

func (c *msCmd) addAction(pc *kingpin.ParseContext) (err error) {
	if c.set == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Message Set Name",
		}, &c.set, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	if len(c.subjects) == 0 {
		subjects := ""
		err = survey.AskOne(&survey.Input{
			Message: "Subjects to consume",
			Help:    "Message Sets consume messages from subjects, this is a space or comma separated list that can include wildcards. Settable using --subjects",
		}, &subjects, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")

		c.subjects = splitString(subjects)
	}

	c.subjects = c.splitCLISubjects()

	if c.storage == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Storage backend",
			Options: []string{"file", "memory"},
			Help:    "Messages sets are stored on the server, this can be one of many backends and all are usable in clustering mode. Settable using --storage",
		}, &c.storage, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	storage := c.storeTypeFromString(c.storage)

	if c.retentionPolicyS == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Retention Policy",
			Options: []string{"Stream", "Interest", "Work Queue"},
			Help:    "Messages are retained either based on limits like size and age (Stream), as long as there are observables (Interest) or until any worker processed them (Work Queue)",
			Default: "Stream",
		}, &c.retentionPolicyS, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "invalid input")
	}

	c.rPolicy = c.retentionPolicyFromString(strings.ToLower(c.retentionPolicyS))

	var maxAge time.Duration
	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Message count limit", "-1", "Defines the amount of messages to keep in the store for this Message Set, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit == 0 {
		c.maxBytesLimit, err = askOneInt("Message size limit", "-1", "Defines the combined size of all messages in a Message Set, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-bytes")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxAgeLimit == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum message age limit",
			Default: "-1",
			Help:    "Defines the oldest messages that can be stored in the Message Set, any messages older than this period will be removed, -1 for unlimited. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --max-age",
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
			Help:    "Defines the maximum size any single message may be to be accepted by the Message Set. Settable using --max-age",
		}, &c.maxMsgSize)
		kingpin.FatalIfError(err, "invalid input")
	}

	_, err = prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "could not create Message Set")

	_, err = jsch.NewStreamFromTemplate(c.set, api.MsgSetConfig{
		Name:           c.set,
		Subjects:       c.subjects,
		MaxMsgs:        c.maxMsgLimit,
		MaxBytes:       c.maxBytesLimit,
		MaxMsgSize:     c.maxMsgSize,
		MaxAge:         maxAge,
		Storage:        storage,
		NoAck:          !c.ack,
		Retention:      c.rPolicy,
		MaxObservables: -1,
		Replicas:       0,
	})
	kingpin.FatalIfError(err, "could not create Message Set")

	fmt.Printf("Message Set %s was created\n\n", c.set)

	return c.infoAction(pc)
}

func (c *msCmd) rmAction(_ *kingpin.ParseContext) (err error) {
	c.connectAndAskSet()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete Message Set %s", c.set), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := jsch.LoadStream(c.set)
	kingpin.FatalIfError(err, "could not remove Message Set")

	err = stream.Delete()
	kingpin.FatalIfError(err, "could not remove Message Set")

	return nil
}

func (c *msCmd) purgeAction(pc *kingpin.ParseContext) (err error) {
	c.connectAndAskSet()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge Message Set %s", c.set), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := jsch.LoadStream(c.set)
	kingpin.FatalIfError(err, "could not purge Message Set")

	err = stream.Purge()
	kingpin.FatalIfError(err, "could not purge Message Set")

	return c.infoAction(pc)
}

func (c *msCmd) lsAction(_ *kingpin.ParseContext) (err error) {
	prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	sets, err := jsch.StreamNames()
	kingpin.FatalIfError(err, "could not list Message Set")

	if c.json {
		err = printJSON(sets)
		kingpin.FatalIfError(err, "could not display sets")
		return nil
	}

	if len(sets) == 0 {
		fmt.Println("No Message Sets defined")
		return nil
	}

	fmt.Println("Message Sets:")
	fmt.Println()
	for _, s := range sets {
		fmt.Printf("\t%s\n", s)
	}
	fmt.Println()

	return nil
}

func (c *msCmd) getAction(_ *kingpin.ParseContext) (err error) {
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

	stream, err := jsch.LoadStream(c.set)
	kingpin.FatalIfError(err, "could not load stream %s", c.set)

	item, err := stream.LoadMessage(int(c.msgID))
	kingpin.FatalIfError(err, "could not retrieve %s#%d", c.set, c.msgID)

	if c.json {
		printJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v on Subject %s\n\n", c.set, c.msgID, item.Time, item.Subject)
	fmt.Println(string(item.Data))
	fmt.Println()
	return nil
}

func (c *msCmd) connectAndAskSet() {
	nc, err := newNatsConn(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")
	jsch.SetConnection(nc)

	c.set, err = selectMessageSet(c.set)
	kingpin.FatalIfError(err, "could not pick a Message Set")
}

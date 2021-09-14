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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go"
)

type consumerCmd struct {
	consumer    string
	stream      string
	json        bool
	force       bool
	ack         bool
	raw         bool
	destination string
	inputFile   string
	outFile     string

	selectedConsumer *jsm.Consumer

	ackPolicy           string
	ackWait             time.Duration
	bpsRateLimit        uint64
	delivery            string
	ephemeral           bool
	filterSubject       string
	idleHeartbeat       string
	maxAckPending       int
	maxDeliver          int
	maxWaiting          int
	deliveryGroup       string
	pull                bool
	pullCount           int
	replayPolicy        string
	reportLeaderDistrib bool
	samplePct           int
	startPolicy         string
	validateOnly        bool
	description         string

	mgr *jsm.Manager
	nc  *nats.Conn
}

func configureConsumerCommand(app *kingpin.Application) {
	c := &consumerCmd{}

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("ack", "Acknowledgement policy (none, all, explicit)").StringVar(&c.ackPolicy)
		f.Flag("bps", "Restrict message delivery to a certain bit per second").Default("0").Uint64Var(&c.bpsRateLimit)
		f.Flag("deliver", "Start policy (all, new, last, subject, 1h, msg sequence)").PlaceHolder("POLICY").StringVar(&c.startPolicy)
		f.Flag("deliver-group", "Delivers push messages only to subscriptions matching this group").Default("_unset_").PlaceHolder("GROUP").StringVar(&c.deliveryGroup)
		f.Flag("description", "Sets a contextual description for the consumer").StringVar(&c.description)
		f.Flag("ephemeral", "Create an ephemeral Consumer").Default("false").BoolVar(&c.ephemeral)
		f.Flag("filter", "Filter Stream by subjects").Default("_unset_").StringVar(&c.filterSubject)
		OptionalBoolean(f.Flag("flow-control", "Enable Push consumer flow control"))
		f.Flag("heartbeat", "Enable idle Push consumer heartbeats (-1 disable)").StringVar(&c.idleHeartbeat)
		f.Flag("max-deliver", "Maximum amount of times a message will be delivered").PlaceHolder("TRIES").IntVar(&c.maxDeliver)
		f.Flag("max-outstanding", "Maximum pending Acks before consumers are paused").Hidden().Default("-1").IntVar(&c.maxAckPending)
		f.Flag("max-pending", "Maximum pending Acks before consumers are paused").Default("-1").IntVar(&c.maxAckPending)
		f.Flag("max-waiting", "Maximum number of outstanding pulls allowed").PlaceHolder("PULLS").IntVar(&c.maxWaiting)
		f.Flag("pull", "Deliver messages in 'pull' mode").BoolVar(&c.pull)
		f.Flag("replay", "Replay Policy (instant, original)").PlaceHolder("POLICY").EnumVar(&c.replayPolicy, "instant", "original")
		f.Flag("sample", "Percentage of requests to sample for monitoring purposes").Default("-1").IntVar(&c.samplePct)
		f.Flag("target", "Push based delivery target subject").PlaceHolder("SUBJECT").StringVar(&c.delivery)
		f.Flag("wait", "Acknowledgement waiting time").Default("-1s").DurationVar(&c.ackWait)
	}

	cons := app.Command("consumer", "JetStream Consumer management").Alias("con").Alias("obs").Alias("c")

	consAdd := cons.Command("add", "Creates a new Consumer").Alias("create").Alias("new").Action(c.createAction)
	consAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	consAdd.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	consAdd.Flag("validate", "Only validates the configuration against the official Schema").BoolVar(&c.validateOnly)
	consAdd.Flag("output", "Save configuration instead of creating").PlaceHolder("FILE").StringVar(&c.outFile)
	addCreateFlags(consAdd)

	consCp := cons.Command("copy", "Creates a new Consumer based on the configuration of another").Alias("cp").Action(c.cpAction)
	consCp.Arg("stream", "Stream name").Required().StringVar(&c.stream)
	consCp.Arg("source", "Source Consumer name").Required().StringVar(&c.consumer)
	consCp.Arg("destination", "Destination Consumer name").Required().StringVar(&c.destination)
	addCreateFlags(consCp)

	consInfo := cons.Command("info", "Consumer information").Alias("nfo").Action(c.infoAction)
	consInfo.Arg("stream", "Stream name").StringVar(&c.stream)
	consInfo.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	consLs := cons.Command("ls", "List known Consumers").Alias("list").Action(c.lsAction)
	consLs.Arg("stream", "Stream name").StringVar(&c.stream)
	consLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	consNext := cons.Command("next", "Retrieves messages from Pull Consumers without interactive prompts").Action(c.nextAction)
	consNext.Arg("stream", "Stream name").Required().StringVar(&c.stream)
	consNext.Arg("consumer", "Consumer name").Required().StringVar(&c.consumer)
	consNext.Flag("ack", "Acknowledge received message").Default("true").BoolVar(&c.ack)
	consNext.Flag("raw", "Show only the message").Short('r').BoolVar(&c.raw)
	consNext.Flag("wait", "Wait up to this period to acknowledge messages").DurationVar(&c.ackWait)
	consNext.Flag("count", "Number of messages to try to fetch from the pull consumer").Default("1").IntVar(&c.pullCount)

	consRm := cons.Command("rm", "Removes a Consumer").Alias("delete").Alias("del").Action(c.rmAction)
	consRm.Arg("stream", "Stream name").StringVar(&c.stream)
	consRm.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	consSub := cons.Command("sub", "Retrieves messages from Consumers").Action(c.subAction)
	consSub.Arg("stream", "Stream name").StringVar(&c.stream)
	consSub.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consSub.Flag("ack", "Acknowledge received message").Default("true").BoolVar(&c.ack)
	consSub.Flag("raw", "Show only the message").Short('r').BoolVar(&c.raw)

	conCluster := cons.Command("cluster", "Manages a clustered Consumer").Alias("c")
	conClusterDown := conCluster.Command("step-down", "Force a new leader election by standing down the current leader").Alias("elect").Alias("down").Alias("d").Action(c.leaderStandDown)
	conClusterDown.Arg("stream", "Stream to act on").StringVar(&c.stream)
	conClusterDown.Arg("consumer", "Consumer to act on").StringVar(&c.consumer)

	conReport := cons.Command("report", "Reports on Consmer statistics").Action(c.reportAction)
	conReport.Arg("stream", "Stream name").StringVar(&c.stream)
	conReport.Flag("raw", "Show un-formatted numbers").Short('r').BoolVar(&c.raw)
	conReport.Flag("leaders", "Show details about the leaders").Short('l').BoolVar(&c.reportLeaderDistrib)
}

func (c *consumerCmd) leaderStandDown(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, true)

	consumer, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	if err != nil {
		return err
	}

	info, err := consumer.LatestState()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("consumer %q > %q is not clustered", consumer.StreamName(), consumer.Name())
	}

	leader := info.Cluster.Leader
	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(info.Cluster.Replicas)+1)
	err = consumer.LeaderStepDown()
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTimer(500 * time.Millisecond).C {
		if ctr == 5 {
			return fmt.Errorf("consumer did not elect a new leader in time")
		}
		ctr++

		info, err = consumer.State()
		if err != nil {
			log.Printf("Failed to retrieve Consumer State: %s", err)
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
	c.showConsumer(consumer)
	return nil
}

func (c *consumerCmd) rmAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, true)

	var err error

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete Consumer %s > %s", c.stream, c.consumer), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if c.selectedConsumer == nil {
		c.selectedConsumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		kingpin.FatalIfError(err, "could not load Consumer")
	}

	return c.selectedConsumer.Delete()
}

func (c *consumerCmd) lsAction(pc *kingpin.ParseContext) error {
	c.connectAndSetup(true, false)

	stream, err := c.mgr.LoadStream(c.stream)
	kingpin.FatalIfError(err, "could not load Consumers")

	consumers, err := stream.ConsumerNames()
	kingpin.FatalIfError(err, "could not load Consumers")

	if c.json {
		err = printJSON(consumers)
		kingpin.FatalIfError(err, "could not display Consumers")
		return nil
	}

	if len(consumers) == 0 {
		fmt.Println("No Consumers defined")
		return nil
	}

	fmt.Printf("Consumers for Stream %s:\n", c.stream)
	fmt.Println()
	for _, sc := range consumers {
		fmt.Printf("\t%s\n", sc)
	}
	fmt.Println()

	return nil
}

func (c *consumerCmd) showConsumer(consumer *jsm.Consumer) {
	config := consumer.Configuration()
	state, err := consumer.LatestState()
	kingpin.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)

	c.showInfo(config, state)
}

func (c *consumerCmd) showInfo(config api.ConsumerConfig, state api.ConsumerInfo) {
	if c.json {
		printJSON(state)
		return
	}

	fmt.Printf("Information for Consumer %s > %s created %s\n", state.Stream, state.Name, state.Created.Local().Format(time.RFC3339))
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	if config.Durable != "" {
		fmt.Printf("        Durable Name: %s\n", config.Durable)
	}
	if config.Description != "" {
		fmt.Printf("         Description: %s\n", config.Description)
	}
	if config.DeliverSubject != "" {
		fmt.Printf("    Delivery Subject: %s\n", config.DeliverSubject)
	} else {
		fmt.Printf("           Pull Mode: true\n")
	}
	if config.FilterSubject != "" {
		fmt.Printf("      Filter Subject: %s\n", config.FilterSubject)
	}
	switch config.DeliverPolicy {
	case api.DeliverAll:
		fmt.Printf("      Deliver Policy: All\n")
	case api.DeliverLast:
		fmt.Printf("      Deliver Policy: Last\n")
	case api.DeliverNew:
		fmt.Printf("      Deliver Policy: New\n")
	case api.DeliverLastPerSubject:
		fmt.Printf("      Deliver Policy: Last Per Subject\n")
	case api.DeliverByStartTime:
		fmt.Printf("      Deliver Policy: Since %v\n", config.OptStartTime)
	case api.DeliverByStartSequence:
		fmt.Printf("      Deliver Policy: From Sequence %d\n", config.OptStartSeq)
	}
	if config.DeliverGroup != "" && config.DeliverSubject != "" {
		fmt.Printf(" Deliver Queue Group: %s\n", config.DeliverGroup)
	}
	fmt.Printf("          Ack Policy: %s\n", config.AckPolicy.String())
	if config.AckPolicy != api.AckNone {
		fmt.Printf("            Ack Wait: %v\n", config.AckWait)
	}
	fmt.Printf("       Replay Policy: %s\n", config.ReplayPolicy.String())
	if config.MaxDeliver != -1 {
		fmt.Printf("  Maximum Deliveries: %d\n", config.MaxDeliver)
	}
	if config.SampleFrequency != "" {
		fmt.Printf("       Sampling Rate: %s\n", config.SampleFrequency)
	}
	if config.RateLimit > 0 {
		fmt.Printf("          Rate Limit: %s / second\n", humanize.IBytes(config.RateLimit/8))
	}
	if config.MaxAckPending > 0 {
		fmt.Printf("     Max Ack Pending: %s\n", humanize.Comma(int64(config.MaxAckPending)))
	}
	if config.MaxWaiting > 0 {
		fmt.Printf("   Max Waiting Pulls: %s\n", humanize.Comma(int64(config.MaxWaiting)))
	}
	if config.Heartbeat > 0 {
		fmt.Printf("      Idle Heartbeat: %s\n", humanizeDuration(config.Heartbeat))
	}
	if config.DeliverSubject != "" {
		fmt.Printf("        Flow Control: %v\n", config.FlowControl)
	}

	fmt.Println()

	if state.Cluster != nil && state.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                Name: %s\n", state.Cluster.Name)
		fmt.Printf("              Leader: %s\n", state.Cluster.Leader)
		for _, r := range state.Cluster.Replicas {
			since := fmt.Sprintf("seen %s ago", humanizeDuration(r.Active))
			if r.Active == 0 || r.Active == math.MaxInt64 {
				since = "not seen"
			}

			if r.Current {
				fmt.Printf("             Replica: %s, current, %s\n", r.Name, since)
			} else {
				fmt.Printf("             Replica: %s, outdated, %s\n", r.Name, since)
			}
		}
		fmt.Println()
	}

	fmt.Println("State:")
	fmt.Println()
	if state.Delivered.Last == nil {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %d Stream sequence: %d\n", state.Delivered.Consumer, state.Delivered.Stream)
	} else {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %d Stream sequence: %d Last delivery: %s ago\n", state.Delivered.Consumer, state.Delivered.Stream, humanizeDuration(time.Since(*state.Delivered.Last)))
	}

	if config.AckPolicy != api.AckNone {
		if state.AckFloor.Last == nil {
			fmt.Printf("     Acknowledgment floor: Consumer sequence: %d Stream sequence: %d\n", state.AckFloor.Consumer, state.AckFloor.Stream)
		} else {
			fmt.Printf("     Acknowledgment floor: Consumer sequence: %d Stream sequence: %d Last Ack: %s ago\n", state.AckFloor.Consumer, state.AckFloor.Stream, humanizeDuration(time.Since(*state.AckFloor.Last)))
		}
		if config.MaxAckPending > 0 {
			fmt.Printf("         Outstanding Acks: %d out of maximum %d\n", state.NumAckPending, config.MaxAckPending)
		} else {
			fmt.Printf("         Outstanding Acks: %d\n", state.NumAckPending)
		}
		fmt.Printf("     Redelivered Messages: %d\n", state.NumRedelivered)
	}

	fmt.Printf("     Unprocessed Messages: %d\n", state.NumPending)
	if config.DeliverSubject == "" {
		if config.MaxWaiting > 0 {
			fmt.Printf("            Waiting Pulls: %d of maximum %d\n", state.NumWaiting, config.MaxWaiting)
		} else {
			fmt.Printf("            Waiting Pulls: %d of unlimited\n", state.NumWaiting)
		}
	} else {
		if state.PushBound {
			if config.DeliverGroup != "" {
				fmt.Printf("          Active Interest: Active using Queue Group %s", config.DeliverGroup)
			} else {
				fmt.Printf("          Active Interest: Active")
			}
		} else {
			fmt.Printf("          Active Interest: No interest")
		}
	}

	fmt.Println()
}

func (c *consumerCmd) infoAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, true)

	var err error
	consumer := c.selectedConsumer
	if consumer == nil {
		consumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		kingpin.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)
	}

	c.showConsumer(consumer)

	return nil
}

func (c *consumerCmd) replayPolicyFromString(p string) api.ReplayPolicy {
	switch strings.ToLower(p) {
	case "instant":
		return api.ReplayInstant
	case "original":
		return api.ReplayOriginal
	default:
		kingpin.Fatalf("invalid replay policy '%s'", p)
		return api.ReplayInstant
	}
}

func (c *consumerCmd) ackPolicyFromString(p string) api.AckPolicy {
	switch strings.ToLower(p) {
	case "none":
		return api.AckNone
	case "all":
		return api.AckAll
	case "explicit":
		return api.AckExplicit
	default:
		kingpin.Fatalf("invalid ack policy '%s'", p)
		// unreachable
		return api.AckExplicit
	}
}

func (c *consumerCmd) sampleFreqFromString(s int) string {
	if s > 100 || s < 0 {
		kingpin.Fatalf("sample percent is not between 0 and 100")
	}

	if s > 0 {
		return strconv.Itoa(c.samplePct)
	}

	return ""
}

func (c *consumerCmd) defaultConsumer() *api.ConsumerConfig {
	return &api.ConsumerConfig{
		AckPolicy:    api.AckExplicit,
		ReplayPolicy: api.ReplayInstant,
	}
}

func (c *consumerCmd) setStartPolicy(cfg *api.ConsumerConfig, policy string) {
	if policy == "" {
		return
	}

	if policy == "all" {
		cfg.DeliverPolicy = api.DeliverAll
	} else if policy == "last" {
		cfg.DeliverPolicy = api.DeliverLast
	} else if policy == "new" || policy == "next" {
		cfg.DeliverPolicy = api.DeliverNew
	} else if policy == "subject" || policy == "last_per_subject" {
		cfg.DeliverPolicy = api.DeliverLastPerSubject
	} else if ok, _ := regexp.MatchString("^\\d+$", policy); ok {
		seq, _ := strconv.Atoi(policy)
		cfg.DeliverPolicy = api.DeliverByStartSequence
		cfg.OptStartSeq = uint64(seq)
	} else {
		d, err := parseDurationString(policy)
		kingpin.FatalIfError(err, "could not parse starting delta")
		t := time.Now().UTC().Add(-d)
		cfg.DeliverPolicy = api.DeliverByStartTime
		cfg.OptStartTime = &t
	}
}

func (c *consumerCmd) cpAction(pc *kingpin.ParseContext) (err error) {
	c.connectAndSetup(true, false)

	source, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	kingpin.FatalIfError(err, "could not load source Consumer")

	cfg := source.Configuration()

	if c.ackWait > 0 {
		cfg.AckWait = c.ackWait
	}

	if c.samplePct != -1 {
		cfg.SampleFrequency = c.sampleFreqFromString(c.samplePct)
	}

	if c.startPolicy != "" {
		c.setStartPolicy(&cfg, c.startPolicy)
	}

	if c.ephemeral {
		cfg.Durable = ""
	} else {
		cfg.Durable = c.destination
	}

	if c.delivery != "" {
		cfg.DeliverSubject = c.delivery
	}

	if c.pull {
		cfg.DeliverSubject = ""
		c.ackPolicy = "explicit"
		cfg.MaxWaiting = c.maxWaiting
	}

	if c.ackPolicy != "" {
		cfg.AckPolicy = c.ackPolicyFromString(c.ackPolicy)
	}

	if c.filterSubject != "_unset_" {
		cfg.FilterSubject = c.filterSubject
	}

	if c.replayPolicy != "" {
		cfg.ReplayPolicy = c.replayPolicyFromString(c.replayPolicy)
	}

	if c.maxDeliver != 0 {
		cfg.MaxDeliver = c.maxDeliver
	}

	if c.bpsRateLimit != 0 {
		cfg.RateLimit = c.bpsRateLimit
	}

	if c.maxAckPending != -1 {
		cfg.MaxAckPending = c.maxAckPending
	}

	if c.idleHeartbeat != "" && c.idleHeartbeat != "-" {
		hb, err := parseDurationString(c.idleHeartbeat)
		kingpin.FatalIfError(err, "Invalid heartbeat duration")
		cfg.Heartbeat = hb
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	fc := pc.SelectedCommand.GetFlag("flow-control").Model().Value.(*OptionalBoolValue)
	if fc.IsSetByUser() {
		cfg.FlowControl = fc.Value()
	}

	if cfg.DeliverSubject == "" {
		cfg.Heartbeat = 0
		cfg.FlowControl = false
	}

	if c.deliveryGroup != "_unset_" {
		cfg.DeliverGroup = c.deliveryGroup
	}

	if c.delivery == "" {
		cfg.DeliverGroup = ""
	}

	consumer, err := c.mgr.NewConsumerFromDefault(c.stream, cfg)
	kingpin.FatalIfError(err, "Consumer creation failed")

	if cfg.Durable == "" {
		return nil
	}

	c.consumer = cfg.Durable

	c.showConsumer(consumer)

	return nil
}

func (c *consumerCmd) prepareConfig(pc *kingpin.ParseContext) (cfg *api.ConsumerConfig, err error) {
	cfg = c.defaultConsumer()
	cfg.Description = c.description

	if c.inputFile != "" {
		f, err := ioutil.ReadFile(c.inputFile)
		if err != nil {
			return nil, err
		}

		cfg = &api.ConsumerConfig{}
		err = json.Unmarshal(f, cfg)

		if cfg.Durable != "" && c.consumer != "" && cfg.Durable != c.consumer {
			if c.consumer != "" {
				cfg.Durable = c.consumer
			} else {
				return cfg, fmt.Errorf("durable consumer name in %s does not match CLI consumer name %s", c.inputFile, c.consumer)
			}
		}

		if cfg.DeliverSubject != "" && c.delivery != "" {
			cfg.DeliverSubject = c.delivery
		}

		return cfg, err
	}

	if c.consumer == "" && !c.ephemeral {
		err = survey.AskOne(&survey.Input{
			Message: "Consumer name",
			Help:    "This will be used for the name of the durable subscription to be used when referencing this Consumer later. Settable using 'name' CLI argument",
		}, &c.consumer, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "could not request durable name")
	}
	cfg.Durable = c.consumer

	if ok, _ := regexp.MatchString(`\.|\*|>`, cfg.Durable); ok {
		kingpin.Fatalf("durable name can not contain '.', '*', '>'")
	}

	if !c.pull && c.delivery == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Delivery target (empty for Pull Consumers)",
			Help:    "Consumers can be in 'push' or 'pull' mode, in 'push' mode messages are dispatched in real time to a target NATS subject, this is that subject. Leaving this blank creates a 'pull' mode Consumer. Settable using --target and --pull",
		}, &c.delivery)
		kingpin.FatalIfError(err, "could not request delivery target")
	}

	if c.ephemeral && c.delivery == "" {
		kingpin.Fatalf("ephemeral Consumers has to be push-based")
	}

	cfg.DeliverSubject = c.delivery

	// pull is always explicit
	if c.delivery == "" {
		c.ackPolicy = "explicit"
	}

	if cfg.DeliverSubject != "" && c.deliveryGroup == "_unset_" {
		err = survey.AskOne(&survey.Input{
			Message: "Delivery Queue Group",
			Help:    "When set push consumers will only deliver messages to subscriptions matching this queue group",
		}, &c.deliveryGroup)
		kingpin.FatalIfError(err, "could not request delivery group")
	}
	cfg.DeliverGroup = c.deliveryGroup

	if c.startPolicy == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Start policy (all, new, last, subject, 1h, msg sequence)",
			Help:    "This controls how the Consumer starts out, does it make all messages available, only the latest, latest per subject, ones after a certain time or time sequence. Settable using --deliver",
		}, &c.startPolicy, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "could not request start policy")
	}

	c.setStartPolicy(cfg, c.startPolicy)

	if c.ackPolicy == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Acknowledgement policy",
			Options: []string{"explicit", "all", "none"},
			Default: "none",
			Help:    "Messages that are not acknowledged will be redelivered at a later time. 'none' means no acknowledgement is needed only 1 delivery ever, 'all' means acknowledging message 10 will also acknowledge 0-9 and 'explicit' means each has to be acknowledged specifically. Settable using --ack",
		}, &c.ackPolicy)
		kingpin.FatalIfError(err, "could not ask acknowledgement policy")
	}

	if c.replayPolicy == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Replay policy",
			Options: []string{"instant", "original"},
			Default: "instant",
			Help:    "Messages can be replayed at the rate they arrived in or as fast as possible. Settable using --replay",
		}, &c.replayPolicy)
		kingpin.FatalIfError(err, "could not ask replay policy")
	}

	cfg.AckPolicy = c.ackPolicyFromString(c.ackPolicy)
	if cfg.AckPolicy == api.AckNone {
		cfg.MaxDeliver = -1
	}

	if c.ackWait > 0 {
		cfg.AckWait = c.ackWait
	}

	if c.samplePct > 0 {
		if c.samplePct > 100 {
			kingpin.Fatalf("sample percent is not between 0 and 100")
		}

		cfg.SampleFrequency = strconv.Itoa(c.samplePct)
	}

	if cfg.DeliverSubject != "" {
		if c.replayPolicy == "" {
			mode := ""
			err = survey.AskOne(&survey.Select{
				Message: "Replay policy",
				Options: []string{"instant", "original"},
				Default: "instant",
				Help:    "Replay policy is the time interval at which messages are delivered to interested parties. 'instant' means deliver all as soon as possible while 'original' will match the time intervals in which messages were received, useful for replaying production traffic in development. Settable using --replay",
			}, &mode)
			kingpin.FatalIfError(err, "could not ask replay policy")
			c.replayPolicy = mode
		}
	}

	if c.replayPolicy != "" {
		cfg.ReplayPolicy = c.replayPolicyFromString(c.replayPolicy)
	}

	if c.filterSubject == "_unset_" {
		err = survey.AskOne(&survey.Input{
			Message: "Filter Stream by subject (blank for all)",
			Default: "",
			Help:    "Stream can consume more than one subject - or a wildcard - this allows you to filter out just a single subject from all the ones entering the Stream for delivery to the Consumer. Settable using --filter",
		}, &c.filterSubject)
		kingpin.FatalIfError(err, "could not ask for filtering subject")
	}
	cfg.FilterSubject = c.filterSubject
	if cfg.FilterSubject == "" && cfg.DeliverPolicy == api.DeliverLastPerSubject {
		cfg.FilterSubject = ">"
	}

	if c.maxDeliver == 0 && cfg.AckPolicy != api.AckNone {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum Allowed Deliveries",
			Default: "-1",
			Help:    "When this is -1 unlimited attempts to deliver an un acknowledged message is made, when this is >0 it will be maximum amount of times a message is delivered after which it is ignored. Settable using --max-deliver.",
		}, &c.maxDeliver)
		kingpin.FatalIfError(err, "could not ask for maximum allowed deliveries")
	}

	if c.maxAckPending == -1 && cfg.AckPolicy != api.AckNone {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum Acknowledgements Pending",
			Default: "0",
			Help:    "The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended. Settable using --max-pending.",
		}, &c.maxAckPending)
		kingpin.FatalIfError(err, "could not ask for maximum outstanding acknowledgements")
	}

	if cfg.DeliverSubject != "" {
		if c.idleHeartbeat == "-1" {
			cfg.Heartbeat = 0
		} else if c.idleHeartbeat != "" {
			cfg.Heartbeat, err = parseDurationString(c.idleHeartbeat)
			kingpin.FatalIfError(err, "invalid heartbeat duration")
		} else {
			idle := "0s"
			err = survey.AskOne(&survey.Input{
				Message: "Idle Heartbeat",
				Help:    "When a Push consumer is idle for the given period an empty message with a Status header of 100 will be sent to the delivery subject, settable using --heartbeat",
				Default: "0s",
			}, &idle)
			kingpin.FatalIfError(err, "could not ask for idle heartbeat")
			cfg.Heartbeat, err = parseDurationString(idle)
			kingpin.FatalIfError(err, "invalid heartbeat duration")
		}
	}

	if cfg.DeliverSubject != "" {
		fc := pc.SelectedCommand.GetFlag("flow-control").Model().Value.(*OptionalBoolValue)
		if !fc.IsSetByUser() {
			flow, err := askConfirmation("Enable Flow Control, ie --flow-control", false)
			kingpin.FatalIfError(err, "could not ask flow control")
			fc.SetBool(flow)
		}

		cfg.FlowControl = fc.Value()
	}

	if c.maxAckPending == -1 {
		c.maxAckPending = 0
	}

	cfg.MaxAckPending = c.maxAckPending

	if cfg.DeliverSubject == "" {
		cfg.MaxWaiting = c.maxWaiting
	}

	if c.maxDeliver != 0 && cfg.AckPolicy != api.AckNone {
		cfg.MaxDeliver = c.maxDeliver
	}

	if c.bpsRateLimit > 0 && cfg.DeliverSubject == "" {
		return nil, fmt.Errorf("rate limits are only possible on Push consumers")
	}
	cfg.RateLimit = c.bpsRateLimit

	return cfg, nil
}

func (c *consumerCmd) validateCfg(cfg *api.ConsumerConfig) (bool, []byte, []string, error) {
	j, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, nil, nil, err
	}

	if os.Getenv("NOVALIDATE") != "" {
		return true, nil, nil, nil
	}

	valid, errs := cfg.Validate(new(SchemaValidator))

	return valid, j, errs, nil
}

func (c *consumerCmd) createAction(pc *kingpin.ParseContext) (err error) {
	cfg, err := c.prepareConfig(pc)
	if err != nil {
		return err
	}

	switch {
	case c.validateOnly:
		valid, j, errs, err := c.validateCfg(cfg)
		kingpin.FatalIfError(err, "Could not validate configuration")

		fmt.Println(string(j))
		fmt.Println()
		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Println("Configuration is a valid Consumer")
		return nil

	case c.outFile != "":
		valid, j, errs, err := c.validateCfg(cfg)
		kingpin.FatalIfError(err, "Could not validate configuration")

		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		return ioutil.WriteFile(c.outFile, j, 0644)
	}

	c.connectAndSetup(true, false)

	created, err := c.mgr.NewConsumerFromDefault(c.stream, *cfg)
	kingpin.FatalIfError(err, "Consumer creation failed")

	c.consumer = created.Name()

	c.showConsumer(created)

	return nil
}

func (c *consumerCmd) getNextMsgDirect(stream string, consumer string) error {
	req := &api.JSApiConsumerGetNextRequest{Batch: 1, Expires: timeout}

	sub, err := c.nc.SubscribeSync(nats.NewInbox())
	kingpin.FatalIfError(err, "subscribe failed")
	sub.AutoUnsubscribe(1)

	err = c.mgr.NextMsgRequest(stream, consumer, sub.Subject, req)
	kingpin.FatalIfError(err, "could not request next message")

	fatalIfNotPull := func() {
		cons, err := c.mgr.LoadConsumer(stream, consumer)
		kingpin.FatalIfError(err, "could not load consumer %q", consumer)

		if !cons.IsPullMode() {
			kingpin.Fatalf("consumer %q is not a Pull consumer", consumer)
		}
	}

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		fatalIfNotPull()
	}
	kingpin.FatalIfError(err, "no message received")

	if msg.Header != nil && msg.Header.Get("Status") == "503" {
		fatalIfNotPull()
	}

	if !c.raw {
		info, err := jsm.ParseJSMsgMetadata(msg)
		if err != nil {
			if msg.Reply == "" {
				fmt.Printf("--- subject: %s\n", msg.Subject)
			} else {
				fmt.Printf("--- subject: %s reply: %s\n", msg.Subject, msg.Reply)
			}

		} else {
			fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %d\n", time.Now().Format("15:04:05"), msg.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence(), info.Pending())
		}

		if len(msg.Header) > 0 {
			fmt.Println()
			fmt.Println("Headers:")
			fmt.Println()
			for h, vals := range msg.Header {
				for _, val := range vals {
					fmt.Printf("  %s: %s\n", h, val)
				}
			}

			fmt.Println()
			fmt.Println("Data:")
			fmt.Println()
		}

		fmt.Println()
		fmt.Println(string(msg.Data))
	} else {
		fmt.Println(string(msg.Data))
	}

	if c.ack {
		var stime time.Duration
		if c.ackWait > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			stime = time.Duration(r.Intn(int(c.ackWait)))

		}

		if stime > 0 {
			time.Sleep(stime)
		}

		err = msg.Respond(nil)
		kingpin.FatalIfError(err, "could not Acknowledge message")
		c.nc.Flush()
		if !c.raw {
			if stime > 0 {
				fmt.Printf("\nAcknowledged message after %s delay\n", stime)
			} else {
				fmt.Println("\nAcknowledged message")
			}
			fmt.Println()
		}
	}

	return nil
}

func (c *consumerCmd) subscribeConsumer(consumer *jsm.Consumer) (err error) {
	if !c.raw {
		fmt.Printf("Subscribing to topic %s auto acknowlegement: %v\n\n", consumer.DeliverySubject(), c.ack)
		fmt.Println("Consumer Info:")
		fmt.Printf("  Ack Policy: %s\n", consumer.AckPolicy().String())
		if consumer.AckPolicy() != api.AckNone {
			fmt.Printf("    Ack Wait: %v\n", consumer.AckWait())
		}
		fmt.Println()
	}

	_, err = c.nc.Subscribe(consumer.DeliverySubject(), func(m *nats.Msg) {
		if len(m.Data) == 0 && m.Header.Get("Status") == "100" {
			stalled := m.Header.Get("Nats-Consumer-Stalled")
			if stalled != "" {
				c.nc.Publish(stalled, nil)
			} else {
				m.Respond(nil)
			}

			return
		}

		var msginfo *jsm.MsgInfo
		var err error

		if len(m.Reply) > 0 {
			msginfo, err = jsm.ParseJSMsgMetadata(m)
		}

		kingpin.FatalIfError(err, "could not parse JetStream metadata: '%s'", m.Reply)

		if !c.raw {
			now := time.Now().Format("15:04:05")

			if msginfo != nil {
				fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %d\n", now, m.Subject, msginfo.Delivered(), msginfo.ConsumerSequence(), msginfo.StreamSequence(), msginfo.Pending())
			} else {
				fmt.Printf("[%s] %s reply: %s\n", now, m.Subject, m.Reply)
			}

			if len(m.Header) > 0 {
				if len(m.Data) == 0 && m.Reply != "" && m.Header.Get("Status") == "100" {
					m.Respond(nil)
					return
				}

				fmt.Println()
				fmt.Println("Headers:")
				fmt.Println()

				for h, vals := range m.Header {
					for _, val := range vals {
						fmt.Printf("   %s: %s\n", h, val)
					}
				}

				fmt.Println()
				fmt.Println("Data:")
			}

			fmt.Printf("%s\n", string(m.Data))
			if !strings.HasSuffix(string(m.Data), "\n") {
				fmt.Println()
			}
		} else {
			fmt.Println(string(m.Data))
		}

		if c.ack {
			err = m.Respond(nil)
			if err != nil {
				fmt.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
			}
		}
	})
	kingpin.FatalIfError(err, "could not subscribe")

	<-context.Background().Done()

	return nil
}

func (c *consumerCmd) subAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, true, nats.UseOldRequestStyle())

	consumer, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	kingpin.FatalIfError(err, "could not load Consumer")

	if consumer.AckPolicy() == api.AckNone {
		c.ack = false
	}

	switch {
	case consumer.IsPullMode():
		return c.getNextMsgDirect(consumer.StreamName(), consumer.Name())
	case consumer.IsPushMode():
		return c.subscribeConsumer(consumer)
	default:
		return fmt.Errorf("consumer %s > %s is in an unknown state", c.stream, c.consumer)
	}
}

func (c *consumerCmd) nextAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(false, false, nats.UseOldRequestStyle())

	var err error

	for i := 0; i < c.pullCount; i++ {
		err = c.getNextMsgDirect(c.stream, c.consumer)
		if err != nil {
			break
		}
	}
	return err
}

func (c *consumerCmd) connectAndSetup(askStream bool, askConsumer bool, opts ...nats.Option) {
	var err error

	c.nc, c.mgr, err = prepareHelper("", append(natsOpts(), opts...)...)
	kingpin.FatalIfError(err, "setup failed")

	if c.stream != "" && c.consumer != "" {
		c.selectedConsumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		if err == nil {
			return
		}
	}

	if askStream {
		c.stream, _, err = selectStream(c.mgr, c.stream, c.force)
		kingpin.FatalIfError(err, "could not select Stream")

		if askConsumer {
			c.consumer, c.selectedConsumer, err = selectConsumer(c.mgr, c.stream, c.consumer, c.force)
			kingpin.FatalIfError(err, "could not select Consumer")
		}
	}
}

func (c *consumerCmd) reportAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, false)

	s, err := c.mgr.LoadStream(c.stream)
	if err != nil {
		return err
	}

	ss, err := s.LatestState()
	if err != nil {
		return err
	}

	leaders := make(map[string]*raftLeader)

	table := newTableWriter(fmt.Sprintf("Consumer report for %s with %d consumers", c.stream, ss.Consumers))
	table.AddHeaders("Consumer", "Mode", "Ack Policy", "Ack Wait", "Ack Pending", "Redelivered", "Unprocessed", "Ack Floor", "Cluster")
	err = s.EachConsumer(func(cons *jsm.Consumer) {
		cs, err := cons.LatestState()
		if err != nil {
			log.Printf("Could not obtain consumer state for %s: %s", cons.Name(), err)
			return
		}

		mode := "Push"
		if cons.IsPullMode() {
			mode = "Pull"
		}

		if cs.Cluster != nil {
			if cs.Cluster.Leader != "" {
				_, ok := leaders[cs.Cluster.Leader]
				if !ok {
					leaders[cs.Cluster.Leader] = &raftLeader{name: cs.Cluster.Leader, cluster: cs.Cluster.Name}
				}
				leaders[cs.Cluster.Leader].groups++
			}
		}

		if c.raw {
			table.AddRow(cons.Name(), mode, cons.AckPolicy().String(), cons.AckWait(), cs.NumAckPending, cs.NumRedelivered, cs.NumPending, cs.AckFloor.Stream, renderCluster(cs.Cluster))
		} else {
			unprocessed := "0"
			if cs.NumPending > 0 {
				unprocessed = fmt.Sprintf("%s / %0.0f%%", humanize.Comma(int64(cs.NumPending)), math.Floor(float64(cs.NumPending)/float64(ss.Msgs)*100))
			}

			table.AddRow(cons.Name(), mode, cons.AckPolicy().String(), humanizeDuration(cons.AckWait()), humanize.Comma(int64(cs.NumAckPending)), humanize.Comma(int64(cs.NumRedelivered)), unprocessed, humanize.Comma(int64(cs.AckFloor.Stream)), renderCluster(cs.Cluster))
		}
	})
	if err != nil {
		return err
	}

	fmt.Println(table.Render())

	if c.reportLeaderDistrib && len(leaders) > 0 {
		renderRaftLeaders(leaders, "Consumers")
	}

	return nil
}

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

	bpsRateLimit  uint64
	maxDeliver    int
	pull          bool
	replayPolicy  string
	startPolicy   string
	ackPolicy     string
	ackWait       time.Duration
	samplePct     int
	filterSubject string
	delivery      string
	ephemeral     bool
	validateOnly  bool
}

func configureConsumerCommand(app *kingpin.Application) {
	c := &consumerCmd{}

	addCreateFlags := func(f *kingpin.CmdClause) {
		f.Flag("target", "Push based delivery target subject").StringVar(&c.delivery)
		f.Flag("filter", "Filter Stream by subjects").Default("_unset_").StringVar(&c.filterSubject)
		f.Flag("replay", "Replay Policy (instant, original)").EnumVar(&c.replayPolicy, "instant", "original")
		f.Flag("deliver", "Start policy (all, new, last, 1h, msg sequence)").StringVar(&c.startPolicy)
		f.Flag("ack", "Acknowledgement policy (none, all, explicit)").StringVar(&c.ackPolicy)
		f.Flag("wait", "Acknowledgement waiting time").Default("-1s").DurationVar(&c.ackWait)
		f.Flag("sample", "Percentage of requests to sample for monitoring purposes").Default("-1").IntVar(&c.samplePct)
		f.Flag("ephemeral", "Create an ephemeral Consumer").Default("false").BoolVar(&c.ephemeral)
		f.Flag("pull", "Deliver messages in 'pull' mode").BoolVar(&c.pull)
		f.Flag("max-deliver", "Maximum amount of times a message will be delivered").IntVar(&c.maxDeliver)
		f.Flag("bps", "Restrict message delivery to a certain bit per second").Default("0").Uint64Var(&c.bpsRateLimit)
	}

	cons := app.Command("consumer", "JetStream Consumer management").Alias("con").Alias("obs").Alias("c")

	consAdd := cons.Command("add", "Creates a new Consumer").Alias("create").Alias("new").Action(c.createAction)
	consAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	consAdd.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	consAdd.Flag("validate", "Only validates the configuration against the official Schema").BoolVar(&c.validateOnly)
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

	consRm := cons.Command("rm", "Removes a Consumer").Alias("delete").Alias("del").Action(c.rmAction)
	consRm.Arg("stream", "Stream name").StringVar(&c.stream)
	consRm.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	consSub := cons.Command("sub", "Retrieves messages from Consumers").Action(c.subAction)
	consSub.Arg("stream", "Stream name").StringVar(&c.stream)
	consSub.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consSub.Flag("ack", "Acknowledge received message").Default("true").BoolVar(&c.ack)
	consSub.Flag("raw", "Show only the message").Short('r').BoolVar(&c.raw)
}

func (c *consumerCmd) rmAction(_ *kingpin.ParseContext) error {
	c.connectAndSetup(true, true)

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete Consumer %s > %s", c.stream, c.consumer), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	consumer, err := jsm.LoadConsumer(c.stream, c.consumer)
	kingpin.FatalIfError(err, "could not load Consumer")

	return consumer.Delete()
}

func (c *consumerCmd) lsAction(pc *kingpin.ParseContext) error {
	c.connectAndSetup(true, false)

	stream, err := jsm.LoadStream(c.stream)
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
	state, err := consumer.State()
	kingpin.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)

	c.showInfo(config, state)
}

func (c *consumerCmd) showInfo(config api.ConsumerConfig, state api.ConsumerInfo) {
	if c.json {
		printJSON(state)
		return
	}

	fmt.Printf("Information for Consumer %s > %s\n", state.Stream, state.Name)
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	if config.Durable != "" {
		fmt.Printf("        Durable Name: %s\n", config.Durable)
	}
	if config.DeliverSubject != "" {
		fmt.Printf("    Delivery Subject: %s\n", config.DeliverSubject)
	} else {
		fmt.Printf("           Pull Mode: true\n")
	}
	if config.FilterSubject != "" {
		fmt.Printf("      Filter Subject: %s\n", config.FilterSubject)
	}
	if config.OptStartSeq != 0 {
		fmt.Printf("      Start Sequence: %d\n", config.OptStartSeq)
	}
	if config.OptStartTime != nil && config.OptStartTime.Second() != 0 {
		fmt.Printf("          Start Time: %v\n", config.OptStartTime)
	}
	if config.DeliverPolicy == api.DeliverAll {
		fmt.Printf("         Deliver All: true\n")
	}
	if config.DeliverPolicy == api.DeliverLast {
		fmt.Printf("        Deliver Last: true\n")
	}
	if config.DeliverPolicy == api.DeliverNew {
		fmt.Printf("        Deliver Next: true\n")
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

	fmt.Println()

	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("  Last Delivered Message: Consumer sequence: %d Stream sequence: %d\n", state.Delivered.ConsumerSeq, state.Delivered.StreamSeq)
	fmt.Printf("    Acknowledgment floor: Consumer sequence: %d Stream sequence: %d\n", state.AckFloor.ConsumerSeq, state.AckFloor.StreamSeq)
	fmt.Printf("        Pending Messages: %d\n", state.NumPending)
	fmt.Printf("    Redelivered Messages: %d\n", state.NumRedelivered)
	fmt.Println()
}

func (c *consumerCmd) infoAction(pc *kingpin.ParseContext) error {
	c.connectAndSetup(true, true)

	consumer, err := jsm.LoadConsumer(c.stream, c.consumer)
	kingpin.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)

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
	} else if ok, _ := regexp.MatchString("^\\d+$", policy); ok {
		seq, _ := strconv.Atoi(policy)
		cfg.DeliverPolicy = api.DeliverByStartSequence
		cfg.OptStartSeq = uint64(seq)
	} else {
		d, err := parseDurationString(policy)
		kingpin.FatalIfError(err, "could not parse starting delta")
		t := time.Now().Add(-d)
		cfg.DeliverPolicy = api.DeliverByStartTime
		cfg.OptStartTime = &t
	}
}

func (c *consumerCmd) cpAction(pc *kingpin.ParseContext) (err error) {
	c.connectAndSetup(true, false)

	source, err := jsm.LoadConsumer(c.stream, c.consumer)
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

	consumer, err := jsm.NewConsumerFromDefault(c.stream, cfg)
	kingpin.FatalIfError(err, "Consumer creation failed")

	if cfg.Durable == "" {
		return nil
	}

	c.consumer = cfg.Durable

	c.showConsumer(consumer)

	return nil
}

func (c *consumerCmd) prepareConfig() (cfg *api.ConsumerConfig, err error) {
	cfg = c.defaultConsumer()

	if c.inputFile != "" {
		f, err := ioutil.ReadFile(c.inputFile)
		if err != nil {
			return nil, err
		}

		cfg = &api.ConsumerConfig{}
		err = json.Unmarshal(f, cfg)

		if cfg.Durable != "" && c.consumer != "" && cfg.Durable != c.consumer {
			return cfg, fmt.Errorf("non durable consumer name in %s does not match CLI consumer name %s", c.inputFile, c.consumer)
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
			Message: "Delivery target",
			Help:    "Consumers can be in 'push' or 'pull' mode, in 'push' mode messages are dispatched in real time to a target NATS subject, this is that subject. Leaving this blank creates a 'pull' mode Consumer. Settable using --target and --pull",
		}, &c.delivery)
		kingpin.FatalIfError(err, "could not request delivery target")
	}

	if c.ephemeral && c.delivery == "" {
		kingpin.Fatalf("ephemeral Consumers has to be push-based.")
	}

	cfg.DeliverSubject = c.delivery

	// pull is always explicit
	if c.delivery == "" {
		c.ackPolicy = "explicit"
	}

	if c.startPolicy == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Start policy (all, new, last, 1h, msg sequence)",
			Help:    "This controls how the Consumer starts out, does it make all messages available, only the latest, ones after a certain time or time sequence. Settable using --deliver",
		}, &c.startPolicy, survey.WithValidator(survey.Required))
		kingpin.FatalIfError(err, "could not request start policy")
	}

	c.setStartPolicy(cfg, c.startPolicy)

	if c.ackPolicy == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Acknowledgement policy",
			Options: []string{"none", "all", "explicit"},
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

	if c.maxDeliver == 0 && cfg.AckPolicy != api.AckNone {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum Allowed Deliveries",
			Default: "-1",
			Help:    "When this is -1 unlimited attempts to deliver an un acknowledged message is made, when this is >0 it will be maximum amount of times a message is delivered after which it is ignored. Settable using --max-deliver.",
		}, &c.maxDeliver)
		kingpin.FatalIfError(err, "could not ask for maximum allowed deliveries")
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

func (c *consumerCmd) createAction(pc *kingpin.ParseContext) (err error) {
	cfg, err := c.prepareConfig()
	if err != nil {
		return err
	}

	if c.validateOnly {
		j, err := json.MarshalIndent(cfg, "", "  ")
		kingpin.FatalIfError(err, "Could not marshal configuration")
		fmt.Println(string(j))
		fmt.Println()
		valid, errs := cfg.Validate()
		if !valid {
			kingpin.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Println("Configuration is a valid Consumer")
		return nil
	}

	c.connectAndSetup(true, false)

	created, err := jsm.NewConsumerFromDefault(c.stream, *cfg)
	kingpin.FatalIfError(err, "Consumer creation failed")

	c.consumer = created.Name()

	c.showConsumer(created)

	return nil
}

func (c *consumerCmd) getNextMsgDirect(stream string, consumer string) error {
	msg, err := jsm.NextMsg(stream, consumer)
	kingpin.FatalIfError(err, "could not load next message")

	if !c.raw {
		info, err := jsm.ParseJSMsgMetadata(msg)
		if err != nil {
			fmt.Printf("--- subject: %s\n", msg.Subject)
		} else {
			fmt.Printf("--- subject: %s / delivered: %d / stream seq: %d / consumer seq: %d\n", msg.Subject, info.Delivered(), info.StreamSequence(), info.ConsumerSequence())
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
		err = msg.Respond(api.AckAck)
		kingpin.FatalIfError(err, "could not Acknowledge message")
		jsm.Flush()
		if !c.raw {
			fmt.Println("\nAcknowledged message")
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

	_, err = consumer.Subscribe(func(m *nats.Msg) {
		msginfo, err := jsm.ParseJSMsgMetadata(m)
		kingpin.FatalIfError(err, "could not parse JetStream metadata")

		if !c.raw {
			now := time.Now().Format("15:04:05")

			if msginfo != nil {
				fmt.Printf("[%s] subject: %s / delivered: %d / consumer seq: %d / stream seq: %d\n", now, m.Subject, msginfo.Delivered(), msginfo.ConsumerSequence(), msginfo.StreamSequence())
			} else {
				fmt.Printf("[%s] %s reply: %s\n", now, m.Subject, m.Reply)
			}

			if len(m.Header) > 0 {
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
	c.connectAndSetup(true, true)

	consumer, err := jsm.LoadConsumer(c.stream, c.consumer)
	kingpin.FatalIfError(err, "could not get Consumer info")

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
	c.connectAndSetup(false, false)

	return c.getNextMsgDirect(c.stream, c.consumer)
}

func (c *consumerCmd) connectAndSetup(askStream bool, askConsumer bool) {
	_, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	if askStream {
		c.stream, err = selectStream(c.stream, c.force)
		kingpin.FatalIfError(err, "could not select Stream")

		if askConsumer {
			c.consumer, err = selectConsumer(c.stream, c.consumer, c.force)
			kingpin.FatalIfError(err, "could not select Consumer")
		}
	}
}

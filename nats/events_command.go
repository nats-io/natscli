package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type eventsCmd struct {
	json  bool
	ce    bool
	short bool

	bodyF      string
	bodyFRe    *regexp.Regexp
	subjectF   string
	subjectFRe *regexp.Regexp

	metricsF    bool
	advisoriesF bool
	allF        bool
	connsF      bool
	latencySubj []string

	sync.Mutex
}

func configureEventsCommand(app *kingpin.Application) {
	c := &eventsCmd{}

	events := app.Command("events", "Show Advisories and Events").Alias("event").Alias("e").Action(c.eventsAction)
	events.Flag("all", "Show all events").Default("false").Short('a').BoolVar(&c.allF)
	events.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	events.Flag("cloudevent", "Produce CloudEvents v1.0 output").BoolVar(&c.ce)
	events.Flag("short", "Short event format").BoolVar(&c.short)
	events.Flag("subject", "Filter the messages by the subject using regular expressions").Default(".").StringVar(&c.subjectF)
	events.Flag("filter", "Filter across the entire event using regular expressions").Default(".").StringVar(&c.bodyF)
	events.Flag("metrics", "Shows JetStream metric events (false)").Default("false").BoolVar(&c.metricsF)
	events.Flag("advisories", "Shows advisory events (true)").Default("true").BoolVar(&c.advisoriesF)
	events.Flag("connections", "Shows connections being opened and closed (false)").Default("false").BoolVar(&c.connsF)
	events.Flag("latency", "Show service latency samples received on a specific subject").PlaceHolder("SUBJECT").StringsVar(&c.latencySubj)
}

func (c *eventsCmd) handleNATSEvent(m *nats.Msg) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}

	if c.json && !c.ce {
		fmt.Println(string(m.Data))
		return
	}

	handle := func() error {
		kind, event, err := api.ParseMessage(m.Data)
		if err != nil {
			return fmt.Errorf("parsing failed: %s", err)
		}

		if kind == "io.nats.unknown_message" {
			return fmt.Errorf("unknown event schema")
		}

		ne, ok := event.(api.Event)
		if !ok {
			return fmt.Errorf("event %q does not implement the Event interface", kind)
		}

		var format api.RenderFormat
		switch {
		case c.ce:
			format = api.ApplicationCloudEventV1Format
		case c.short:
			format = api.TextCompactFormat
		default:
			format = api.TextExtendedFormat
		}

		err = api.RenderEvent(os.Stdout, ne, format)
		if err != nil {
			return fmt.Errorf("display failed: %s", err)
		}

		fmt.Println()

		return nil
	}

	err := handle()
	if err != nil {
		fmt.Printf("Event error: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
	}
}

func (c *eventsCmd) Printf(f string, arg ...interface{}) {
	if !c.json {
		fmt.Printf(f, arg...)
	}
}

func (c *eventsCmd) eventsAction(_ *kingpin.ParseContext) error {
	if c.ce {
		c.json = true
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.subjectFRe, err = regexp.Compile(strings.ToUpper(c.subjectF))
	kingpin.FatalIfError(err, "invalid subjects regular expression")
	c.bodyFRe, err = regexp.Compile(strings.ToUpper(c.bodyF))
	kingpin.FatalIfError(err, "invalid body regular expression")

	if c.advisoriesF || c.allF {
		c.Printf("Listening for Advisories on %s.>\n", api.JSAdvisoryPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JSAdvisoryPrefix), func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if c.metricsF || c.allF {
		c.Printf("Listening for Metrics on %s.>\n", api.JSMetricPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JSMetricPrefix), func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if c.connsF || c.allF {
		c.Printf("Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.CONNECT", func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})

		c.Printf("Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.DISCONNECT", func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if len(c.latencySubj) > 0 {
		for _, s := range c.latencySubj {
			c.Printf("Listening for latency samples on %s\n", s)
			nc.Subscribe(s, func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})
		}
	}

	<-context.Background().Done()

	return nil
}

func leftPad(s string, indent int) string {
	var out []string
	format := fmt.Sprintf("%%%ds", indent)

	for _, l := range strings.Split(s, "\n") {
		out = append(out, fmt.Sprintf(format, " ")+l)
	}

	return strings.Join(out, "\n")
}

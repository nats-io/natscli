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
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type eventsCmd struct {
	json  bool
	ce    bool
	short bool

	bodyF   string
	bodyFRe *regexp.Regexp

	showJsMetrics        bool
	showJsAdvisories     bool
	showServerAdvisories bool
	showAll              bool
	extraSubjects        []string

	sync.Mutex
}

func configureEventsCommand(app *kingpin.Application) {
	c := &eventsCmd{}

	events := app.Command("events", "Show Advisories and Events").Alias("event").Alias("e").Action(c.eventsAction)
	events.Flag("all", "Show all events").Default("false").Short('a').BoolVar(&c.showAll)
	events.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	events.Flag("cloudevent", "Produce CloudEvents v1 output").BoolVar(&c.ce)
	events.Flag("short", "Short event format").BoolVar(&c.short)
	events.Flag("filter", "Filter across the entire event using regular expressions").Default(".").StringVar(&c.bodyF)
	events.Flag("js-metric", "Shows JetStream metric events (false)").Default("false").BoolVar(&c.showJsMetrics)
	events.Flag("js-advisory", "Shows advisory events (false)").Default("false").BoolVar(&c.showJsAdvisories)
	events.Flag("srv-advisory", "Shows NATS Server advisories (true)").Default("true").BoolVar(&c.showServerAdvisories)
	events.Flag("subjects", "Show Advisories and Metrics received on specific subjects").PlaceHolder("SUBJECTS").StringsVar(&c.extraSubjects)

	cheats["events"] = `# To view common system events
nats events
nats events --short --all
nats events --no-srv-advisory --js-metric --js-advisory
nats events --no-srv-advisory --subjects service.latency.weather
`
}

func (c *eventsCmd) handleNATSEvent(m *nats.Msg) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
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

		if trace {
			log.Printf("Received %s event on subject %s", kind, m.Subject)
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

	nc, _, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.bodyFRe, err = regexp.Compile(strings.ToUpper(c.bodyF))
	kingpin.FatalIfError(err, "invalid body regular expression")

	if !c.showAll && !c.showJsAdvisories && !c.showJsMetrics && !c.showServerAdvisories && len(c.extraSubjects) == 0 {
		return fmt.Errorf("no events were chosen")
	}

	if c.showJsAdvisories || c.showAll {
		c.Printf("Listening for Advisories on %s.>\n", jsm.EventSubject(api.JSAdvisoryPrefix, config.JSEventPrefix()))
		nc.Subscribe(fmt.Sprintf("%s.>", jsm.EventSubject(api.JSAdvisoryPrefix, config.JSEventPrefix())), func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if c.showJsMetrics || c.showAll {
		c.Printf("Listening for Metrics on %s.>\n", jsm.EventSubject(api.JSMetricPrefix, config.JSEventPrefix()))
		nc.Subscribe(fmt.Sprintf("%s.>", jsm.EventSubject(api.JSMetricPrefix, config.JSEventPrefix())), func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if c.showServerAdvisories || c.showAll {
		c.Printf("Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.CONNECT", func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})

		c.Printf("Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.DISCONNECT", func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})

		c.Printf("Listening for Authentication Errors events on $SYS.SERVER.*.CLIENT.AUTH.ERR\n")
		nc.Subscribe("$SYS.SERVER.*.CLIENT.AUTH.ERR", func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if len(c.extraSubjects) > 0 {
		for _, s := range c.extraSubjects {
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

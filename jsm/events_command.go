package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	api "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jetstream/jsch"
)

type eventsCmd struct {
	json bool
}

func configureEventsCommand(app *kingpin.Application) {
	c := &eventsCmd{}
	events := app.Command("events", "Show JetStream Advisories and Events").Alias("event").Alias("e").Action(c.eventsAction)
	events.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
}

func (c *eventsCmd) eventsAction(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	if !c.json {
		fmt.Printf("Listening for Advisories on %s.>\n", api.JetStreamAdvisoryPrefix)
		fmt.Printf("Listening for Metrics on %s.>\n", api.JetStreamMetricPrefix)
	}

	nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamAdvisoryPrefix), func(m *nats.Msg) {
		c.renderAdvisory(m)
	})

	nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamMetricPrefix), func(m *nats.Msg) {
		c.renderMetric(m)
	})

	<-context.Background().Done()

	return nil
}

func parseTime(t string) time.Time {
	tstamp, err := time.Parse(time.RFC3339, t)
	if err != nil {
		tstamp = time.Now().UTC()
	}

	return tstamp
}

func leftPad(s string, indent int) string {
	out := []string{}
	format := fmt.Sprintf("%%%ds", indent)

	for _, l := range strings.Split(s, "\n") {
		out = append(out, fmt.Sprintf(format, " ")+l)
	}

	return strings.Join(out, "\n")
}

func (c *eventsCmd) renderAdvisory(m *nats.Msg) {
	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	_, event, err := jsch.ParseEvent(m.Data)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	switch event := event.(type) {
	case *api.ConsumerDeliveryExceededAdvisory:
		fmt.Printf("[%s] [%s] Delivery Attempts Exceeded\n", parseTime(event.Time).Format("15:04:05"), event.ID)
		fmt.Printf("         Consumer: %s > %s\n", event.Stream, event.Consumer)
		fmt.Printf("  Stream Sequence: %d\n", event.StreamSeq)
		fmt.Printf("       Deliveries: %d\n", event.Deliveries)
		fmt.Println()

	case *api.JetStreamAPIAudit:
		fmt.Printf("[%s] [%s] API Access\n", parseTime(event.Time).Format("15:04:05"), event.ID)
		fmt.Printf("      Server: %s\n", event.Server)
		fmt.Printf("     Subject: %s\n", event.Subject)
		fmt.Printf("      Client:\n")
		if event.Client.User != "" {
			fmt.Printf("               User: %s Account: %s\n", event.Client.User, event.Client.Account)
		} else {
			fmt.Printf("            Account: %s\n", event.Client.Account)
		}
		fmt.Printf("               Host: %s\n", net.JoinHostPort(event.Client.Host, strconv.Itoa(event.Client.Port)))
		fmt.Printf("                 ID: %d\n", event.Client.CID)
		if event.Client.Name != "" {
			fmt.Printf("               Name: %s\n", event.Client.Name)
		}
		if event.Client.Language != "" && event.Client.Version != "" {
			fmt.Printf("            Lanuage: %s %s\n", event.Client.Language, event.Client.Version)
		}
		fmt.Println()

		fmt.Println("    Request:")
		fmt.Println()
		if event.Request != "" {
			fmt.Println(leftPad(event.Request, 10))
		} else {
			fmt.Println("          Empty Request")
		}
		fmt.Println()

		if event.Response != "" {
			fmt.Println("    Response:")
			fmt.Println()
			fmt.Println(leftPad(event.Response, 10))
		}
		fmt.Println()
	default:
		fmt.Println(string(m.Data))
	}
}

func (c *eventsCmd) renderMetric(m *nats.Msg) {
	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	_, event, err := jsch.ParseEvent(m.Data)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	switch event := event.(type) {
	case *api.ConsumerAckMetric:
		fmt.Printf("[%s] [%s] Acknowledgement Sample\n", parseTime(event.Time).Format("15:04:05"), event.ID)
		fmt.Printf("              Consumer: %s > %s\n", event.Stream, event.Consumer)
		fmt.Printf("       Stream Sequence: %d\n", event.StreamSeq)
		fmt.Printf("     Consumer Sequence: %d\n", event.ConsumerSeq)
		fmt.Printf("            Deliveries: %d\n", event.Deliveries)
		fmt.Printf("                 Delay: %v\n", time.Duration(event.Delay))
		fmt.Println()

	default:
		fmt.Println(string(m.Data))
	}
}

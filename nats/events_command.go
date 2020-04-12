package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go"
)

type eventsCmd struct {
	json bool

	bodyF      string
	bodyFRe    *regexp.Regexp
	subjectF   string
	subjectFRe *regexp.Regexp

	metricsF    bool
	advisoriesF bool
	allF        bool
	connsF      bool
}

func configureEventsCommand(app *kingpin.Application) {
	c := &eventsCmd{}
	events := app.Command("events", "Show Advisories and Events").Alias("event").Alias("e").Action(c.eventsAction)
	events.Flag("all", "Show all events").Default("false").Short('a').BoolVar(&c.allF)
	events.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	events.Flag("subject", "Filter the messages by the subject using regular expressions").Default(".").StringVar(&c.subjectF)
	events.Flag("filter", "Filter across the entire event using regular expressions").Default(".").StringVar(&c.bodyF)
	events.Flag("metrics", "Shows metric events (false)").Default("false").BoolVar(&c.metricsF)
	events.Flag("advisories", "Shows advisory events (true)").Default("true").BoolVar(&c.advisoriesF)
	events.Flag("connections", "Shows connections being opened and closed (false)").Default("false").BoolVar(&c.connsF)

}

func (c *eventsCmd) Printf(f string, arg ...interface{}) {
	if !c.json {
		fmt.Printf(f, arg...)
	}
}

func (c *eventsCmd) eventsAction(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.subjectFRe, err = regexp.Compile(strings.ToUpper(c.subjectF))
	kingpin.FatalIfError(err, "invalid subjects regular expression")
	c.bodyFRe, err = regexp.Compile(strings.ToUpper(c.bodyF))
	kingpin.FatalIfError(err, "invalid body regular expression")

	if c.advisoriesF || c.allF {
		c.Printf("Listening for Advisories on %s.>\n", api.JetStreamAdvisoryPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamAdvisoryPrefix), func(m *nats.Msg) {
			c.renderAdvisory(m)
		})
	}

	if c.metricsF || c.allF {
		c.Printf("Listening for Metrics on %s.>\n", api.JetStreamMetricPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamMetricPrefix), func(m *nats.Msg) {
			c.renderMetric(m)
		})
	}

	if c.connsF || c.allF {
		c.Printf("Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.CONNECT", func(m *nats.Msg) {
			c.renderConnection(m)
		})

		c.Printf("Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT\n")
		nc.Subscribe("$SYS.ACCOUNT.*.DISCONNECT", func(m *nats.Msg) {
			c.renderDisconnection(m)
		})
	}

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

func (c *eventsCmd) renderDisconnection(m *nats.Msg) {
	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}

	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	event := server.DisconnectEventMsg{}
	err := json.Unmarshal(m.Data, &event)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	fmt.Printf("[%s] [%d] Client Disconnection\n", event.Server.Time.Format("15:04:05"), event.Server.Seq)
	fmt.Printf("   Reason: %s\n", event.Reason)
	fmt.Printf("   Server: %s\n", event.Server.Name)
	fmt.Println()
	fmt.Println("   Client:")
	fmt.Printf("            ID: %d\n", event.Client.ID)
	if event.Client.User != "" {
		fmt.Printf("          User: %s\n", event.Client.User)
	}
	if event.Client.Name != "" {
		fmt.Printf("          Name: %s\n", event.Client.Name)
	}
	fmt.Printf("       Account: %s\n", event.Client.Account)
	fmt.Println()
	fmt.Println("   Stats:")
	fmt.Printf("      Received: %d messages (%s)\n", event.Received.Msgs, humanize.IBytes(uint64(event.Received.Bytes)))
	fmt.Printf("     Published: %d messages (%s)\n", event.Sent.Msgs, humanize.IBytes(uint64(event.Sent.Bytes)))
	fmt.Println()
}

func (c *eventsCmd) renderConnection(m *nats.Msg) {
	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	event := server.ConnectEventMsg{}
	err := json.Unmarshal(m.Data, &event)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	fmt.Printf("[%s] [%d] Client Connection\n", event.Server.Time.Format("15:04:05"), event.Server.Seq)
	fmt.Println("   Server:")
	fmt.Printf("          Name: %s\n", event.Server.Name)
	if event.Server.Cluster != "" {
		fmt.Printf("       Cluster: %s\n", event.Server.Cluster)
	}
	fmt.Println()
	fmt.Println("   Client:")
	fmt.Printf("          ID: %d\n", event.Client.ID)
	if event.Client.User != "" {
		fmt.Printf("        User: %s\n", event.Client.User)
	}
	if event.Client.Name != "" {
		fmt.Printf("        Name: %s\n", event.Client.Name)
	}
	fmt.Printf("     Account: %s\n", event.Client.Account)
	fmt.Printf("    Language: %s\n", event.Client.Lang)
	fmt.Printf("     Version: %s\n", event.Client.Version)
	fmt.Printf("        Host: %s\n", event.Client.Host)
	fmt.Println()
}

func (c *eventsCmd) renderDeliveryExceeded(event *api.ConsumerDeliveryExceededAdvisory, m *nats.Msg) {
	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	fmt.Printf("[%s] [%s] Delivery Attempts Exceeded\n", parseTime(event.Time).Format("15:04:05"), event.ID)
	fmt.Printf("         Consumer: %s > %s\n", event.Stream, event.Consumer)
	fmt.Printf("  Stream Sequence: %d\n", event.StreamSeq)
	fmt.Printf("       Deliveries: %d\n", event.Deliveries)
	fmt.Println()
}

func (c *eventsCmd) renderAPIAudit(event *api.JetStreamAPIAudit, m *nats.Msg) {
	if !c.subjectFRe.MatchString(strings.ToUpper(event.Subject)) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

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
}

func (c *eventsCmd) renderAckSample(event *api.ConsumerAckMetric, m *nats.Msg) {
	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	fmt.Printf("[%s] [%s] Acknowledgement Sample\n", parseTime(event.Time).Format("15:04:05"), event.ID)
	fmt.Printf("              Consumer: %s > %s\n", event.Stream, event.Consumer)
	fmt.Printf("       Stream Sequence: %d\n", event.StreamSeq)
	fmt.Printf("     Consumer Sequence: %d\n", event.ConsumerSeq)
	fmt.Printf("            Deliveries: %d\n", event.Deliveries)
	fmt.Printf("                 Delay: %v\n", time.Duration(event.Delay))
	fmt.Println()
}

func (c *eventsCmd) renderAdvisory(m *nats.Msg) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	_, event, err := jsm.ParseEvent(m.Data)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	switch event := event.(type) {
	case *api.ConsumerDeliveryExceededAdvisory:
		c.renderDeliveryExceeded(event, m)

	case *api.JetStreamAPIAudit:
		c.renderAPIAudit(event, m)

	default:
		fmt.Println(string(m.Data))
	}
}

func (c *eventsCmd) renderMetric(m *nats.Msg) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	_, event, err := jsm.ParseEvent(m.Data)
	if err != nil {
		fmt.Printf("Event parsing failed: %s\n\n", err)
		fmt.Println(leftPad(string(m.Data), 10))
		return
	}

	switch event := event.(type) {
	case *api.ConsumerAckMetric:
		c.renderAckSample(event, m)

	default:
		fmt.Println(string(m.Data))
	}
}

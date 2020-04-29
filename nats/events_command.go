package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/api"
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
	latencySubj []string

	templates map[string]*template.Template

	sync.Mutex
}

func configureEventsCommand(app *kingpin.Application) {
	c := &eventsCmd{
		templates: make(map[string]*template.Template),
	}

	events := app.Command("events", "Show Advisories and Events").Alias("event").Alias("e").Action(c.eventsAction)
	events.Flag("all", "Show all events").Default("false").Short('a').BoolVar(&c.allF)
	events.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	events.Flag("subject", "Filter the messages by the subject using regular expressions").Default(".").StringVar(&c.subjectF)
	events.Flag("filter", "Filter across the entire event using regular expressions").Default(".").StringVar(&c.bodyF)
	events.Flag("metrics", "Shows metric events (false)").Default("false").BoolVar(&c.metricsF)
	events.Flag("advisories", "Shows advisory events (true)").Default("true").BoolVar(&c.advisoriesF)
	events.Flag("connections", "Shows connections being opened and closed (false)").Default("false").BoolVar(&c.connsF)
	events.Flag("latency", "Show service latency samples received on a specific subject").PlaceHolder("SUBJECT").Default("").StringsVar(&c.latencySubj)
}

func (c *eventsCmd) parseTemplates() (err error) {
	t := map[string]string{}

	t["io.nats.jetstream.advisory.v1.max_deliver"] = `
[{{ .Time | ShortTime }}] [{{ .ID }}] Delivery Attempts Exceeded

          Consumer: {{ .Stream }} > {{ .Consumer }}
   Stream Sequence: {{ .StreamSeq }}
        Deliveries: {{ .Deliveries }}
`

	t["io.nats.jetstream.advisory.v1.api_audit"] = `
[{{ .Time | ShortTime }}] [{{ .ID }}] JetStream API Access

      Server: {{ .Server }}
     Subject: {{ .Subject }}
      Client:
{{- if .Client.User }}
               User: {{ .Client.User }} Account: {{ .Client.Account }}
{{- end }}
               Host: {{ HostPort .Client.Host .Client.Port }}
                CID: {{ .Client.CID }}
{{- if .Client.Name }}
               Name: {{ .Client.Name }}
{{- end }}
           Language: {{ .Client.Language }} {{ .Client.Version }}

    Request:
{{ if .Request }}
{{ .Request | LeftPad 10 }}
{{- else }}
          Empty Request
{{- end }}

    Response:

{{ .Response | LeftPad 10 }}
`

	t["io.nats.jetstream.metric.v1.consumer_ack"] = `
[{{ .Time | ShortTime }}] [{{ .ID }}] Acknowledgment Sample

              Consumer: {{ .Stream }} > {{ .Consumer }}
       Stream Sequence: {{ .StreamSeq }}
     Consumer Sequence: {{ .ConsumerSeq }}
            Deliveries: {{ .Deliveries }}
                 Delay: {{ .Delay }}
`

	t["io.nats.server.metric.v1.service_latency"] = `
{{- if .Error }}
[{{ .Time | ShortTime }}] [{{ .ID }}] Service Latency - {{ .Error }}
{{- else }}
[{{ .Time | ShortTime }}] [{{ .ID }}] Service Latency
{{- end }}

   Start Time: {{ .RequestStart | NanoTime }}
{{- if .Error }}
        Error: {{ .Error }}
{{- end }}

   Latencies:

      Request Duration: {{ .TotalLatency }}
{{- if .Requestor }}
             Requestor: {{ .Requestor.RTT }}
{{- end }}
           NATS System: {{ .SystemLatency }}
               Service: {{ .ServiceLatency }}
{{ with .Requestor }}
   Requestor:
{{ if .User }}
        User: {{ .User }}
{{- end }}
{{- if .Name }}
        Name: {{ .Name }}
{{- end }}
{{- if .CID }}
          IP: {{ .IP }}
         CID: {{ .CID }}
      Server: {{ .Server }}
{{- end }}
         RTT: {{ .RTT }}
{{- end }}
{{ with .Responder }}
   Responder:
{{ if .User }}
        User: {{ .User }}
{{- end }}
{{- if .Name }}
        Name: {{ .Name }}
{{- end }}
          IP: {{ .IP }}
         CID: {{ .CID }}
      Server: {{ .Server }}
         RTT: {{ .RTT }}
{{- end }}
`

	t["io.nats.server.advisory.v1.client_connect"] = `
[{{ .Time | ShortTime }}] [{{ .ID }}] Client Disconnection

{{- if .Reason }}
   Reason: {{ .Reason }}
{{- end }}
   Server: {{ .Server.Name }}
{{- if .Server.Cluster }}
  Cluster: {{ .Server.Cluster }}
{{- end }}

   Client:
            ID: {{ .Client.ID }}
{{- if .Client.User }}
          User: {{ .Client.User }}
{{- end }}
{{- if .Client.Name }}
          Name: {{ .Client.Name }}
{{- end }}
       Account: {{ .Client.Account }}
{{- if .Client.Lang }}
      Language: {{ .Client.Lang }} {{ .Client.Version }}
{{- end }}
{{- if .Client.Host }}
          Host: {{ .Client.Host }}
{{- end }}
{{ if .Stats }}
   Stats:
      Received: {{ .Received.Msgs }} messages ({{ .Received.Bytes | IBytes }})
     Published: {{ .Sent.Msgs }} messages ({{ .Sent.Bytes | IBytes }})
           RTT: {{ .Client.RTT }}
{{- end }}
`

	t["io.nats.server.advisory.v1.client_disconnect"] = t["io.nats.server.advisory.v1.client_connect"]

	for k, tmpl := range t {
		c.templates[k], err = template.New(k).Funcs(map[string]interface{}{
			"ShortTime": func(v time.Time) string { return v.Format("15:04:05") },
			"NanoTime":  func(v time.Time) string { return v.Format("15:04:05.000") },
			"IBytes":    func(v int64) string { return humanize.IBytes(uint64(v)) },
			"HostPort":  func(h string, p int) string { return net.JoinHostPort(h, strconv.Itoa(p)) },
			"LeftPad":   func(indent int, v string) string { return leftPad(v, indent) },
		}).Parse(tmpl)
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *eventsCmd) handleNATSEvent(m *nats.Msg) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(m.Data))) {
		return
	}

	if !c.subjectFRe.MatchString(strings.ToUpper(m.Subject)) {
		return
	}

	if c.json {
		fmt.Println(string(m.Data))
		return
	}

	handle := func() error {
		kind, event, err := jsm.ParseEvent(m.Data)
		if err != nil {
			return fmt.Errorf("parsing failed: %s", err)
		}

		if kind == "io.nats.unknown_event" {
			return fmt.Errorf("unknown event")
		}

		tmpl, ok := c.templates[kind]
		if !ok {
			return fmt.Errorf("unknown template for %s", kind)
		}

		err = tmpl.Execute(os.Stdout, event)
		if err != nil {
			return fmt.Errorf("display failed: %s", err)
		}

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
	err := c.parseTemplates()
	if err != nil {
		kingpin.Fatalf(err.Error())
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	c.subjectFRe, err = regexp.Compile(strings.ToUpper(c.subjectF))
	kingpin.FatalIfError(err, "invalid subjects regular expression")
	c.bodyFRe, err = regexp.Compile(strings.ToUpper(c.bodyF))
	kingpin.FatalIfError(err, "invalid body regular expression")

	if c.advisoriesF || c.allF {
		c.Printf("Listening for Advisories on %s.>\n", api.JetStreamAdvisoryPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamAdvisoryPrefix), func(m *nats.Msg) {
			c.handleNATSEvent(m)
		})
	}

	if c.metricsF || c.allF {
		c.Printf("Listening for Metrics on %s.>\n", api.JetStreamMetricPrefix)
		nc.Subscribe(fmt.Sprintf("%s.>", api.JetStreamMetricPrefix), func(m *nats.Msg) {
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

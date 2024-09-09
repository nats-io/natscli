// Copyright 2019-2024 The NATS Authors
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

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/choria-io/fisk/units"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api/server/tracing"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	iu "github.com/nats-io/natscli/internal/util"
)

type traceCmd struct {
	subject string
	deliver bool
	showTs  bool
	header  map[string]string
	payload units.Base2Bytes
}

type traceStats struct {
	egress map[string]int
	sync.Mutex
}

func (s *traceStats) incEgressKind(kind int) {
	s.Lock()
	if s.egress == nil {
		s.egress = map[string]int{}
	}

	kinds := jsm.ServerKindString(kind)

	if _, ok := s.egress[kinds]; !ok {
		s.egress[kinds] = 0
	}

	s.egress[kinds]++

	s.Unlock()
}

func configureTraceCommand(app commandHost) {
	c := &traceCmd{
		header: make(map[string]string),
	}

	trace := app.Command("trace", "Trace message delivery within an NATS network").Action(c.traceAction)
	trace.Arg("subject", "The subject to publish to").Required().StringVar(&c.subject)
	trace.Arg("payload", "The message body to send").BytesVar(&c.payload)
	trace.Flag("deliver", "Deliver the message to the final destination").UnNegatableBoolVar(&c.deliver)
	trace.Flag("timestamp", "Show event timestamps").Short('T').UnNegatableBoolVar(&c.showTs)
	trace.Flag("header", "Adds headers to the trace message using K:V format").Short('H').StringMapVar(&c.header)
}

func init() {
	registerCommand("trace", 0, configureTraceCommand)
}

func (c *traceCmd) traceAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	if !iu.ServerMinVersion(nc, 2, 11, 0) {
		return fmt.Errorf("tracing messages require NATS Server 2.11")
	}

	msg := nats.NewMsg(c.subject)
	for k, v := range c.header {
		msg.Header.Set(k, v)
	}
	msg.Data, err = c.payload.MarshalText()
	if err != nil {
		return err
	}

	deliver := ""
	if c.deliver {
		deliver = "with delivery to the final destination"
	}

	fmt.Printf("Tracing message route to subject %s %s\n\n", c.subject, deliver)

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	var traces chan *nats.Msg
	if opts().Trace {
		traces = make(chan *nats.Msg, 10)
		wg.Add(1)
		go func(ctx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()

			for {
				select {
				case msg := <-traces:
					fmt.Printf(">>> %s\n", string(msg.Data))
				case <-ctx.Done():
					return
				}
			}

		}(lctx, &wg)
	}

	event, err := tracing.TraceMsg(nc, msg, c.deliver, opts().Timeout, traces)
	if (event == nil || !errors.Is(err, nats.ErrTimeout)) && err != nil {
		return err
	}

	cancel()
	wg.Wait()

	ingress := event.Ingress()
	ts := event.Server.Time
	if ingress != nil {
		ts = ingress.Timestamp
	}

	stat := &traceStats{}

	c.renderTrace(event, ts, stat, 0)

	fmt.Println()
	fmt.Printf("Legend: Client: %s Router: %s Gateway: %s Leafnode: %s JetStream: %s Error: --X\n",
		c.arrowForKind(server.CLIENT),
		c.arrowForKind(server.ROUTER),
		c.arrowForKind(server.GATEWAY),
		c.arrowForKind(server.LEAF),
		c.arrowForKind(server.JETSTREAM),
	)
	fmt.Println()
	if len(stat.egress) > 0 {
		cols := newColumns("Egress Count:")
		stat.Lock()
		cols.AddMapInts(stat.egress, true, true)
		stat.Unlock()
		cols.Frender(os.Stdout)
	}

	if v, ok := stat.egress[jsm.ServerKindString(server.JETSTREAM)]; ok && v > 1 {
		fmt.Printf("WARNING: The message was received by %d streams", v)
		fmt.Println()
	}

	return nil
}

func (c *traceCmd) renderTrace(event *server.MsgTraceEvent, ts time.Time, stat *traceStats, indent int) {
	if event == nil {
		return
	}

	mu := sync.Mutex{}
	prefix := strings.Repeat(" ", indent*4)

	printf := func(format string, a ...any) {
		mu.Lock()
		fmt.Printf(prefix+format+"\n", a...)
		mu.Unlock()
	}

	printlines := func(lines []string) {
		for _, line := range lines {
			printf(line)
		}
	}

	printlines(c.renderIngress(event, event.Ingress(), ts))
	printlines(c.renderSubjectMapping(event, event.SubjectMapping(), ts))
	printlines(c.renderServices(event, event.ServiceImports(), ts))
	printlines(c.renderStreams(event, event.StreamExports(), ts))
	printlines(c.renderJetStream(event, event.JetStream(), stat, ts))

	egresses := event.Egresses()
	if len(egresses) == 0 && event.Ingress() != nil && event.Ingress().Kind == server.CLIENT && event.Ingress().Error == "" {
		printlines([]string{"--X No active interest"})
	}
	for _, egress := range egresses {
		printlines(c.renderEgress(event, egress, ts, stat))
		c.renderTrace(egress.Link, egress.Timestamp, stat, indent+1)
	}
}

func (c *traceCmd) renderEgress(event *server.MsgTraceEvent, egress *server.MsgTraceEgress, ts time.Time, stat *traceStats) []string {
	if egress.Error != "" && event.Server.ID == "" {
		return []string{fmt.Sprintf("!!! Egress Error: %v", egress.Error)}
	}

	parts := []string{c.arrowForKind(egress.Kind)}

	if egress.Name == "" {
		parts = append(parts, fmt.Sprintf("%s %s", jsm.ServerKindString(egress.Kind), jsm.ServerCidString(egress.Kind, egress.CID)))
	} else {
		parts = append(parts, fmt.Sprintf("%s %q %s", jsm.ServerKindString(egress.Kind), egress.Name, jsm.ServerCidString(egress.Kind, egress.CID)))
	}

	if egress.Account != "" {
		parts = append(parts, c.pairs("account", egress.Account)...)
	}

	if egress.Subscription != "" {
		parts = append(parts, c.pairs("subject", egress.Subscription)...)
	}
	if egress.Queue != "" {
		parts = append(parts, c.pairs("queue", egress.Queue)...)
	}

	lines := []string{strings.Join(parts, " ")}
	if egress.Error != "" {
		lines = append(lines, fmt.Sprintf("  --X Error: %s", egress.Error))
	}

	stat.incEgressKind(egress.Kind)

	return c.tsPrefixLines(lines, ts, egress.Timestamp)
}

func (c *traceCmd) renderSubjectMapping(event *server.MsgTraceEvent, mapping *server.MsgTraceSubjectMapping, ts time.Time) []string {
	if mapping == nil {
		return nil
	}

	return []string{c.tsPrefix(fmt.Sprintf("    Mapping subject:%q", mapping.MappedTo), ts, mapping.Timestamp)}
}

func (c *traceCmd) renderJetStream(event *server.MsgTraceEvent, jetstream *server.MsgTraceJetStream, stat *traceStats, ts time.Time) []string {
	if jetstream == nil {
		return nil
	}

	if jetstream.Error != "" && jetstream.Stream == "" {
		return []string{fmt.Sprintf("!!! Expected JetStream event: %v", jetstream.Error)}
	}

	action := "stored"
	if jetstream.NoInterest {
		action = "no interest"
	}

	parts := []string{fmt.Sprintf("%s JetStream action:%q stream:%q subject:%q", c.arrowForKind(server.JETSTREAM), action, jetstream.Stream, jetstream.Subject)}

	lines := []string{strings.Join(parts, " ")}
	if jetstream.Error != "" {
		lines = append(lines, fmt.Sprintf("--X Error: %s", jetstream.Error))
	}

	stat.incEgressKind(server.JETSTREAM)

	return c.tsPrefixLines(lines, ts, jetstream.Timestamp)
}

func (c *traceCmd) renderStreams(event *server.MsgTraceEvent, streams []*server.MsgTraceStreamExport, ts time.Time) []string {
	res := []string{}
	for _, stream := range streams {
		res = append(res, c.tsPrefix(fmt.Sprintf("    Stream Export subject:%q account:%q", stream.To, stream.Account), ts, stream.Timestamp))
	}
	return res
}

func (c *traceCmd) renderServices(event *server.MsgTraceEvent, services []*server.MsgTraceServiceImport, ts time.Time) []string {
	res := []string{}
	for _, svc := range services {
		res = append(res, c.tsPrefix(fmt.Sprintf("    Service Import from:%q to:%q account:%q", svc.From, svc.To, svc.Account), ts, svc.Timestamp))
	}
	return res
}

func (c *traceCmd) renderIngress(event *server.MsgTraceEvent, ingress *server.MsgTraceIngress, ts time.Time) []string {
	if ingress == nil {
		return nil
	}

	if ingress.Error != "" && event.Server.ID == "" {
		return []string{fmt.Sprintf("  !!! Ingress Error: %v", ingress.Error)}
	}

	var parts []string

	if ingress.Name == "" {
		parts = append(parts, fmt.Sprintf("%s %s", jsm.ServerKindString(ingress.Kind), jsm.ServerCidString(ingress.Kind, ingress.CID)))
	} else {
		parts = append(parts, fmt.Sprintf("%s %q %s", jsm.ServerKindString(ingress.Kind), ingress.Name, jsm.ServerCidString(ingress.Kind, ingress.CID)))
	}

	if event.Server.Cluster != "" && (ingress.Kind == server.GATEWAY || ingress.Kind == server.CLIENT) {
		parts = append(parts, c.pairs("cluster", event.Server.Cluster)...)
	}

	parts = append(parts, c.pairs("server", event.Server.Name, "version", event.Server.Version)...)

	lines := []string{strings.Join(parts, " ")}
	if ingress.Error != "" {
		lines = append(lines, fmt.Sprintf("--X Error: %s", ingress.Error))
	}

	return c.tsPrefixLines(lines, ts, ingress.Timestamp)
}

func (c *traceCmd) arrowForKind(kind int) string {
	switch kind {
	case server.CLIENT:
		return "--C"
	case server.JETSTREAM:
		return "--J"
	case server.ROUTER:
		return "-->"
	case server.GATEWAY:
		return "==>"
	case server.LEAF:
		return "~~>"
	default:
		return ""
	}
}

func (c *traceCmd) tsPrefixLines(msgs []string, ots time.Time, ets time.Time) []string {
	res := []string{}

	for _, line := range msgs {
		res = append(res, c.tsPrefix(line, ots, ets))
	}

	return res
}

func (c *traceCmd) tsPrefix(msg string, ots time.Time, ets time.Time) string {
	var since time.Duration
	if !(ots.IsZero() && ets.IsZero()) {
		since = ets.Sub(ots).Round(time.Microsecond)
	}

	ts := since.String()
	if since < 0 {
		ts = "Skew"
	}

	if c.showTs {
		return fmt.Sprintf("[%s] %s", ts, msg)
	}

	return msg
}

func (c *traceCmd) pairs(parts ...string) []string {
	if len(parts)%2 != 0 {
		panic("invalid pairs")
	}

	var ret []string
	for i := 0; i < len(parts)-1; i = i + 2 {
		ret = append(ret, fmt.Sprintf("%s:%q", parts[i], parts[i+1]))
	}

	return ret
}

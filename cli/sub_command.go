// Copyright 2020-2025 The NATS Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/natscli/internal/asciigraph"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/jetstreamext"
	terminal "golang.org/x/term"
)

type subCmd struct {
	subjects              []string
	queue                 string
	durable               string
	raw                   bool
	translate             string
	jsAck                 bool
	inbox                 bool
	match                 bool
	dump                  string
	limit                 uint
	sseq                  uint64
	deliverAll            bool
	deliverNew            bool
	stopAtPendingZero     bool
	reportSubjects        bool
	reportSubjectsCount   int
	reportSub             bool
	deliverLast           bool
	deliverSince          string
	deliverLastPerSubject bool
	headersOnly           bool
	stream                string
	jetStream             bool
	ignoreSubjects        []string
	wait                  time.Duration
	timeStamps            bool
	deltaTimeStamps       bool
	subjectsOnly          bool
	graphOnly             bool
	width                 int
	height                int
	messageRates          map[string]*subMessageRate
	direct                bool
	streamObj             jetstream.Stream
}

type subMessageRate struct {
	lastCount int
	rates     []float64
}

// subscriptionState holds the shared, mutable state used during command execution
type subscriptionState struct {
	// msgMu guards concurrent access to message counters and printing logic
	msgMu *sync.Mutex

	// cancelFn is called to terminate sub
	cancelFn context.CancelFunc

	// counter tracks how many messages have been received across all subjects
	counter uint

	// startTime is the timestamp at which the command execution started
	startTime time.Time

	// subjMu guards concurrent access to the subject reporting maps
	subjMu *sync.Mutex

	// ignoreSubjects is the list of subjects to skip when receiving messages
	ignoreSubjects []string

	// subjectReportMap tracks how many messages were received per subject
	subjectReportMap map[string]int64

	// subjectBytesReportMap tracks how many bytes were received per subject
	subjectBytesReportMap map[string]int64

	// matchMap holds inbound request messages, keyed by their expected reply subject
	matchMap map[string]*nats.Msg

	// dump determines whether messages should be dumped to stdout or files
	dump bool
}

func configureSubCommand(app commandHost) {
	c := &subCmd{}

	subHelp := `
	Jetstream will be activated when related options like --stream, --durable, direct or --ack are supplied.

		E.g. nats sub <subject that is bound to a stream> --all

	Uses an ephemeral consumer without ack by default.  

	For specific consumer options please pre-create a consumer using 'nats consumer add'.app.

		E.g. when explicit acknowledgement is required.

	Use nats stream view <stream> for inspecting messages.	
		
	`

	act := app.Command("subscribe", "Generic subscription client").Alias("sub").Action(c.subscribe)
	act.HelpLong(subHelp)
	addCheat("sub", act)

	act.Arg("subjects", "Subjects to subscribe to").StringsVar(&c.subjects)
	act.Flag("queue", "Subscribe to a named queue group").StringVar(&c.queue)
	act.Flag("durable", "Use a durable consumer (requires JetStream)").StringVar(&c.durable)
	act.Flag("raw", "Show the raw data received").Short('r').UnNegatableBoolVar(&c.raw)
	act.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.translate)
	act.Flag("ack", "Acknowledge JetStream message that have the correct metadata").BoolVar(&c.jsAck)
	act.Flag("match-replies", "Match replies to requests").UnNegatableBoolVar(&c.match)
	act.Flag("inbox", "Subscribes to a generated inbox").Short('i').UnNegatableBoolVar(&c.inbox)
	act.Flag("count", "Quit after receiving this many messages").UintVar(&c.limit)
	act.Flag("dump", "Dump received messages to files, 1 file per message. Specify - for null terminated STDOUT for use with xargs -0").PlaceHolder("DIRECTORY").StringVar(&c.dump)
	act.Flag("headers-only", "Do not render any data, shows only headers").UnNegatableBoolVar(&c.headersOnly)
	act.Flag("subjects-only", "Prints only the messages' subjects").UnNegatableBoolVar(&c.subjectsOnly)
	act.Flag("start-sequence", "Starts at a specific Stream sequence (requires JetStream)").PlaceHolder("SEQUENCE").Uint64Var(&c.sseq)
	act.Flag("all", "Delivers all messages found in the Stream (requires JetStream)").UnNegatableBoolVar(&c.deliverAll)
	act.Flag("new", "Delivers only future messages (requires JetStream)").UnNegatableBoolVar(&c.deliverNew)
	act.Flag("last", "Delivers the most recent and all future messages (requires JetStream)").UnNegatableBoolVar(&c.deliverLast)
	act.Flag("since", "Delivers messages received since a duration like 1d3h5m2s(requires JetStream)").PlaceHolder("DURATION").StringVar(&c.deliverSince)
	act.Flag("last-per-subject", "Deliver the most recent messages for each subject in the Stream (requires JetStream)").UnNegatableBoolVar(&c.deliverLastPerSubject)
	act.Flag("terminate-at-end", "Stops consuming messages from JetStream once all messages were received").Short('T').UnNegatableBoolVar(&c.stopAtPendingZero)
	act.Flag("stream", "Subscribe to a specific stream (required JetStream)").PlaceHolder("STREAM").StringVar(&c.stream)
	act.Flag("ignore-subject", "Subjects for which corresponding messages will be ignored and therefore not shown in the output").Short('I').PlaceHolder("SUBJECT").StringsVar(&c.ignoreSubjects)
	act.Flag("wait", "Unsubscribe after this amount of time without any traffic").DurationVar(&c.wait)
	act.Flag("report-subjects", "Subscribes to subject patterns and builds a de-duplicated report of active subjects receiving data").UnNegatableBoolVar(&c.reportSubjects)
	act.Flag("report-subscriptions", "Subscribes to subject patterns and builds a de-duplicated report of active subscriptions receiving data").UnNegatableBoolVar(&c.reportSub)
	act.Flag("report-top", "Number of subjects to show when doing 'report-subjects'").Default("10").IntVar(&c.reportSubjectsCount)
	act.Flag("timestamp", "Show timestamps in output").Short('t').UnNegatableBoolVar(&c.timeStamps)
	act.Flag("delta-time", "Show time since start in output").Short('d').UnNegatableBoolVar(&c.deltaTimeStamps)
	act.Flag("graph", "Graph the rate of messages received").UnNegatableBoolVar(&c.graphOnly)
	act.Flag("direct", "Subscribe using batched direct gets instead of a durable consumer (requires JetStream)").UnNegatableBoolVar(&c.direct)
}

func init() {
	registerCommand("sub", 17, configureSubCommand)
}

func (c *subCmd) startGraph(ctx context.Context, mu *sync.Mutex) {
	resizeData := func(data []float64, width int) []float64 {
		if width <= 0 {
			return data
		}

		length := len(data)

		if length > width {
			return data[length-width:]
		}

		return data
	}

	go func() {
		ticker := time.NewTicker(time.Second)

		for {
			select {
			case <-ticker.C:
				mu.Lock()

				width, height, err := terminal.GetSize(int(os.Stdout.Fd()))
				if err != nil {
					width = c.width
					height = c.height
				}
				c.width = width
				c.height = height
				if c.width > 15 {
					c.width -= 10
				}
				if c.height > 10 {
					c.height -= 6
				}

				iu.ClearScreen()

				for _, subject := range c.subjects {
					rates := c.messageRates[subject]
					count := 0
					if rates.lastCount > 0 {
						count = rates.lastCount
						rates.lastCount = 0
					}
					rates.rates = append(rates.rates, float64(count))
					rates.rates = resizeData(rates.rates, c.width)

					msgRatePlot := asciigraph.Plot(rates.rates,
						asciigraph.Caption(fmt.Sprintf("%s / second", subject)),
						asciigraph.Width(c.width),
						asciigraph.Height((c.height/(len(c.subjects)+1))-1),
						asciigraph.LowerBound(0),
						asciigraph.Precision(0),
						asciigraph.ValueFormatter(fFloatFixedDecimal),
					)
					fmt.Println(msgRatePlot)
					fmt.Println()
				}

				mu.Unlock()
			case <-ctx.Done():
				ticker.Stop()
			}
		}
	}()
}

func (c *subCmd) startSubjectReporting(ctx context.Context, subjMu *sync.Mutex, subjectReportMap map[string]int64, subjectBytesReportMap map[string]int64, subjCount int) {
	go func() {
		ticker := time.NewTicker(time.Second)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
			case <-ticker.C:
				subjectRows := [][]any{}

				if runtime.GOOS != "windows" {
					fmt.Print("\033[2J")
					fmt.Print("\033[H")
				}

				totalBytes := int64(0)
				totalCount := int64(0)

				subjMu.Lock()
				keys := make([]string, 0, len(subjectReportMap))

				for k := range subjectReportMap {
					keys = append(keys, k)
				}
				// sort.Strings(keys)
				// sort by count in descending order, and if count is equal, by
				// subject in ascending order.
				sort.Slice(keys, func(i, j int) bool {
					lhs, rhs := subjectReportMap[keys[i]], subjectReportMap[keys[j]]
					if lhs == rhs {
						return keys[i] < keys[j]
					}
					return lhs > rhs
				})

				for count, k := range keys {

					subjectRows = append(subjectRows, []any{k, f(subjectReportMap[k]), fiBytes(uint64(subjectBytesReportMap[k]))})
					totalCount += subjectReportMap[k]
					totalBytes += subjectBytesReportMap[k]
					if (count + 1) == subjCount {
						break
					}
				}
				subjMu.Unlock()

				tableHeaderString := ""
				if subjCount == 1 {
					tableHeaderString = "Top Subject Report"
				} else {
					tableHeaderString = fmt.Sprintf("Top %d Active Subjects Report", subjCount)
				}
				table := iu.NewTableWriter(opts(), tableHeaderString)
				table.AddHeaders("Subject", "Message Count", "Bytes")
				table.AddFooter("Totals", f(totalCount), fiBytes(uint64(totalBytes)))
				for i := range subjectRows {
					table.AddRow(subjectRows[i]...)
				}
				fmt.Println(table.Render())
			}
		}
	}()
}

func (c *subCmd) validateInputs(ctx context.Context, nc *nats.Conn, mgr *jsm.Manager, js jetstream.JetStream) error {
	c.jetStream = c.sseq > 0 || len(c.durable) > 0 || c.deliverAll || c.deliverNew || c.deliverLast || c.deliverSince != "" || c.deliverLastPerSubject || c.stream != "" || c.direct

	if len(c.subjects) > 0 && c.durable != "" {
		return fmt.Errorf("cannot specify both subjects and a durable consumer")
	}

	switch {
	case len(c.subjects) == 0 && c.inbox:
		c.subjects = []string{nc.NewRespInbox()}
	case len(c.subjects) == 0 && c.stream == "":
		return fmt.Errorf("subject is required")
	case len(c.subjects) > 1 && c.jetStream:
		return fmt.Errorf("streams subscribe support only 1 subject")
	}

	if c.stopAtPendingZero && !c.jetStream {
		return fmt.Errorf("--terminate-at-end is only supported for JetStream subscriptions")
	}
	if c.inbox && c.jetStream {
		return fmt.Errorf("generating inboxes is not compatible with JetStream subscriptions")
	}
	if c.queue != "" && c.jetStream {
		return fmt.Errorf("queue group subscriptions are not supported with JetStream")
	}
	if c.dump == "-" && c.inbox {
		return fmt.Errorf("generating inboxes is not compatible with dumping to stdout using null terminated strings")
	}
	if c.reportSubjects && c.reportSubjectsCount == 0 {
		return fmt.Errorf("subject count must be at least one")
	}
	if c.reportSub {
		c.reportSubjects = true
	}

	if c.timeStamps && c.deltaTimeStamps {
		return fmt.Errorf("timestamp and delta-time flags are mutually exclusive")
	}

	if c.dump != "" && c.dump != "-" {
		err := os.MkdirAll(c.dump, 0700)
		if err != nil {
			return err
		}
	}

	if c.jetStream {
		var err error
		// if we are passed subjects without a stream, we determine the stream name before trying to load it for further checks
		if c.stream == "" {
			c.stream, err = js.StreamNameBySubject(ctx, c.subjects[0])
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return fmt.Errorf("no stream found with subject: %s", c.subjects[0])
			}
			if err != nil {
				return err
			}
		}

		c.streamObj, err = js.Stream(ctx, c.stream)
		if err != nil {
			return err
		}

		config := c.streamObj.CachedInfo().Config
		c.direct = c.direct || config.Retention == jetstream.WorkQueuePolicy || config.Retention == jetstream.InterestPolicy

		if c.direct {
			if len(c.subjects) > 1 {
				return fmt.Errorf("cannot enable direct gets: cannot perform direct gets from multiple subjects")

			}
			// Direct Addressing needs to be allowed on the stream
			if !c.streamObj.CachedInfo().Config.AllowDirect {
				return fmt.Errorf("cannot enable direct gets: allow_direct is not set to true for stream %s", c.stream)
			}
			err := iu.RequireAPILevel(mgr, 1, "subscribing using direct get requires NATS Server 2.11")
			if err != nil {
				return err
			}
		}

		if c.match {
			return fmt.Errorf("cannot enable --match-replies for JetStream streams")
		}
	}

	return nil
}

func (c *subCmd) createMsgHandler(subState subscriptionState, subs *[]*nats.Subscription, t *time.Timer) nats.MsgHandler {
	return func(m *nats.Msg) {
		subState.msgMu.Lock()
		defer subState.msgMu.Unlock()

		if c.shouldIgnore(m.Subject, subState.ignoreSubjects) {
			return
		}

		if c.shouldAck(m) {
			defer func() {
				err := m.Respond(nil)
				if err != nil && !subState.dump && !c.raw {
					log.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
				}
			}()
		}

		subState.counter++

		switch {
		case c.reportSubjects:
			c.handleSubjectReport(m, subState.subjMu, subState.subjectReportMap, subState.subjectBytesReportMap)
		case c.graphOnly:
			c.handleGraphUpdate(m, subState.subjMu)
		default:
			c.handleMsg(m, subState.matchMap, subState)
		}

		if c.limit > 0 && subState.counter == c.limit {
			for _, sub := range *subs {
				sub.Unsubscribe()
			}

			// if no reply matching, or if didn't yet get all replies
			if !c.match || len(subState.matchMap) == 0 {
				subState.cancelFn()
			}
			return
		}

		if t != nil && !t.Reset(c.wait) {
			subState.cancelFn()
			return
		}
	}
}

// createJetStreamMsgHandler sets up a message handler specifically for jetstream.Msg message types
func (c *subCmd) createJetStreamMsgHandler(subState subscriptionState, nc *nats.Conn, consumers *[]jetstream.ConsumeContext, t *time.Timer) jetstream.MessageHandler {
	return func(m jetstream.Msg) {
		subState.msgMu.Lock()
		defer subState.msgMu.Unlock()

		if c.shouldIgnore(m.Subject(), subState.ignoreSubjects) {
			return
		}

		if c.handleJetstreamFlowContolMsg(m, nc) {
			return
		}

		if c.jsAck {
			defer func() {
				err := m.Ack()
				if err != nil && !subState.dump && !c.raw {
					log.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
				}
			}()
		}

		subState.counter++

		if c.reportSubjects {
			c.handleJetStreamSubjectReport(m, subState.subjMu, subState.subjectReportMap, subState.subjectBytesReportMap)
		} else {
			c.printJetStreamMsg(m, nil, subState.counter)
		}

		meta, _ := m.Metadata()
		if (c.limit > 0 && subState.counter == c.limit) || (c.stopAtPendingZero && meta != nil && meta.NumPending == 0) {
			for _, cCtx := range *consumers {
				cCtx.Stop()
			}

			// if no reply matching, or if didn't yet get all replies
			if !c.match || len(subState.matchMap) == 0 {
				subState.cancelFn()
			}

			return
		}

		if t != nil && !t.Reset(c.wait) {
			subState.cancelFn()
			return
		}
	}
}

func (c *subCmd) shouldIgnore(subject string, ignoreSubjects []string) bool {
	for _, ignore := range ignoreSubjects {
		if server.SubjectsCollide(subject, ignore) {
			return true
		}
	}
	return false
}

func (c *subCmd) handleJetstreamFlowContolMsg(m jetstream.Msg, nc *nats.Conn) bool {
	if len(m.Data()) == 0 && m.Headers().Get("Status") == "100" {
		if m.Reply() != "" {
			err := nc.Publish(m.Reply(), nil)
			if err != nil {
				log.Printf("Failed to respond to Flow Control message%s", err)
			}
			log.Printf("Responding to Flow Control message")
		} else if stalled := m.Headers().Get("Nats-Consumer-Stalled"); stalled != "" {
			err := nc.Publish(stalled, nil)
			if err != nil {
				log.Printf("Failed to restart stalled consumer %s", err)
			}
			log.Printf("Resuming stalled consumer")
		}
		return true
	}
	return false
}

func (c *subCmd) shouldAck(m *nats.Msg) bool {
	var info *jsm.MsgInfo
	if m.Reply != "" {
		info, _ = jsm.ParseJSMsgMetadata(m)
	}
	if c.jsAck && info != nil {
		return true
	}
	return false
}

func (c *subCmd) handleSubjectReport(m *nats.Msg, mu *sync.Mutex, reportMap map[string]int64, bytesMap map[string]int64) {
	sub := m.Subject
	if c.reportSub && m.Sub != nil {
		sub = m.Sub.Subject
	}
	mu.Lock()
	reportMap[sub]++
	bytesMap[sub] += int64(len(m.Data))
	mu.Unlock()
}

func (c *subCmd) handleJetStreamSubjectReport(m jetstream.Msg, mu *sync.Mutex, reportMap map[string]int64, bytesMap map[string]int64) {
	sub := m.Subject()
	mu.Lock()
	reportMap[sub]++
	bytesMap[sub] += int64(len(m.Data()))
	mu.Unlock()
}

func (c *subCmd) handleGraphUpdate(m *nats.Msg, mu *sync.Mutex) {
	if m.Sub == nil {
		return
	}
	mu.Lock()
	c.messageRates[m.Sub.Subject].lastCount++
	mu.Unlock()
}

func (c *subCmd) handleMsg(m *nats.Msg, matchMap map[string]*nats.Msg, subState subscriptionState) {
	if c.match && m.Reply != "" {
		matchMap[m.Reply] = m
	} else {
		c.printMsg(m, nil, subState.counter, subState.startTime)
	}
}

func (c *subCmd) createMatchHandler(subState subscriptionState) nats.MsgHandler {
	return func(reply *nats.Msg) {
		subState.msgMu.Lock()
		defer subState.msgMu.Unlock()

		request, ok := subState.matchMap[reply.Subject]
		if !ok {
			return
		}

		c.printMsg(request, reply, subState.counter, subState.startTime)
		delete(subState.matchMap, reply.Subject)

		// if reached limit and matched all requests
		if subState.counter == c.limit && len(subState.matchMap) == 0 {
			if reply.Sub != nil {
				reply.Sub.Unsubscribe()
			}
			subState.cancelFn()
		}
	}
}

func (c *subCmd) graphSubscribe(ctx context.Context, subState subscriptionState, nc *nats.Conn, handler nats.MsgHandler, subs *[]*nats.Subscription) error {
	width, height, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("failed to get terminal dimensions: %w", err)
	}
	if width < 20 || height < 20 {
		return fmt.Errorf("please increase terminal dimensions")
	}
	if width > 15 {
		width -= 10
	}
	if height > 10 {
		height -= 6
	}

	c.width = width
	c.height = height
	c.messageRates = make(map[string]*subMessageRate)
	for _, subject := range c.subjects {
		c.messageRates[subject] = &subMessageRate{
			rates: make([]float64, width),
		}
	}

	if len(c.subjects) > 4 {
		return fmt.Errorf("maximum 4 subject patterns may be graphed")
	}

	err = c.defaultSubscribe(nc, handler, subs)
	if err != nil {
		return err
	}

	c.startGraph(ctx, subState.subjMu)
	return nil
}

func (c *subCmd) reportSubscribe(ctx context.Context, subState subscriptionState, nc *nats.Conn, handler nats.MsgHandler, subs *[]*nats.Subscription) error {
	err := c.defaultSubscribe(nc, handler, subs)
	if err != nil {
		return err
	}

	c.startSubjectReporting(ctx, subState.subjMu, subState.subjectReportMap, subState.subjectBytesReportMap, c.reportSubjectsCount)
	return nil
}

// jetStreamSubscribe creates a jetstream specific subscription
func (c *subCmd) jetStreamSubscribe(ctx context.Context, subState subscriptionState, handler jetstream.MessageHandler, consumerContexts *[]jetstream.ConsumeContext, js jetstream.JetStream) error {
	subMsg := c.firstSubject()
	if subMsg == "" {
		subMsg = f(c.streamObj.CachedInfo().Config.Subjects)
	}

	ignoredSubjInfo := ""
	if len(subState.ignoreSubjects) > 0 {
		ignoredSubjInfo = fmt.Sprintf("\nIgnored subjects: %s", f(subState.ignoreSubjects))
	}

	var cons jetstream.Consumer
	var pcons jetstream.PushConsumer

	consumerConfig, err := c.makeConsumerConfig(subMsg, ignoredSubjInfo)
	if err != nil {
		return err
	}

	isPushConsumer := false
	var info *jetstream.ConsumerInfo

	if c.durable != "" {
		cons, pcons, info, isPushConsumer, err = c.setupDurableConsumer(ctx, js, consumerConfig)
		if err != nil {
			return err
		}
		c.jsAck = info.Config.AckPolicy != jetstream.AckNonePolicy
		if err := c.setConsumerSubjects(info); err != nil {
			return err
		}
	} else {
		cons, err = c.streamObj.CreateConsumer(ctx, consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to create consumer: %w", err)
		}
		c.jsAck = false
	}

	var cCtx jetstream.ConsumeContext
	if isPushConsumer {
		cCtx, err = pcons.Consume(handler)
	} else {
		cCtx, err = cons.Consume(handler)
	}
	if err != nil {
		return err
	}
	*consumerContexts = append(*consumerContexts, cCtx)

	return nil
}

// reportJetStreamSubscribe sets up a jetstream specific subscription for subject reporting
func (c *subCmd) reportJetStreamSubscribe(ctx context.Context, subState subscriptionState, handler jetstream.MessageHandler, consumerContexts *[]jetstream.ConsumeContext, js jetstream.JetStream) error {
	err := c.jetStreamSubscribe(ctx, subState, handler, consumerContexts, js)
	if err != nil {
		return err
	}

	c.startSubjectReporting(ctx, subState.subjMu, subState.subjectReportMap, subState.subjectBytesReportMap, c.reportSubjectsCount)
	return nil
}

func (c *subCmd) makeConsumerConfig(subject, ignoredInfo string) (jetstream.ConsumerConfig, error) {
	cfg := jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckNonePolicy,
		HeadersOnly:   c.headersOnly || c.subjectsOnly,
		FilterSubject: subject,
	}

	switch {
	case c.sseq > 0:
		log.Printf("Subscribing to subject %s starting at sequence %d %s", subject, c.sseq, ignoredInfo)
		cfg.OptStartSeq = c.sseq
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
	case c.deliverLast:
		log.Printf("Subscribing to subject %s from last received message %s", subject, ignoredInfo)
		cfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case c.deliverAll:
		log.Printf("Subscribing to subject %s from first received message %s", subject, ignoredInfo)
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case c.deliverNew:
		log.Printf("Subscribing to subject %s for new messages %s", subject, ignoredInfo)
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case c.deliverSince != "":
		d, err := fisk.ParseDuration(c.deliverSince)
		if err != nil {
			return cfg, err
		}
		start := time.Now().Add(-d)
		cfg.OptStartTime = &start
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		log.Printf("Subscribing to subject %s since %s %s", subject, f(d), ignoredInfo)
	case c.deliverLastPerSubject:
		log.Printf("Subscribing to subject %s for last message per subject %s", subject, ignoredInfo)
		cfg.DeliverPolicy = jetstream.DeliverLastPerSubjectPolicy
		cfg.FilterSubject = subject
	}

	return cfg, nil
}

func (c *subCmd) setupDurableConsumer(ctx context.Context, js jetstream.JetStream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, jetstream.PushConsumer, *jetstream.ConsumerInfo, bool, error) {
	var info *jetstream.ConsumerInfo
	cons, err := c.streamObj.Consumer(ctx, c.durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		cfg.Name = c.durable
		cons, err := c.streamObj.CreateConsumer(ctx, cfg)
		if err != nil {
			return nil, nil, nil, false, fmt.Errorf("failed to create durable consumer %q: %w", c.durable, err)
		}
		info, err = cons.Info(ctx)
		if err != nil {
			return nil, nil, nil, false, fmt.Errorf("error fetching consumer info: %w", err)
		}
		return cons, nil, info, false, nil
	}

	if err == jetstream.ErrNotPullConsumer {
		pcons, err := js.PushConsumer(ctx, c.stream, c.durable)
		if err != nil {
			return nil, nil, nil, true, err
		}
		info, err = pcons.Info(ctx)
		if err != nil {
			return nil, nil, nil, true, err
		}
		log.Printf("Subscribing to JetStream Stream %q using existing push consumer %q", c.stream, c.durable)
		log.Printf("Detected push consumer %q on stream %q — push consumers are deprecated and will be removed in a future release. Please update to use a pull-based consumer.", c.durable, c.stream)
		return nil, pcons, info, true, nil
	}

	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("error loading durable consumer %q: %w", c.durable, err)
	}

	if info == nil {
		info, err = cons.Info(ctx)
		if err != nil {
			return nil, nil, nil, false, fmt.Errorf("error fetching consumer info: %w", err)
		}
	}
	log.Printf("Subscribing to JetStream Stream %q using existing pull consumer %q", c.stream, c.durable)
	return cons, nil, info, false, nil
}

func (c *subCmd) setConsumerSubjects(info *jetstream.ConsumerInfo) error {
	switch {
	case len(info.Config.FilterSubjects) > 1:
		return fmt.Errorf("cannot subscribe to multi filter consumers")
	case len(info.Config.FilterSubjects) == 1:
		c.subjects = info.Config.FilterSubjects
	case info.Config.FilterSubject != "":
		c.subjects = []string{info.Config.FilterSubject}
	}
	return nil
}

func (c *subCmd) defaultSubscribe(nc *nats.Conn, handler nats.MsgHandler, subs *[]*nats.Subscription) error {
	for _, subj := range c.subjects {
		var sub *nats.Subscription
		var err error

		if c.queue == "" {
			sub, err = nc.Subscribe(subj, handler)
		} else {
			sub, err = nc.QueueSubscribe(subj, c.queue, handler)
		}
		if err != nil {
			return err
		}

		*subs = append(*subs, sub)
	}

	return nil
}

func (c *subCmd) directSubscribe(subCtx context.Context, subState subscriptionState, handler nats.MsgHandler, js jetstream.JetStream) error {
	ignoredSubjInfo := ""
	if len(subState.ignoreSubjects) > 0 {
		ignoredSubjInfo = fmt.Sprintf("\nIgnored subjects: %s", f(subState.ignoreSubjects))
	}

	subMsg := c.firstSubject()
	if c.stream != "" {
		if len(c.subjects) == 0 {
			subMsg = f(c.streamObj.CachedInfo().Config.Subjects)
		}
	}

	// GetBatch called later assumes that the first msg is at sequence 0. We get the real starting position here so
	// we can pass it on later
	info, err := c.streamObj.Info(ctx)
	streamState := info.State
	batchSequence := streamState.FirstSeq
	batchSize := 1
	lastForSubjects := false
	var start time.Time
	if err != nil {
		return err
	}

	switch {
	case c.sseq > 0:
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s starting with sequence %d %s", subMsg, c.sseq, ignoredSubjInfo)
		batchSequence = c.sseq
	case c.deliverLast:
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s starting with the last message received %s", subMsg, ignoredSubjInfo)
		batchSequence = streamState.LastSeq
	case c.deliverAll:
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s starting with the first message received %s", subMsg, ignoredSubjInfo)
		batchSequence = streamState.FirstSeq
	case c.deliverNew:
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s delivering any new messages received %s", subMsg, ignoredSubjInfo)
		batchSequence = streamState.LastSeq + 1
	case c.deliverSince != "":
		d, err := fisk.ParseDuration(c.deliverSince)
		if err != nil {
			return err
		}
		start = time.Now().Add(-1 * d)
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s starting with messages since %s %s", subMsg, f(d), ignoredSubjInfo)
		batchSequence = streamState.LastSeq + 1
	case c.deliverLastPerSubject:
		log.Printf("Subscribing to JetStream Stream (direct) holding messages with subject %s for the last messages for each subject in the Stream %s", subMsg, ignoredSubjInfo)
		lastForSubjects = true
		batchSequence = streamState.LastSeq
	}

	for {
		var msgs iter.Seq2[*jetstream.RawStreamMsg, error]
		if lastForSubjects {
			msgs, err = jetstreamext.GetLastMsgsFor(subCtx, js, c.stream, c.subjects, jetstreamext.GetLastMsgsUpToSeq(batchSequence), jetstreamext.GetLastMsgsBatchSize(batchSize))
		} else {
			if !start.IsZero() {
				msgs, err = jetstreamext.GetBatch(subCtx, js, c.stream, batchSize, jetstreamext.GetBatchStartTime(start))
				// We only get the messages from start up to now, then we continue to use batchSequence
				start = time.Time{}
			} else {
				msgs, err = jetstreamext.GetBatch(subCtx, js, c.stream, batchSize, jetstreamext.GetBatchSeq(batchSequence))
			}
		}

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

	msgProcessing:
		for msg, err := range msgs {
			switch {
			case errors.Is(err, jetstreamext.ErrNoMessages):
				break msgProcessing
			case errors.Is(err, context.Canceled):
				return nil
			case err != nil:
				return err
			}

			// Put the relevant fields in a nats.Msg for printing. Omitting the Reply and Sub pointer will guard against
			// the handler functions doing something we can't do with a direct message
			nmsg := &nats.Msg{
				Subject: msg.Subject,
				Header:  msg.Header,
				Data:    msg.Data,
			}

			handler(nmsg)
			batchSequence += uint64(batchSize)
		}
	}
}

func (c *subCmd) subscribe(p *fisk.ParseContext) error {
	if len(c.subjects) == 0 && c.stream == "" && !c.inbox {
		fmt.Println("No subjects or --stream flag provided.")
		return fisk.ErrRequiredArgument
	}

	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	var (
		subs             []*nats.Subscription
		consumerContexts []jetstream.ConsumeContext
		ctx, cancel      = context.WithCancel(ctx)
	)

	subState := subscriptionState{
		msgMu:                 &sync.Mutex{},
		cancelFn:              cancel,
		counter:               0,
		startTime:             time.Now(),
		subjMu:                &sync.Mutex{},
		ignoreSubjects:        iu.SplitCLISubjects(c.ignoreSubjects),
		subjectReportMap:      make(map[string]int64),
		subjectBytesReportMap: make(map[string]int64),
		matchMap:              make(map[string]*nats.Msg),
		dump:                  c.dump != "",
	}

	js, err := newJetStreamWithOptions(nc, opts())
	if err != nil {
		return err
	}
	err = c.validateInputs(ctx, nc, mgr, js)
	if err != nil {
		return err
	}

	defer cancel()

	// If the wait timeout is set, then we will cancel after the timer fires.
	var t *time.Timer
	if c.wait > 0 {
		t = time.AfterFunc(c.wait, func() {
			cancel()
		})
		defer t.Stop()
	}

	if c.reportSubjects {
		subState.subjectReportMap = make(map[string]int64)
		subState.subjectBytesReportMap = make(map[string]int64)
	}

	msgHandler := c.createMsgHandler(subState, &subs, t)
	jetstreamMsgHandler := c.createJetStreamMsgHandler(subState, nc, &consumerContexts, t)

	matchHandler := c.createMatchHandler(subState)

	if c.match {
		inSubj := "_INBOX.>"
		if opts().InboxPrefix != "" {
			inSubj = fmt.Sprintf("%v.>", opts().InboxPrefix)
		}

		if !c.raw && c.dump == "" {
			log.Printf("Matching replies with inbox prefix %v", inSubj)
		}

		_, err = nc.Subscribe(inSubj, matchHandler)
		if err != nil {
			return err
		}
	}

	var ignoredSubjInfo string
	if len(subState.ignoreSubjects) > 0 {
		ignoredSubjInfo = fmt.Sprintf("\nIgnored subjects: %s", f(subState.ignoreSubjects))
	}

	if (!c.raw && c.dump == "") || c.inbox {
		switch {
		case c.jetStream:
			// logs later depending on settings
		case c.jsAck:
			log.Printf("Subscribing on %s with acknowledgement of JetStream messages %s", c.firstSubject(), ignoredSubjInfo)
		default:
			log.Printf("Subscribing on %s %s", strings.Join(c.subjects, ", "), ignoredSubjInfo)
		}
	}

	switch {
	case c.graphOnly:
		err = c.graphSubscribe(ctx, subState, nc, msgHandler, &subs)
	case c.reportSubjects:
		if c.jetStream {
			err = c.reportJetStreamSubscribe(ctx, subState, jetstreamMsgHandler, &consumerContexts, js)
		} else {
			err = c.reportSubscribe(ctx, subState, nc, msgHandler, &subs)
		}
	case c.direct:
		err = c.directSubscribe(ctx, subState, msgHandler, js)
	case c.jetStream:
		err = c.jetStreamSubscribe(ctx, subState, jetstreamMsgHandler, &consumerContexts, js)
	default:
		err = c.defaultSubscribe(nc, msgHandler, &subs)
	}
	if err != nil {
		return err
	}

	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

func (c *subCmd) firstSubject() string {
	if len(c.subjects) == 0 {
		return ""
	}

	return c.subjects[0]
}

func (c *subCmd) printMsg(msg *nats.Msg, reply *nats.Msg, ctr uint, startTime time.Time) {
	var replyMsg *nats.Msg
	if reply != nil {
		replyMsg = c.makeMsg(reply.Subject, reply.Header, reply.Data, reply.Reply)
	}

	var ts string
	if c.timeStamps {
		ts = fmt.Sprintf(" @ %s", time.Now().Format(time.StampMicro))
	} else if c.deltaTimeStamps {
		ts = fmt.Sprintf(" @ %s", time.Since(startTime).String())
	}

	if opts().Trace && msg.Reply != "" {
		fmt.Printf("<<< Reply Subject: %v\n", msg.Reply)
	}

	var info *jsm.MsgInfo
	if msg.Reply != "" || c.direct {
		info, _ = jsm.ParseJSMsgMetadata(msg)
	}

	// Output format 1: dumping, to stdout or files
	if c.dump != "" {
		stdout := c.dump == "-"
		seq := fmt.Sprintf("%d", ctr)

		if info != nil {
			seq = fmt.Sprintf("%d", info.StreamSequence())
		}

		reqFile := filepath.Join(c.dump, fmt.Sprintf("%s.json", seq))
		repFile := filepath.Join(c.dump, fmt.Sprintf("%s_reply.json", seq))

		c.dumpMsg(msg, stdout, reqFile, ctr)
		if replyMsg != nil {
			c.dumpMsg(replyMsg, stdout, repFile, ctr)
		}
		return
	}

	// Output format 2: raw
	if c.raw {
		outPutMSGBodyCompact(msg.Data, c.translate, "", "")
		if replyMsg != nil {
			fmt.Println(string(replyMsg.Data))
		}
		return
	}

	// Output format 3: pretty
	if c.direct && info != nil {
		fmt.Printf("[#%d]%s Received JetStream message (direct): stream: %s seq %d / subject: %s / time: %v\n",
			ctr, ts, info.Stream(), info.StreamSequence(), msg.Subject, f(info.TimeStamp()))
	} else {
		fmt.Printf("[#%d]%s Received on %q", ctr, ts, msg.Subject)
		if msg.Reply != "" {
			fmt.Printf(" with reply %q", msg.Reply)
		}
		fmt.Println()
	}

	if !c.subjectsOnly {
		c.prettyPrintMsg(msg, c.headersOnly, c.translate)
	}

	if replyMsg != nil {
		if info != nil {
			fmt.Printf("[#%d]%s Matched reply JetStream message: stream: %s seq %d / subject: %s / time: %v\n",
				ctr, ts, info.Stream(), info.StreamSequence(), replyMsg.Subject, f(info.TimeStamp()))
		} else {
			fmt.Printf("[#%d]%s Matched reply on %q\n", ctr, ts, replyMsg.Subject)
		}
		c.prettyPrintMsg(replyMsg, c.headersOnly, c.translate)
	}
}

func (c *subCmd) printJetStreamMsg(msg jetstream.Msg, reply jetstream.Msg, ctr uint) {
	meta, err := msg.Metadata()
	if err != nil {
		log.Printf("Failed to parse message metadata: %s", err)
		return
	}

	dataMsg := c.makeMsg(msg.Subject(), msg.Headers(), msg.Data(), msg.Reply())

	var replyMsg *nats.Msg
	if reply != nil {
		replyMsg = c.makeMsg(reply.Subject(), reply.Headers(), reply.Data(), reply.Reply())
	}

	var ts string
	if c.timeStamps {
		ts = fmt.Sprintf(" @ %s", meta.Timestamp.Format(time.StampMicro))
	} else if c.deltaTimeStamps {
		ts = fmt.Sprintf(" @ %s", time.Since(meta.Timestamp).String())
	}

	if opts().Trace && dataMsg.Reply != "" {
		fmt.Printf("<<< Reply Subject: %v\n", dataMsg.Reply)
	}

	summary := fmt.Sprintf("stream: %s seq: %s / pending: %s / subject: %s / time: %s", meta.Stream, f(meta.Sequence.Stream), f(meta.NumPending), dataMsg.Subject, f(meta.Timestamp))

	// Output format 1: dumping, to stdout or files
	if c.dump != "" {
		stdout := c.dump == "-"
		seq := fmt.Sprintf("%d", meta.Sequence.Stream)
		reqFile := filepath.Join(c.dump, fmt.Sprintf("%s.json", seq))
		repFile := filepath.Join(c.dump, fmt.Sprintf("%s_reply.json", seq))

		c.dumpMsg(dataMsg, stdout, reqFile, ctr)
		if replyMsg != nil {
			c.dumpMsg(replyMsg, stdout, repFile, ctr)
		}
		return
	}

	// Output format 2: raw
	if c.raw {
		outPutMSGBodyCompact(dataMsg.Data, c.translate, "", "")
		if replyMsg != nil {
			fmt.Println(string(replyMsg.Data))
		}
		return
	}

	// Output format 3: pretty
	fmt.Printf("[#%d]%s Received JetStream message: %s\n", ctr, ts, summary)

	if !c.subjectsOnly {
		c.prettyPrintMsg(dataMsg, c.headersOnly, c.translate)
	}

	if replyMsg != nil {
		fmt.Printf("[#%d] Matched reply JetStream message: %s\n", ctr, summary)
		c.prettyPrintMsg(replyMsg, c.headersOnly, c.translate)
	}
}

func (c *subCmd) dumpMsg(msg *nats.Msg, stdout bool, filepath string, ctr uint) {
	serMsg := c.makeMsg(msg.Subject, msg.Header, msg.Data, msg.Reply)

	if c.translate != "" {
		data, err := filterDataThroughCmd(msg.Data, c.translate, "", "")
		if err != nil {
			log.Printf("%q\nError while translating msg body: %s\n\n", data, err.Error())
			return
		}
		serMsg.Data = data
	}

	jm, err := json.Marshal(serMsg)
	if err != nil {
		log.Printf("Could not JSON encode message: %s", err)
	} else if stdout {
		fmt.Fprintf(os.Stdout, "%s\000", jm)
	} else {
		err = os.WriteFile(filepath, jm, 0600)
		if err != nil {
			log.Printf("Could not save message: %s", err)
		}
		if ctr%100 == 0 {
			fmt.Print(".")
		}
	}
}

func (c *subCmd) makeMsg(subject string, headers nats.Header, data []byte, reply string) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Header = headers
	msg.Data = data
	msg.Reply = reply
	return msg
}

func (c *subCmd) prettyPrintMsg(msg *nats.Msg, headersOnly bool, filter string) {
	c.printHeadersAndBody(msg.Header, msg.Data, msg.Subject, headersOnly, filter)
}

func (c *subCmd) printHeadersAndBody(hdr nats.Header, data []byte, subject string, headersOnly bool, filter string) {
	if len(hdr) > 0 {
		for h, vals := range hdr {
			for _, val := range vals {
				fmt.Printf("%s: %s\n", h, val)
			}
		}
		fmt.Println()
	}

	if !headersOnly {
		outPutMSGBody(data, filter, subject, "")
	}
}

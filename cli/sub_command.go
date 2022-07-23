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

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type subCmd struct {
	subject               string
	queue                 string
	durable               string
	raw                   bool
	jsAck                 bool
	inbox                 bool
	match                 bool
	dump                  string
	limit                 uint
	sseq                  uint64
	deliverAll            bool
	deliverNew            bool
	deliverLast           bool
	deliverSince          string
	deliverLastPerSubject bool
	headersOnly           bool
	stream                string
	jetStream             bool
	excludeSubjets        []string
}

func configureSubCommand(app commandHost) {
	c := &subCmd{}
	act := app.Command("subscribe", "Generic subscription client").Alias("sub").Action(c.subscribe)
	addCheat("sub", act)

	act.Arg("subject", "Subject to subscribe to").StringVar(&c.subject)
	act.Flag("queue", "Subscribe to a named queue group").StringVar(&c.queue)
	act.Flag("durable", "Use a durable consumer (requires JetStream)").StringVar(&c.durable)
	act.Flag("raw", "Show the raw data received").Short('r').UnNegatableBoolVar(&c.raw)
	act.Flag("ack", "Acknowledge JetStream message that have the correct metadata").BoolVar(&c.jsAck)
	act.Flag("match-replies", "Match replies to requests").UnNegatableBoolVar(&c.match)
	act.Flag("inbox", "Subscribes to a generate inbox").Short('i').UnNegatableBoolVar(&c.inbox)
	act.Flag("count", "Quit after receiving this many messages").UintVar(&c.limit)
	act.Flag("dump", "Dump received messages to files, 1 file per message. Specify - for null terminated STDOUT for use with xargs -0").PlaceHolder("DIRECTORY").StringVar(&c.dump)
	act.Flag("headers-only", "Do not render any data, shows only headers").UnNegatableBoolVar(&c.headersOnly)
	act.Flag("start-sequence", "Starts at a specific Stream sequence (requires JetStream)").PlaceHolder("SEQUENCE").Uint64Var(&c.sseq)
	act.Flag("all", "Delivers all messages found in the Stream (requires JetStream").UnNegatableBoolVar(&c.deliverAll)
	act.Flag("new", "Delivers only future messages (requires JetStream)").UnNegatableBoolVar(&c.deliverNew)
	act.Flag("last", "Delivers the most recent and all future messages (requires JetStream)").UnNegatableBoolVar(&c.deliverLast)
	act.Flag("since", "Delivers messages received since a duration (requires JetStream)").PlaceHolder("DURATION").StringVar(&c.deliverSince)
	act.Flag("last-per-subject", "Deliver the most recent messages for each subject in the Stream (requires JetStream)").UnNegatableBoolVar(&c.deliverLastPerSubject)
	act.Flag("stream", "Subscribe to a specific stream (required JetStream)").PlaceHolder("STREAM").StringVar(&c.stream)
	act.Flag("exclude-subjects", "Subjects for which corresponding messages will be excluded and therefore not shown in the output").StringsVar(&c.excludeSubjets)
}

func init() {
	registerCommand("sub", 17, configureSubCommand)
}

func (c *subCmd) subscribe(p *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.subject == "" && c.inbox {
		c.subject = nc.NewRespInbox()
	} else if c.subject == "" && c.stream == "" {
		return fmt.Errorf("subject is required")
	}

	if c.dump == "-" && c.inbox {
		return fmt.Errorf("generating inboxes is not compatible with dumping to stdout using null terminated strings")
	}

	c.jetStream = c.sseq > 0 || len(c.durable) > 0 || c.deliverAll || c.deliverNew || c.deliverLast || c.deliverSince != "" || c.deliverLastPerSubject || c.stream != ""

	if c.inbox && c.jetStream {
		return fmt.Errorf("generating inboxes is not compatible with JetStream subscriptions")
	}
	if c.queue != "" && c.jetStream {
		return fmt.Errorf("queu group subscriptions are not supported with JetStream")
	}

	var (
		sub             *nats.Subscription
		mu              = sync.Mutex{}
		dump            = c.dump != ""
		ctr             = uint(0)
		excludeSubjects = splitCLISubjects(c.excludeSubjets)
		ctx, cancel     = context.WithCancel(ctx)

		replySub *nats.Subscription
		matchMap map[string]*nats.Msg
	)
	defer cancel()

	handler := func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		var info *jsm.MsgInfo
		if m.Reply != "" {
			info, _ = jsm.ParseJSMsgMetadata(m)
		}

		if c.jsAck && info != nil {
			defer func() {
				err = m.Respond(nil)
				if err != nil && !dump && !c.raw {
					log.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
				}
			}()
		}

		// flow control
		if len(m.Data) == 0 && m.Header.Get("Status") == "100" {
			if m.Reply != "" {
				m.Respond(nil)
				log.Printf("Responding to Flow Control message")
			} else if stalled := m.Header.Get("Nats-Consumer-Stalled"); stalled != "" {
				nc.Publish(stalled, nil)
				log.Printf("Resuming stalled consumer")
			}
		}

		if len(excludeSubjects) > 0 {
			for _, excludeSubj := range excludeSubjects {
				if server.SubjectsCollide(m.Subject, excludeSubj) {
					return
				}
			}
		}

		ctr++
		if c.match && m.Reply != "" {
			matchMap[m.Reply] = m
		} else {
			printMsg(c, m, nil, ctr)
		}

		if ctr == c.limit {
			sub.Unsubscribe()
			// if no reply matching, or if didn't yet get all replies
			if !c.match || len(matchMap) == 0 {
				cancel()
			}
		}
	}

	matchHandler := func(reply *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		request, ok := matchMap[reply.Subject]
		if !ok {
			return
		}

		printMsg(c, request, reply, ctr)
		delete(matchMap, reply.Subject)

		// if reached limit and matched all requests
		if ctr == c.limit && len(matchMap) == 0 {
			replySub.Unsubscribe()
			cancel()
		}
	}

	if c.match {
		inSubj := "_INBOX.>"
		if opts.InboxPrefix != "" {
			inSubj = fmt.Sprintf("%v.>", opts.InboxPrefix)
		}

		matchMap = make(map[string]*nats.Msg)
		replySub, err = nc.Subscribe(inSubj, matchHandler)
		if err != nil {
			return err
		}
	}

	var excludedSubjInfo string
	if len(excludeSubjects) > 0 {
		excludedSubjInfo = fmt.Sprintf("\nExcluded subjects: %s", strings.Join(excludeSubjects, ", "))
	}

	if (!c.raw && c.dump == "") || c.inbox {
		switch {
		case c.jetStream:
			// logs later depending on settings
		case c.jsAck:
			log.Printf("Subscribing on %s with acknowledgement of JetStream messages %s", c.subject, excludedSubjInfo)
		default:
			log.Printf("Subscribing on %s %s", c.subject, excludedSubjInfo)
		}
	}

	switch {
	case c.jetStream:
		var js nats.JetStreamContext
		js, err = nc.JetStream()
		if err != nil {
			return err
		}

		opts := []nats.SubOpt{
			nats.EnableFlowControl(),
			nats.IdleHeartbeat(5 * time.Second),
			nats.AckNone(),
		}

		if c.headersOnly {
			opts = append(opts, nats.HeadersOnly())
		}

		if len(c.durable) > 0 {
			opts = append(opts, nats.Durable(c.durable))
		}

		subMsg := c.subject
		if c.stream != "" {
			if c.subject == "" {
				str, err := js.StreamInfo(c.stream)
				if err != nil {
					return err
				}
				subMsg = strings.Join(str.Config.Subjects, ", ")
			}
			opts = append(opts, nats.BindStream(c.stream))
		}

		switch {
		case c.sseq > 0:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with sequence %d %s", subMsg, c.sseq, excludedSubjInfo)
			opts = append(opts, nats.StartSequence(c.sseq))
		case c.deliverLast:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with the last message received %s", subMsg, excludedSubjInfo)
			opts = append(opts, nats.DeliverLast())
		case c.deliverAll:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with the first message received %s", subMsg, excludedSubjInfo)

			opts = append(opts, nats.DeliverAll())
		case c.deliverNew:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s delivering any new messages received %s", subMsg, excludedSubjInfo)

			opts = append(opts, nats.DeliverNew())
		case c.deliverSince != "":
			var d time.Duration
			d, err = parseDurationString(c.deliverSince)
			if err != nil {
				return err
			}

			start := time.Now().Add(-1 * d)
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with messages since %s %s", subMsg, humanizeDuration(d), excludeSubjects)

			opts = append(opts, nats.StartTime(start))
		case c.deliverLastPerSubject:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s for the last messages for each subject in the Stream %s", subMsg, excludedSubjInfo)
			opts = append(opts, nats.DeliverLastPerSubject())
		}

		c.jsAck = false
		if c.stream != "" {
			sub, err = js.Subscribe("", handler, opts...)
		} else {
			sub, err = js.Subscribe(c.subject, handler, opts...)
		}

	case c.queue != "":
		sub, err = nc.QueueSubscribe(c.subject, c.queue, handler)
	default:
		sub, err = nc.Subscribe(c.subject, handler)
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

func printMsg(c *subCmd, msg *nats.Msg, reply *nats.Msg, ctr uint) {
	var info *jsm.MsgInfo
	if msg.Reply != "" {
		info, _ = jsm.ParseJSMsgMetadata(msg)
	}

	if opts.Trace && msg.Reply != "" {
		fmt.Printf("<<< Reply Subject: %v\n", msg.Reply)
	}

	if c.dump != "" {
		// Output format 1/3: dumping, to stdout or files

		var (
			stdout      = c.dump == "-"
			requestFile string
			replyFile   string
		)
		if !stdout {
			if info == nil {
				requestFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", ctr))
				replyFile = filepath.Join(c.dump, fmt.Sprintf("%d_reply.json", ctr))
			} else {
				requestFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", info.StreamSequence()))
				replyFile = filepath.Join(c.dump, fmt.Sprintf("%d_reply.json", info.StreamSequence()))
			}
		}

		dumpMsg(msg, stdout, requestFile, ctr)
		if reply != nil {
			dumpMsg(reply, stdout, replyFile, ctr)
		}

	} else if c.raw {
		// Output format 2/3: raw

		fmt.Println(string(msg.Data))
		if reply != nil {
			fmt.Println(string(reply.Data))
		}

	} else {
		// Output format 3/3: pretty

		if info == nil {
			if msg.Reply != "" {
				fmt.Printf("[#%d] Received on %q with reply %q\n", ctr, msg.Subject, msg.Reply)
			} else {
				fmt.Printf("[#%d] Received on %q\n", ctr, msg.Subject)
			}
		} else if c.jetStream {
			fmt.Printf("[#%d] Received JetStream message: stream: %s seq %d / subject: %s / time: %v\n", ctr, info.Stream(), info.StreamSequence(), msg.Subject, info.TimeStamp().Format(time.RFC3339))
		} else {
			fmt.Printf("[#%d] Received JetStream message: consumer: %s > %s / subject: %s / delivered: %d / consumer seq: %d / stream seq: %d\n", ctr, info.Stream(), info.Consumer(), msg.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence())
		}

		prettyPrintMsg(msg, c.headersOnly)

		if reply != nil {
			if info == nil {
				fmt.Printf("[#%d] Matched reply on %q\n", ctr, reply.Subject)
			} else if c.jetStream {
				fmt.Printf("[#%d] Matched reply JetStream message: stream: %s seq %d / subject: %s / time: %v\n", ctr, info.Stream(), info.StreamSequence(), reply.Subject, info.TimeStamp().Format(time.RFC3339))
			} else {
				fmt.Printf("[#%d] Matched reply JetStream message: consumer: %s > %s / subject: %s / delivered: %d / consumer seq: %d / stream seq: %d\n", ctr, info.Stream(), info.Consumer(), reply.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence())
			}

			prettyPrintMsg(reply, c.headersOnly)

		}

	} // output format type dispatch
}

func dumpMsg(msg *nats.Msg, stdout bool, filepath string, ctr uint) {
	// dont want sub etc
	serMsg := nats.NewMsg(msg.Subject)
	serMsg.Header = msg.Header
	serMsg.Data = msg.Data
	serMsg.Reply = msg.Reply

	jm, err := json.Marshal(serMsg)
	if err != nil {
		log.Printf("Could not JSON encode message: %s", err)
	} else if stdout {
		os.Stdout.WriteString(fmt.Sprintf("%s\000", jm))
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

func prettyPrintMsg(msg *nats.Msg, headersOnly bool) {
	if len(msg.Header) > 0 {
		for h, vals := range msg.Header {
			for _, val := range vals {
				fmt.Printf("%s: %s\n", h, val)
			}
		}

		fmt.Println()
	}

	if !headersOnly {
		fmt.Println(string(msg.Data))
		if !strings.HasSuffix(string(msg.Data), "\n") {
			fmt.Println()
		}
	}
}

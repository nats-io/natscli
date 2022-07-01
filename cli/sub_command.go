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
	"github.com/nats-io/nats.go"
)

type subCmd struct {
	subject               string
	queue                 string
	durable               string
	raw                   bool
	jsAck                 bool
	inbox                 bool
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
}

func init() {
	registerCommand("sub", 17, configureSubCommand)
}

func (c *subCmd) subscribe(_ *fisk.ParseContext) error {
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

	jetStream := c.sseq > 0 || len(c.durable) > 0 || c.deliverAll || c.deliverNew || c.deliverLast || c.deliverSince != "" || c.deliverLastPerSubject || c.stream != ""

	if c.inbox && jetStream {
		return fmt.Errorf("generating inboxes is not compatible with JetStream subscriptions")
	}
	if c.queue != "" && jetStream {
		return fmt.Errorf("queu group subscriptions are not supported with JetStream")
	}

	var (
		sub         *nats.Subscription
		mu          = sync.Mutex{}
		dump        = c.dump != ""
		ctr         = uint(0)
		ctx, cancel = context.WithCancel(ctx)
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

		ctr++
		if dump {
			// Output format 1/3: dumping, to stdout or files

			stdout := c.dump == "-"
			outFile := ""
			if !stdout {
				if info == nil {
					outFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", ctr))
				} else {
					outFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", info.StreamSequence()))
				}
			}

			// dont want sub etc
			msg := nats.NewMsg(m.Subject)
			msg.Header = m.Header
			msg.Data = m.Data
			msg.Reply = m.Reply

			jm, err := json.Marshal(msg)
			if err != nil {
				log.Printf("Could not JSON encode message: %s", err)
			} else if stdout {
				os.Stdout.WriteString(fmt.Sprintf("%s\000", jm))
			} else {
				err = os.WriteFile(outFile, jm, 0600)
				if err != nil {
					log.Printf("Could not save message: %s", err)
				}

				if ctr%100 == 0 {
					fmt.Print(".")
				}
			}

		} else if c.raw {
			// Output format 2/3: raw

			fmt.Println(string(m.Data))

		} else {
			// Output format 3/3: pretty

			if info == nil {
				if m.Reply != "" {
					fmt.Printf("[#%d] Received on %q with reply %q\n", ctr, m.Subject, m.Reply)
				} else {
					fmt.Printf("[#%d] Received on %q\n", ctr, m.Subject)
				}
			} else if jetStream {
				fmt.Printf("[#%d] Received JetStream message: stream: %s seq %d / subject: %s / time: %v\n", ctr, info.Stream(), info.StreamSequence(), m.Subject, info.TimeStamp().Format(time.RFC3339))
			} else {
				fmt.Printf("[#%d] Received JetStream message: consumer: %s > %s / subject: %s / delivered: %d / consumer seq: %d / stream seq: %d\n", ctr, info.Stream(), info.Consumer(), m.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence())
			}

			if len(m.Header) > 0 {
				for h, vals := range m.Header {
					for _, val := range vals {
						fmt.Printf("%s: %s\n", h, val)
					}
				}

				fmt.Println()
			}

			if !c.headersOnly {
				fmt.Println(string(m.Data))
				if !strings.HasSuffix(string(m.Data), "\n") {
					fmt.Println()
				}
			}

		} // output format type dispatch

		if ctr == c.limit {
			sub.Unsubscribe()
			cancel()
		}
	}

	if (!c.raw && c.dump == "") || c.inbox {
		switch {
		case jetStream:
			// logs later depending on settings
		case c.jsAck:
			log.Printf("Subscribing on %s with acknowledgement of JetStream messages", c.subject)
		default:
			log.Printf("Subscribing on %s", c.subject)
		}
	}

	switch {
	case jetStream:
		var js nats.JetStream
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

		if c.stream != "" {
			opts = append(opts, nats.BindStream(c.stream))
		}

		switch {
		case c.sseq > 0:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with sequence %d", c.subject, c.sseq)
			opts = append(opts, nats.StartSequence(c.sseq))
		case c.deliverLast:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with the last message received", c.subject)
			opts = append(opts, nats.DeliverLast())
		case c.deliverAll:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with the first message received", c.subject)

			opts = append(opts, nats.DeliverAll())
		case c.deliverNew:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s delivering any new messages received", c.subject)

			opts = append(opts, nats.DeliverNew())
		case c.deliverSince != "":
			var d time.Duration
			d, err = parseDurationString(c.deliverSince)
			if err != nil {
				return err
			}

			start := time.Now().Add(-1 * d)
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s starting with messages since %s", c.subject, humanizeDuration(d))

			opts = append(opts, nats.StartTime(start))
		case c.deliverLastPerSubject:
			log.Printf("Subscribing to JetStream Stream holding messages with subject %s for the last messages for each subject in the Stream", c.subject)
			opts = append(opts, nats.DeliverLastPerSubject())
		}

		c.jsAck = false
		sub, err = js.Subscribe(c.subject, handler, opts...)
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

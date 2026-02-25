// Copyright 2020-2026 The NATS Authors
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
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/jetstreamext"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
)

type pubCmd struct {
	subject    string
	body       string
	bodyIsSet  bool
	replyTo    string
	raw        bool
	hdrs       []string
	cnt        int
	sleep      time.Duration
	forceStdin bool
	jetstream  bool
	sendOn     string
	quiet      bool
	templates  bool
	atomic     bool

	atomicPending []*nats.Msg
}

func configurePubCommand(app commandHost) {
	c := &pubCmd{}

	pubHelp := `Body and Header values of the messages may use Go templates to
create unique messages.

   nats pub test --count 10 "Message {{Count}} @ {{Time}}"

Multiple messages with random strings between 10 and 100 long:

   nats pub test --count 10 "Message {{Count}}: {{ Random 10 100 }}"

Available template functions are:

   Count            the message number
   TimeStamp        RFC3339 format current time
   Unix             seconds since 1970 in UTC
   UnixNano         nano seconds since 1970 in UTC
   Time             the current time
   ID               an unique ID
   Random(min, max) random string at least min long, at most max
`

	pub := app.Command("publish", "Generic data publish utility").Alias("pub").Action(c.publishAction)
	addCheat("pub", pub)
	pub.HelpLong(pubHelp)
	pub.Arg("subject", "Subject to publish to").Required().StringVar(&c.subject)
	pub.Arg("body", "Message body").IsSetByUser(&c.bodyIsSet).StringVar(&c.body)
	pub.Flag("reply", "Sets a custom reply to subject").StringVar(&c.replyTo)
	pub.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	pub.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	pub.Flag("sleep", "When publishing multiple messages, sleep between publishes").DurationVar(&c.sleep)
	pub.Flag("force-stdin", "Force reading from stdin").UnNegatableBoolVar(&c.forceStdin)
	pub.Flag("jetstream", "Publish messages to jetstream").Short('J').UnNegatableBoolVar(&c.jetstream)
	pub.Flag("send-on", "When to send data from stdin: 'eof' (default) or 'newline'").Default("eof").EnumVar(&c.sendOn, "newline", "eof")
	pub.Flag("quiet", "Show just the output received").Short('q').UnNegatableBoolVar(&c.quiet)
	pub.Flag("templates", "Enables template functions in the body and subject (does not affect headers)").Default("true").BoolVar(&c.templates)
	pub.Flag("atomic", "Atomic batch publish to Jetstream (implies --jetstream)").UnNegatableBoolVar(&c.atomic)
}

func init() {
	registerCommand("pub", 11, configurePubCommand)
}

func (c *pubCmd) writeAtomic(nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	streamName, err := js.StreamNameBySubject(ctx, c.subject)
	if err != nil {
		return err
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}

	if !stream.CachedInfo().Config.AllowAtomicPublish {
		return fmt.Errorf("atomic publishing is not allowed for stream %s with subject %s", streamName, c.subject)
	}

	batch, err := jetstreamext.NewBatchPublisher(js)
	if err != nil {
		return err
	}

	messageCount := len(c.atomicPending)
	for _, m := range c.atomicPending[:messageCount-1] {
		if err := batch.AddMsg(m); err != nil {
			return err
		}
	}

	// commit with the last element's data
	ack, err := batch.Commit(ctx, c.subject, c.atomicPending[messageCount-1].Data)
	if err != nil {
		return err
	}

	if !c.quiet {
		msg := fmt.Sprintf("Wrote batch ID: %s Messages: %s Sequence: %s", ack.BatchID, f(ack.BatchSize), f(ack.Sequence))
		if ack.Domain != "" {
			msg += fmt.Sprintf(" Domain: %q", ack.Domain)
		}
		if ack.Value != "" {
			msg += fmt.Sprintf(" Counter Value: %s", ack.Value)
		}
		log.Printf(msg)
	}

	return nil
}

func (c *pubCmd) addToBatch(pub *iu.Publisher) error {
	for i := 1; i <= c.cnt; i++ {
		body, subj, bodyErr, subjErr := pub.ParseTemplates(c.body, c.subject, i)
		if bodyErr != nil {
			log.Printf("Could not parse body template: %s", bodyErr)
		}
		if subjErr != nil {
			log.Printf("Could not parse subject template: %s", subjErr)
		}

		msg, err := pub.PrepareMsg(subj, c.replyTo, []byte(body), c.hdrs, i)
		if err != nil {
			return err
		}

		c.atomicPending = append(c.atomicPending, msg)

		if !c.quiet {
			log.Printf("Adding %d bytes to batch on subject %q\n", len(body), subj)
		}
	}

	return nil
}

func (c *pubCmd) doJetstream(nc *nats.Conn, pub *iu.Publisher) error {
	for i := 1; i <= c.cnt; i++ {
		start := time.Now()
		body, subj, bodyErr, subjErr := pub.ParseTemplates(c.body, c.subject, i)
		if bodyErr != nil {
			log.Printf("Could not parse body template: %s", bodyErr)
		}
		if subjErr != nil {
			log.Printf("Could not parse subject template: %s", subjErr)
		}

		msg, err := pub.PrepareMsg(subj, c.replyTo, []byte(body), c.hdrs, i)
		if err != nil {
			return err
		}

		if !c.quiet {
			log.Printf("Published %d bytes to %q\n", len(body), subj)
		}
		resp, err := nc.RequestMsg(msg, opts().Timeout)
		if err != nil {
			return err
		}

		ack, err := jsm.ParsePubAck(resp)
		if err != nil {
			return err
		}

		if opts().Trace {
			fmt.Printf("<<< %+v\n", string(resp.Data))
		}

		tracker := pub.Tracker
		if tracker != nil {
			tracker.Increment(1)
		} else if !c.quiet {
			msg := fmt.Sprintf("Stored in Stream: %s Sequence: %s", ack.Stream, f(ack.Sequence))
			if ack.Domain != "" {
				msg += fmt.Sprintf(" Domain: %q", ack.Domain)
			}
			if ack.Duplicate {
				msg += " Duplicate: true"
			}
			if ack.Value != "" {
				msg += fmt.Sprintf(" Counter Value: %s", ack.Value)
			}
			log.Printf(msg)
		}

		// If applicable, account for the wait duration in a publish sleep.
		if c.cnt > 1 && c.sleep > 0 {
			st := c.sleep - time.Since(start)
			if st > 0 {
				time.Sleep(st)
			}
		}
	}

	return nil
}

func (c *pubCmd) publishAtomicBatch(ctx context.Context, nc *nats.Conn, pub *iu.Publisher) error {
	eof := c.bodyIsSet

	return pub.Run(ctx, func() error {
		for {
			if pub.UseStdin {
				body, newEof, err := pub.ReadStdin()
				if err != nil {
					return err
				}
				if newEof {
					eof = true
				}
				if body == "" && eof {
					break
				}
				c.body = body
			}

			err := c.addToBatch(pub)
			if err != nil {
				log.Printf("Could not publish message: %s", err)
			}

			if pub.IsSendOnEOF() || eof {
				break
			}
		}

		return c.writeAtomic(nc)
	})
}

func (c *pubCmd) publishJetstream(ctx context.Context, nc *nats.Conn, pub *iu.Publisher) error {
	eof := c.bodyIsSet
	defer pub.StopProgress()

	return pub.Run(ctx, func() error {
		for {
			if pub.UseStdin {
				body, newEof, err := pub.ReadStdin()
				if err != nil {
					return err
				}
				if newEof {
					eof = true
				}
				if body == "" && eof {
					return nil
				}
				c.body = body
			}

			err := c.doJetstream(nc, pub)
			if pub.IsSendOnEOF() {
				return err
			} else if pub.IsSendOnNewLine() {
				if err != nil {
					log.Printf("Could not publish message: %s", err)
				}
				if eof {
					return nil
				}
				continue
			}

			if pub.IsSendOnEOF() || eof {
				return nil
			}
		}
	})
}

func (c *pubCmd) publishNatsMsg(ctx context.Context, nc *nats.Conn, pub *iu.Publisher) error {
	eof := c.bodyIsSet
	defer pub.StopProgress()

	return pub.Run(ctx, func() error {
		for {
			if pub.UseStdin {
				body, newEof, err := pub.ReadStdin()
				if err != nil {
					return err
				}
				if newEof {
					eof = true
				}
				if body == "" && eof {
					return nil
				}
				c.body = body
			}

			for i := 1; i <= c.cnt; i++ {
				body, subj, bodyErr, subjErr := pub.ParseTemplates(c.body, c.subject, i)
				if bodyErr != nil {
					log.Printf("Could not parse body template: %s", bodyErr)
				}
				if subjErr != nil {
					log.Printf("Could not parse subject template: %s", subjErr)
				}

				msg, err := pub.PrepareMsg(subj, c.replyTo, []byte(body), c.hdrs, i)
				if err != nil {
					return err
				}

				err = nc.PublishMsg(msg)
				if err != nil {
					return err
				}
				nc.Flush()

				err = nc.LastError()
				if err != nil {
					return err
				}

				if c.cnt > 1 && c.sleep > 0 {
					time.Sleep(c.sleep)
				}

				tracker := pub.Tracker
				if tracker == nil {
					if !c.quiet {
						log.Printf("Published %d bytes to %q\n", len(body), subj)
					}
				} else {
					tracker.Increment(1)
				}
			}

			if pub.IsSendOnEOF() || eof {
				return nil
			}
		}
	})
}

func (c *pubCmd) publishAction(_ *fisk.ParseContext) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.cnt < 1 {
		c.cnt = 1
	}

	pub, err := iu.NewPublisher(iu.PublisherConfig{
		BodyIsSet:  c.bodyIsSet,
		ForceStdin: c.forceStdin,
		Count:      c.cnt,
		Raw:        c.raw,
		Templates:  c.templates,
		Opts:       opts(),
	})
	if err != nil {
		return err
	}

	if c.sendOn == "newline" {
		pub.SetSendOnNewLine()
	}

	if pub.UseStdin && !c.quiet {
		log.Println("Reading payload from STDIN")
	}

	if c.atomic {
		c.jetstream = true
		if !(pub.UseStdin && pub.IsSendOnNewLine()) {
			return fmt.Errorf("atomic batch publishing requires Jetstream and STDIN with --send-on=newline")
		}
		mgr, err := jsm.New(nc)
		if err != nil {
			return err
		}
		if err = iu.RequireAPILevel(mgr, 2, "Atomic Batch Publishing requires NATS Server 2.12"); err != nil {
			return err
		}
		return c.publishAtomicBatch(ctx, nc, pub)
	} else if c.jetstream {
		return c.publishJetstream(ctx, nc, pub)
	} else {
		return c.publishNatsMsg(ctx, nc, pub)
	}
}

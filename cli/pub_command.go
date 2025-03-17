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
	"fmt"
	"io"
	"math"
	"os"
	"time"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	terminal "golang.org/x/term"
)

type pubCmd struct {
	subject      string
	body         string
	req          bool
	replyTo      string
	raw          bool
	hdrs         []string
	cnt          int
	sleep        time.Duration
	replyCount   int
	replyTimeout time.Duration
	forceStdin   bool
	translate    string
	jetstream    bool
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

	pub := app.Command("publish", "Generic data publish utility").Alias("pub").Action(c.publish)
	addCheat("pub", pub)
	pub.HelpLong(pubHelp)
	pub.Arg("subject", "Subject to publish to").Required().StringVar(&c.subject)
	pub.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	pub.Flag("reply", "Sets a custom reply to subject").StringVar(&c.replyTo)
	pub.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	pub.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	pub.Flag("sleep", "When publishing multiple messages, sleep between publishes").DurationVar(&c.sleep)
	pub.Flag("force-stdin", "Force reading from stdin").UnNegatableBoolVar(&c.forceStdin)
	pub.Flag("jetstream", "Publish messages to jetstream").Short('J').UnNegatableBoolVar(&c.jetstream)

	requestHelp := `Body and Header values of the messages may use Go templates to 
create unique messages.

   nats request test --count 10 "Message {{Count}} @ {{Time}}"

Multiple messages with random strings between 10 and 100 long:

   nats request test --count 10 "Message {{Count}}: {{ Random 10 100 }}"

Available template functions are:

   Count            the message number
   TimeStamp        RFC3339 format current time
   Unix             seconds since 1970 in UTC
   UnixNano         nano seconds since 1970 in UTC
   Time             the current time
   ID               an unique ID
   Random(min, max) random string at least min long, at most max
`

	req := app.Command("request", "Generic request-reply request utility").Alias("req").Action(c.publish)
	req.HelpLong(requestHelp)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').UnNegatableBoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	req.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	req.Flag("replies", "Wait for multiple replies from services. 0 waits until timeout").Default("1").IntVar(&c.replyCount)
	req.Flag("reply-timeout", "Maximum timeout between incoming replies.").Default("300ms").DurationVar(&c.replyTimeout)
	req.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.translate)
}

func init() {
	registerCommand("pub", 11, configurePubCommand)
}

func (c *pubCmd) prepareMsg(subj string, body []byte, seq int) (*nats.Msg, error) {
	msg := nats.NewMsg(subj)
	msg.Reply = c.replyTo
	msg.Data = body

	return msg, iu.ParseStringsToMsgHeader(c.hdrs, seq, msg)
}

func (c *pubCmd) doReq(nc *nats.Conn, progress *progress.Tracker) error {
	logOutput := !c.raw && progress == nil

	for i := 1; i <= c.cnt; i++ {
		if logOutput {
			log.Printf("Sending request on %q\n", c.subject)
		}

		body, err := iu.PubReplyBodyTemplate(c.body, "", i)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		subj, err := iu.PubReplyBodyTemplate(c.subject, "", i)
		if err != nil {
			log.Printf("Could not parse subject template: %s", err)
		}

		msg, err := c.prepareMsg(string(subj), body, i)
		if err != nil {
			return err
		}

		msg.Reply = nc.NewRespInbox()

		s, err := nc.SubscribeSync(msg.Reply)
		if err != nil {
			return err
		}

		err = nc.PublishMsg(msg)
		if err != nil {
			return err
		}

		if progress != nil {
			progress.Increment(1)
		}

		// loop through the reply count.
		start := time.Now()

		// Honor the overall timeout for the first response.  No
		// responders will circuit break.
		timeout := opts().Timeout

		// loop until reply count is met, or if zero, until we
		// timeout receiving messages.
		rc := 0
		var rttAg time.Duration
		for {
			m, err := s.NextMsg(timeout)
			if err != nil {
				if err == nats.ErrTimeout {
					// continue to publish additional messages.
					break
				}
				if err == nats.ErrNoResponders {
					log.Printf("No responders are available")
					return nil
				}
				return err
			}

			rtt := time.Since(start)

			switch {
			case c.raw:
				outPutMSGBody(m.Data, c.translate, m.Subject, "")
			case logOutput:
				log.Printf("Received with rtt %v", rtt)

				if len(m.Header) > 0 {
					for h, vals := range m.Header {
						for _, val := range vals {
							log.Printf("%s: %s", h, val)
						}
					}
					fmt.Println()
				}

				outPutMSGBody(m.Data, c.translate, m.Subject, "")
			}

			rc++
			if c.replyCount > 0 && rc == c.replyCount {
				break
			}

			if c.replyCount == 0 {
				// if we are waiting for the general timeout then
				// calculate remaining
				timeout = opts().Timeout - time.Since(start)
			} else {
				// Otherwise, use the average response deltas
				rttAg += rtt
				timeout = rttAg/time.Duration(rc) + c.replyTimeout
			}
		}

		// Unsubscribe for the unbound case, NOOP is already auto unsubscribed.
		s.Unsubscribe()

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

func (c *pubCmd) doJetstream(nc *nats.Conn, progress *progress.Tracker) error {
	for i := 1; i <= c.cnt; i++ {
		start := time.Now()
		body, err := iu.PubReplyBodyTemplate(c.body, "", i)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		subj, err := iu.PubReplyBodyTemplate(c.subject, "", i)
		if err != nil {
			log.Printf("Could not parse subject template: %s", err)
		}

		msg, err := c.prepareMsg(string(subj), body, i)
		if err != nil {
			return err
		}

		msg.Subject = string(subj)

		log.Printf("Published %d bytes to %q\n", len(body), c.subject)

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

		if progress != nil {
			progress.Increment(1)
		} else {
			msg := fmt.Sprintf("Stored in Stream: %s Sequence: %s", ack.Stream, f(ack.Sequence))
			if ack.Domain != "" {
				msg += fmt.Sprintf(" Domain: %q", ack.Domain)
			}
			if ack.Duplicate {
				msg += " Duplicate: true"
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

func (c *pubCmd) publish(_ *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.cnt < 1 {
		c.cnt = math.MaxInt16
	}

	if c.body == "!nil!" && (terminal.IsTerminal(int(os.Stdout.Fd())) || c.forceStdin) {
		log.Println("Reading payload from STDIN")
		body, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		c.body = string(body)
	}

	var tracker *progress.Tracker
	var progbar progress.Writer

	if c.cnt > 20 && !c.raw {
		progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
			Total: int64(c.cnt),
		})
		if err != nil {
			return err
		}

		defer func() {
			progbar.Stop()
			time.Sleep(300 * time.Millisecond)
		}()
	}

	if c.jetstream {
		return c.doJetstream(nc, tracker)
	}

	if c.req || c.replyCount >= 1 {
		return c.doReq(nc, tracker)
	}

	for i := 1; i <= c.cnt; i++ {
		body, err := iu.PubReplyBodyTemplate(c.body, "", i)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		subj, err := iu.PubReplyBodyTemplate(c.subject, "", i)
		if err != nil {
			log.Printf("Could not parse subject template: %s", err)
		}

		msg, err := c.prepareMsg(string(subj), body, i)
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

		if progbar == nil {
			log.Printf("Published %d bytes to %q\n", len(body), c.subject)
		} else {
			tracker.Increment(1)
		}
	}

	return nil
}

// Copyright 2026 The NATS Authors
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
	"math"
	"os/signal"
	"syscall"
	"time"

	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/nats.go"
	iu "github.com/nats-io/natscli/internal/util"
)

type reqCmd struct {
	subject      string
	body         string
	bodyIsSet    bool
	req          bool
	replyTo      string
	raw          bool
	hdrs         []string
	cnt          int
	replyCount   int
	replyTimeout time.Duration
	forceStdin   bool
	translate    string
	sendOn       string
	quiet        bool
	templates    bool
	sleep        time.Duration
}

func configureReqCommand(app commandHost) {
	c := &reqCmd{}

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

	req := app.Command("request", "Generic request-reply request utility").Alias("req").Action(c.requestAction)
	req.HelpLong(requestHelp)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").IsSetByUser(&c.bodyIsSet).StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').UnNegatableBoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	req.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	req.Flag("replies", "Wait for multiple replies from services. 0 waits until timeout").Default("1").IntVar(&c.replyCount)
	req.Flag("reply-timeout", "Maximum timeout between incoming replies.").Default("300ms").DurationVar(&c.replyTimeout)
	req.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.translate)
	req.Flag("force-stdin", "Force reading from stdin").UnNegatableBoolVar(&c.forceStdin)
	req.Flag("send-on", fmt.Sprintf("When to send data from stdin: '%s' (default) or '%s'", iu.SendOnEOF, iu.SendOnNewline)).Default("eof").EnumVar(&c.sendOn, iu.SendOnNewline, iu.SendOnEOF)
	req.Flag("templates", "Enables template functions in the body and subject (does not affect headers)").Default("true").BoolVar(&c.templates)
}

func init() {
	registerCommand("req", 11, configureReqCommand)
}

func (c *reqCmd) doReq(nc *nats.Conn, progress *progress.Tracker) error {
	logOutput := !c.raw && progress == nil

	for i := 1; i <= c.cnt; i++ {
		if logOutput {
			log.Printf("Sending request on %q\n", c.subject)
		}

		body, subj, bodyErr, subjErr := iu.ParseTemplates(c.body, c.subject, i, c.templates)
		if bodyErr != nil {
			log.Printf("Could not parse body template: %s", bodyErr)
		}
		if subjErr != nil {
			log.Printf("Could not parse subject template: %s", subjErr)
		}
		msg, err := iu.PrepareMsg(subj, c.replyTo, []byte(body), c.hdrs, i)
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
					log.Println()
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

func (c *reqCmd) requestAction(_ *fisk.ParseContext) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	reader, useStdin := iu.SetupStdin(c.bodyIsSet, c.forceStdin)
	if useStdin && !c.quiet {
		log.Println("Reading payload from STDIN")
	}
	complete := make(chan struct{})

	eof := c.bodyIsSet
	if c.cnt < 1 {
		c.cnt = math.MaxInt16
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(complete)

		var tracker *progress.Tracker
		var progbar progress.Writer

		progbar, tracker, err = iu.SetupProgressBar(c.cnt, c.raw, opts())
		if err != nil {
			errCh <- err
			return
		}
		if progbar != nil {
			defer func() {
				progbar.Stop()
				time.Sleep(300 * time.Millisecond)
			}()
		}

		for {
			if useStdin {
				body, newEof, err := iu.ReadStdin(reader, c.sendOn)
				if err != nil {
					errCh <- err
					return
				}
				if newEof {
					eof = true
				}
				if body == "" && eof {
					errCh <- nil
					return
				}
				c.body = body
			}

			err := c.doReq(nc, tracker)
			if err != nil {
				errCh <- err
				return
			}

			if c.sendOn == iu.SendOnEOF || eof {
				errCh <- nil
				return
			}
		}
	}()

	return iu.CleanupOnInterrupt(ctx, reader, complete, errCh)
}

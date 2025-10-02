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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/jetstreamext"

	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	terminal "golang.org/x/term"
)

const (
	sendOnEOF     = "eof"
	sendOnNewline = "newline"
)

type pubCmd struct {
	subject      string
	body         string
	bodyIsSet    bool
	req          bool
	replyTo      string
	raw          bool
	hdrs         []string
	cnt          int
	sleep        time.Duration
	replyCount   int
	repliesAll   bool
	replyTimeout time.Duration
	forceStdin   bool
	translate    string
	jetstream    bool
	sendOn       string
	quiet        bool
	templates    bool
	atomic       bool

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

	pub := app.Command("publish", "Generic data publish utility").Alias("pub").Action(c.publish)
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
	pub.Flag("send-on", fmt.Sprintf("When to send data from stdin: '%s' (default) or '%s'", sendOnEOF, sendOnNewline)).Default("eof").EnumVar(&c.sendOn, sendOnNewline, sendOnEOF)
	pub.Flag("quiet", "Show just the output received").Short('q').UnNegatableBoolVar(&c.quiet)
	pub.Flag("templates", "Enables template functions in the body and subject (does not affect headers)").Default("true").BoolVar(&c.templates)
	pub.Flag("atomic", "Atomic batch publish to Jetstream (implies --jetstream)").UnNegatableBoolVar(&c.atomic)

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
	req.Arg("body", "Message body").IsSetByUser(&c.bodyIsSet).StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').UnNegatableBoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	req.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	req.Flag("replies", "Wait for multiple replies from services. 0 waits until timeout").Default("1").IntVar(&c.replyCount)
	req.Flag("replies-all", fmt.Sprintf("Wait for all replies from services. Requires the service to include the header '%s' with the number of replies in the first reply.", hdrReplyCount)).Default("false").UnNegatableBoolVar(&c.repliesAll)
	req.Flag("reply-timeout", "Maximum timeout between incoming replies.").Default("300ms").DurationVar(&c.replyTimeout)
	req.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.translate)
	req.Flag("send-on", fmt.Sprintf("When to send data from stdin: '%s' (default) or '%s'", sendOnEOF, sendOnNewline)).Default("eof").EnumVar(&c.sendOn, sendOnNewline, sendOnEOF)
	req.Flag("templates", "Enables template functions in the body and subject (does not affect headers)").Default("true").BoolVar(&c.templates)
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

func (c *pubCmd) parseTemplates(request string, ctr int) (string, string) {
	if c.templates {
		body, err := iu.PubReplyBodyTemplate(c.body, request, ctr)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		subj, err := iu.PubReplyBodyTemplate(c.subject, request, ctr)
		if err != nil {
			log.Printf("Could not parse subject template: %s", err)
		}
		return string(body), string(subj)
	}
	return c.body, c.subject
}

func (c *pubCmd) doReq(nc *nats.Conn, progress *progress.Tracker) error {
	logOutput := !c.raw && progress == nil

	for i := 1; i <= c.cnt; i++ {
		if logOutput {
			log.Printf("Sending request on %q\n", c.subject)
		}

		body, subj := c.parseTemplates("", i)
		msg, err := c.prepareMsg(subj, []byte(body), i)
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

			if c.repliesAll && rc == 0 {
				repliesAll := m.Header.Get(hdrReplyCount)
				if repliesAll != "" {
					reployCount, err := strconv.ParseInt(repliesAll, 0, 64)
					if err != nil {
						// TODO(joeriddles): gracefully handle non-numeric reply counter
						return err
					}
					c.replyCount = int(reployCount)
				}
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

func (c *pubCmd) addToBatch() error {
	for i := 1; i <= c.cnt; i++ {
		body, subj := c.parseTemplates("", i)

		msg, err := c.prepareMsg(subj, []byte(body), i)
		if err != nil {
			return err
		}

		c.atomicPending = append(c.atomicPending, msg)

		if !c.quiet {
			log.Printf("Adding %d bytes to batch on subject %q\n", len(body), c.subject)
		}
	}

	return nil
}

func (c *pubCmd) doJetstream(nc *nats.Conn, progress *progress.Tracker) error {
	for i := 1; i <= c.cnt; i++ {
		start := time.Now()
		body, subj := c.parseTemplates("", i)

		msg, err := c.prepareMsg(subj, []byte(body), i)
		if err != nil {
			return err
		}

		if !c.quiet {
			log.Printf("Published %d bytes to %q\n", len(body), c.subject)
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

		if progress != nil {
			progress.Increment(1)
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

// readLine reads a full line from a bufio.Reader regardless of buffer size,
// this is important for reading lines longer than the bufio.Reader's default
// buffer size and nicer than wholesale enlarging the buffer.
func readLine(reader *bufio.Reader) (string, error) {
	var buf bytes.Buffer

	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			return buf.String(), err
		}

		buf.Write(line)
		if !isPrefix {
			return buf.String(), err
		}
	}
}

func (c *pubCmd) publish(_ *fisk.ParseContext) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	var readPipe *io.PipeReader
	var writePipe *io.PipeWriter
	var reader *bufio.Reader

	// Only initialize stdin pipes if configured
	useStdin := !c.bodyIsSet && (terminal.IsTerminal(int(os.Stdout.Fd())) || c.forceStdin)
	if useStdin {
		if !c.quiet {
			log.Println("Reading payload from STDIN")
		}

		readPipe, writePipe = io.Pipe()
		reader = bufio.NewReader(readPipe)

		go func() {
			_, errPipe := io.Copy(writePipe, os.Stdin)
			if errPipe != nil {
				writePipe.CloseWithError(errPipe)
			} else {
				_ = writePipe.Close()
			}
		}()
	}

	complete := make(chan struct{})

	// If a body is set, treat it as EOF, as no more input
	eof := c.bodyIsSet
	if c.cnt < 1 {
		c.cnt = math.MaxInt16
	}
	if c.atomic {
		c.jetstream = true
		if !(useStdin && c.sendOn == sendOnNewline) {
			return fmt.Errorf("atomic batch publishing requires Jetstream and STDIN with --send-on=newline")
		}
		mgr, err := jsm.New(nc)
		if err != nil {
			return err
		}
		if err = iu.RequireAPILevel(mgr, 2, "Atomic Batch Publishing requires NATS Server 2.12"); err != nil {
			return err
		}
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(complete)

		var tracker *progress.Tracker
		var progbar progress.Writer

		if c.cnt > 20 && !c.raw {
			progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(c.cnt),
			})
			if err != nil {
				errCh <- err
				return
			}

			defer func() {
				progbar.Stop()
				time.Sleep(300 * time.Millisecond)
			}()
		}

		for {
			if useStdin {
				switch c.sendOn {
				case sendOnEOF:
					body, err := io.ReadAll(readPipe)
					if err != nil {
						errCh <- err
						return
					}
					c.body = string(body)
				case sendOnNewline:
					body, err := readLine(reader)
					if err != nil && err != io.EOF {
						errCh <- err
						return
					} else if err == io.EOF {
						eof = true
					}
					if body == "" && eof {
						errCh <- nil
						return
					}
					c.body = body
				}
			}

			// We add the line from stdin here, but we can't commit until we've reached EOF
			// commit() happens in the select(), when the goroutine is finished but before
			// our connection is closed
			if c.atomic {
				err = c.addToBatch()
				if err != nil {
					log.Printf("Could not publish message: %s", err)
				}
				continue
			}

			if c.jetstream {
				err = c.doJetstream(nc, tracker)
				if c.sendOn == sendOnEOF {
					errCh <- err
					return
				} else if c.sendOn == sendOnNewline {
					if err != nil {
						log.Printf("Could not publish message: %s", err)
					}
					continue
				}

			}

			if c.req || c.replyCount >= 1 {
				errCh <- c.doReq(nc, tracker)
				return
			}

			for i := 1; i <= c.cnt; i++ {
				body, subj := c.parseTemplates("", i)

				msg, err := c.prepareMsg(subj, []byte(body), i)
				if err != nil {
					errCh <- err
					return
				}

				err = nc.PublishMsg(msg)
				if err != nil {
					errCh <- err
					return
				}
				nc.Flush()

				err = nc.LastError()
				if err != nil {
					errCh <- err
					return
				}

				if c.cnt > 1 && c.sleep > 0 {
					time.Sleep(c.sleep)
				}

				if progbar == nil {
					if !c.quiet {
						log.Printf("Published %d bytes to %q\n", len(body), c.subject)
					}
				} else {
					tracker.Increment(1)
				}
			}

			if c.sendOn == sendOnEOF || eof {
				errCh <- nil
				return
			}
		}
	}()

	// Wait until the core go routine is complete or context is canceled.
	// Closes the remote connection(due to defer), the core loop go routine is just dropped.
	select {
	case <-ctx.Done():
		if readPipe != nil {
			readPipe.Close()
		}
		return fmt.Errorf("interrupted")
	case <-complete:
		if c.atomic {
			err := c.writeAtomic(nc)
			if err != nil {
				errCh <- err
			}
		}
		return <-errCh
	}
}

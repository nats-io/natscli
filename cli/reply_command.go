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
	iu "github.com/nats-io/natscli/internal/util"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/kballard/go-shellquote"
	"github.com/nats-io/nats.go"
)

type replyCmd struct {
	subject string
	body    string
	queue   string
	command string
	echo    bool
	sleep   time.Duration
	limit   uint
	hdrs    []string
}

func configureReplyCommand(app commandHost) {
	c := &replyCmd{}
	help := `The "command" supports extracting some information from the subject the request came in on.

When the subject being listened on is "weather.>" a request on "weather.london" can extract
the "london" part and use it in the command string:

  nats reply 'weather.>' --command "curl -s wttr.in/{{1}}?format=3"

This will request the weather for london when invoked as:

  nats request weather.london ''

Use {{.Request}} to access the request body within the --command
  
The command gets also spawned with two ENVs:
  NATS_REQUEST_SUBJECT
  NATS_REQUEST_BODY

  nats reply 'echo' --command="printenv NATS_REQUEST_BODY" 
  
The body and Header values of the messages may use Go templates to create unique messages.

   nats reply test "Message {{Count}} @ {{Time}}"

Available template functions are:

   Count            the message number
   TimeStamp        RFC3339 format current time
   Unix             seconds since 1970 in UTC
   UnixNano         nano seconds since 1970 in UTC
   Time             the current time
   ID               an unique ID
   Request          the request payload
   Random(min, max) random string at least min long, at most max
`

	act := app.Command("reply", "Generic service reply utility").Action(c.reply)
	act.HelpLong(help)
	addCheat("reply", act)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Arg("body", "Reply body").StringVar(&c.body)
	act.Flag("echo", "Echo back what is received").UnNegatableBoolVar(&c.echo)
	act.Flag("command", "Runs a command and responds with the output if exit code was 0").StringVar(&c.command)
	act.Flag("queue", "Queue group name").Default("NATS-RPLY-22").Short('q').StringVar(&c.queue)
	act.Flag("sleep", "Inject a random sleep delay between replies up to this duration max").PlaceHolder("MAX").DurationVar(&c.sleep)
	act.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	act.Flag("count", "Quit after receiving this many messages").UintVar(&c.limit)
}

func init() {
	registerCommand("reply", 12, configureReplyCommand)
}

func (c *replyCmd) reply(_ *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}

	if c.body == "" && c.command == "" && !c.echo {
		log.Println("No body or command supplied, enabling echo mode")
		c.echo = true
	}

	ic := make(chan os.Signal, 1)
	defer close(ic)
	i := 0
	sub, _ := nc.QueueSubscribe(c.subject, c.queue, func(m *nats.Msg) {
		log.Printf("[#%d] Received on subject %q:", i, m.Subject)
		for h, vals := range m.Header {
			for _, val := range vals {
				log.Printf("%s: %s", h, val)
			}
		}

		fmt.Println()
		fmt.Println(string(m.Data))

		if c.sleep != 0 {
			time.Sleep(time.Duration(rand.Intn(int(c.sleep))))
		}

		msg := nats.NewMsg(m.Reply)
		if nc.HeadersSupported() && len(c.hdrs) > 0 {
			err = iu.ParseStringsToMsgHeader(c.hdrs, i, msg)
			if err != nil {
				return
			}
		}

		switch {
		case c.echo:
			if nc.HeadersSupported() {
				for h, vals := range m.Header {
					for _, v := range vals {
						msg.Header.Add(h, v)
					}
				}

				msg.Header.Add("NATS-Reply-Counter", strconv.Itoa(i))
			}

			msg.Data = m.Data

		case c.command != "":
			rawCmd := c.command
			tokens := strings.Split(m.Subject, ".")

			for i, t := range tokens {
				rawCmd = strings.Replace(rawCmd, fmt.Sprintf("{{%d}}", i), t, -1)
			}

			parsedCmd, err := iu.PubReplyBodyTemplate(rawCmd, string(m.Data), i)
			if err != nil {
				log.Printf("Could not parse command template: %s", err)
			}
			rawCmd = string(parsedCmd)

			cmdParts, err := shellquote.Split(rawCmd)
			if err != nil {
				log.Printf("Could not parse command: %s", err)
				return
			}

			args := []string{}
			if len(cmdParts) > 1 {
				args = cmdParts[1:]
			}

			if opts().Trace {
				log.Printf("Executing: %s", strings.Join(cmdParts, " "))
			}

			cmd := exec.Command(cmdParts[0], args...)
			cmd.Env = os.Environ()
			cmd.Env = append(cmd.Env, fmt.Sprintf("NATS_REQUEST_SUBJECT=%s", m.Subject))
			cmd.Env = append(cmd.Env, fmt.Sprintf("NATS_REQUEST_BODY=%s", string(m.Data)))
			msg.Data, err = cmd.CombinedOutput()
			if err != nil {
				log.Printf("Command %q failed to run: %s", rawCmd, err)
			}

		default:
			body, err := iu.PubReplyBodyTemplate(c.body, string(m.Data), i)
			if err != nil {
				log.Printf("Could not parse body template: %s", err)
			}

			msg.Data = body
		}

		err = m.RespondMsg(msg)
		if err != nil {
			log.Printf("Could not publish reply: %s", err)
			return
		}

		i++

		if c.limit != 0 && uint(i) == c.limit {
			nc.Flush()
			ic <- os.Interrupt
		}
	})
	if sub != nil && c.limit != 0 {
		sub.AutoUnsubscribe(int(c.limit))
	}
	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	log.Printf("Listening on %q in group %q", c.subject, c.queue)

	signal.Notify(ic, os.Interrupt)
	<-ic

	log.Printf("\nDraining...")
	nc.Drain()
	log.Fatalf("Exiting")

	return nil
}

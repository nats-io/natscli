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

package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/kballard/go-shellquote"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type replyCmd struct {
	subject string
	body    string
	queue   string
	command string
	echo    bool
	sleep   time.Duration
	hdrs    []string
}

func configureReplyCommand(app *kingpin.Application) {
	c := &replyCmd{}
	help := `Generic service reply utility

The "command" supports extracting some information from the subject the request came in on.

When the subject being listened on is "weather.>" a request on "weather.london" can extract
the "london" part and use it in the command string:

  nats reply 'weather.>' --command "curl -s wttr.in/{{1}}?format=3"

This will request the weather for london when invoked as:

  nats request weather.london ''

The body and Header values of the messages may use Go templates to create unique messages.

   nats reply test "Message {{Count}} @ {{Time}}"

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

	act := app.Command("reply", help).Action(c.reply)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Arg("body", "Reply body").StringVar(&c.body)
	act.Flag("echo", "Echo back what is received").BoolVar(&c.echo)
	act.Flag("command", "Runs a command and responds with the output if exit code was 0").StringVar(&c.command)
	act.Flag("queue", "Queue group name").Default("NATS-RPLY-22").Short('q').StringVar(&c.queue)
	act.Flag("sleep", "Inject a random sleep delay between replies up to this duration max").PlaceHolder("MAX").DurationVar(&c.sleep)
	act.Flag("header", "Adds headers to the message").Short('H').StringsVar(&c.hdrs)

	cheats["reply"] = `# To set up a responder that runs an external command with the 3rd subject token as argument
nats reply "service.requests.>" --command "service.sh {{2}}"

# To set up basic responder
nats reply service.requests "Message {{Count}} @ {{Time}}"
nats reply service.requests --echo --sleep 10
`
}

func (c *replyCmd) reply(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}

	if c.body == "" && c.command == "" && !c.echo {
		log.Println("No body or command supplied, enabling echo mode")
		c.echo = true
	}

	i := 0
	nc.QueueSubscribe(c.subject, c.queue, func(m *nats.Msg) {
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
			parseStringsToHeader(c.hdrs, i, msg)
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

			cmdParts, err := shellquote.Split(rawCmd)
			if err != nil {
				log.Printf("Could not parse command: %s", err)
				return
			}

			args := []string{}
			if len(cmdParts) > 1 {
				args = cmdParts[1:]
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
			log.Printf("cnt: %d", i)
			body, err := pubReplyBodyTemplate(c.body, i)
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
	})
	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	log.Printf("Listening on %q in group %q", c.subject, c.queue)

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt)
	<-ic

	log.Printf("\nDraining...")
	nc.Drain()
	log.Fatalf("Exiting")

	return nil
}

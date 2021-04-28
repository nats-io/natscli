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
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	terminal "golang.org/x/term"
	"gopkg.in/alecthomas/kingpin.v2"
)

type pubCmd struct {
	subject string
	body    string
	req     bool
	replyTo string
	raw     bool
	hdrs    []string
	cnt     int
	sleep   time.Duration
}

func configurePubCommand(app *kingpin.Application) {
	c := &pubCmd{}
	help := `%s

Body and Header values of the messages may use Go templates to 
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
	pub := app.Command("pub", fmt.Sprintf(help, "Generic data publish utilty")).Action(c.publish)
	pub.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	pub.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	pub.Flag("wait", "Wait for a reply from a service").Short('w').BoolVar(&c.req)
	pub.Flag("reply", "Sets a custom reply to subject").StringVar(&c.replyTo)
	pub.Flag("header", "Adds headers to the message").Short('H').StringsVar(&c.hdrs)
	pub.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	pub.Flag("sleep", "When publishing multiple messages, sleep between publishes").DurationVar(&c.sleep)

	cheats["pub"] = `# To publish 100 messages with a random body between 100 and 1000 characters
nats pub destination.subject "{{ Random 100 1000 }}" -H Count:{{ Count }} --count 100

# To publish messages from STDIN
echo "hello world" | nats pub destination.subject

# To request a response from a server and show just the raw result
nats request destination.subject "hello world" -H "Content-type:text/plain" --raw
`

	req := app.Command("request", fmt.Sprintf(help, "Generic data request utility")).Alias("req").Action(c.publish)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').Default("false").BoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message").Short('H').StringsVar(&c.hdrs)
	req.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
}

func (c *pubCmd) prepareMsg(body []byte, seq int) (*nats.Msg, error) {
	msg := nats.NewMsg(c.subject)
	msg.Reply = c.replyTo
	msg.Data = body

	return msg, parseStringsToHeader(c.hdrs, seq, msg)
}

func (c *pubCmd) doReq(nc *nats.Conn, progress *uiprogress.Bar) error {
	for i := 1; i <= c.cnt; i++ {
		start := time.Now()
		if !c.raw && progress == nil {
			log.Printf("Sending request on %q\n", c.subject)
		}

		body, err := pubReplyBodyTemplate(c.body, i)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		msg, err := c.prepareMsg(body, i)
		if err != nil {
			return err
		}

		m, err := nc.RequestMsg(msg, timeout)
		if err != nil {
			return err
		}

		if c.raw {
			fmt.Println(string(m.Data))
			continue
		}

		if progress != nil {
			progress.Incr()
			continue
		}

		log.Printf("Received on %q rtt %v", m.Subject, time.Since(start))

		if len(m.Header) > 0 {
			for h, vals := range m.Header {
				for _, val := range vals {
					log.Printf("%s: %s", h, val)
				}
			}

			fmt.Println()
		}

		fmt.Println(string(m.Data))
		if !strings.HasSuffix(string(m.Data), "\n") {
			fmt.Println()
		}
	}

	return nil
}

func (c *pubCmd) publish(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.cnt < 1 {
		c.cnt = math.MaxInt16
	}

	if c.body == "!nil!" && terminal.IsTerminal(int(os.Stdout.Fd())) {
		log.Println("Reading payload from STDIN")
		body, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		c.body = string(body)
	}

	var progress *uiprogress.Bar
	if c.cnt > 20 && !c.raw {
		progress = uiprogress.AddBar(c.cnt).PrependFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("%d / %d", b.Current(), c.cnt)
		}).AppendElapsed()
		progress.Width = progressWidth()

		fmt.Println()
		uiprogress.Start()
		defer func() { uiprogress.Stop(); fmt.Println() }()
	}

	if c.req {
		return c.doReq(nc, progress)
	}

	for i := 1; i <= c.cnt; i++ {
		body, err := pubReplyBodyTemplate(c.body, i)
		if err != nil {
			log.Printf("Could not parse body template: %s", err)
		}

		msg, err := c.prepareMsg(body, i)
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

		if progress == nil {
			log.Printf("Published %d bytes to %q\n", len(body), c.subject)
		} else {
			progress.Incr()
		}
	}

	return nil
}

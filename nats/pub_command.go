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
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/ssh/terminal"
	"gopkg.in/alecthomas/kingpin.v2"
)

type pubCmd struct {
	subject string
	body    string
	req     bool
	replyTo string
	raw     bool
	hdrs    []string
}

func configurePubCommand(app *kingpin.Application) {
	c := &pubCmd{}
	pub := app.Command("pub", "Generic data publishing utility").Action(c.publish)
	pub.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	pub.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	pub.Flag("wait", "Wait for a reply from a service").Short('w').BoolVar(&c.req)
	pub.Flag("reply", "Sets a custom reply to subject").StringVar(&c.replyTo)
	pub.Flag("header", "Adds headers to the message").Short('H').StringsVar(&c.hdrs)

	req := app.Command("request", "Generic data request utility").Alias("req").Action(c.publish)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").Default("!nil!").StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').Default("false").BoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message").Short('H').StringsVar(&c.hdrs)
}

func (c *pubCmd) prepareMsg() (*nats.Msg, error) {
	msg := nats.NewMsg(c.subject)
	msg.Reply = c.replyTo
	msg.Data = []byte(c.body)

	for _, hdr := range c.hdrs {
		parts := strings.SplitN(hdr, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header %q", hdr)
		}
		msg.Header.Add(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	return msg, nil
}

func (c *pubCmd) publish(pc *kingpin.ParseContext) error {
	nc, err := newNatsConn(servers, natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.body == "!nil!" && terminal.IsTerminal(int(os.Stdout.Fd())) {
		log.Println("Reading payload from STDIN")
		body, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		c.body = string(body)
	}

	if c.req {
		if len(c.hdrs) > 0 {
			return fmt.Errorf("requests with Headers are nor supported")
		}

		if !c.raw {
			log.Printf("Sending request on [%s]\n", c.subject)
		}

		m, err := nc.Request(c.subject, []byte(c.body), timeout)
		if err != nil {
			return err
		}

		if c.raw {
			fmt.Println(string(m.Data))

			return nil
		}

		log.Printf("Received on [%s]: %q", m.Subject, m.Data)

		return nil
	}

	msg, err := c.prepareMsg()
	if err != nil {
		return err
	}

	nc.PublishMsg(msg)
	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	log.Printf("Published %d bytes to %s\n", len(c.body), c.subject)

	return nil
}

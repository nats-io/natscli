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

	"golang.org/x/crypto/ssh/terminal"
	"gopkg.in/alecthomas/kingpin.v2"
)

type pubCmd struct {
	subject string
	body    string
	req     bool
}

func configurePubCommand(app *kingpin.Application) {
	c := &pubCmd{}
	act := app.Command("pub", "Generic data publishing utility").Action(c.publish)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Arg("body", "Message body").StringVar(&c.body)
	act.Flag("wait", "Wait for a reply from a service").Short('w').BoolVar(&c.req)

	req := app.Command("request", "Generic data request utility").Alias("req").Action(c.publish)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)

}

func (c *pubCmd) publish(pc *kingpin.ParseContext) error {
	nc, err := newNatsConn(servers, natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.body == "" && terminal.IsTerminal(int(os.Stdout.Fd())) {
		log.Println("Reading payload from STDIN")
		body, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		c.body = string(body)
	}

	if c.body == "" {
		return fmt.Errorf("please specify a body to publish either as command argument or on standard input")
	}

	if c.req {
		log.Printf("Sending request on [%s]\n", c.subject)
		m, err := nc.Request(c.subject, []byte(c.body), timeout)
		if err != nil {
			return err
		}

		log.Printf("Received on [%s]: '%s'", m.Subject, string(m.Data))
		return nil
	} else {
		nc.Publish(c.subject, []byte(c.body))
	}

	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	log.Printf("Published %d bytes to %s\n", len(c.body), c.subject)

	return nil
}

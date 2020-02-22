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
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type replyCmd struct {
	subject string
	body    string
	queue   string
	echo    bool
}

func configureReplyCommand(app *kingpin.Application) {
	c := &replyCmd{}
	act := app.Command("reply", "Generic service reply utility").Action(c.reply)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Arg("body", "Reply body").StringVar(&c.body)
	act.Flag("echo", "Echo back what is received").BoolVar(&c.echo)
	act.Flag("queue", "Queue group name").Default("NATS-RPLY-22").Short('q').StringVar(&c.queue)
}

func (c *replyCmd) reply(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn(servers, natsOpts()...)
	if err != nil {
		return err
	}

	if c.body == "" && !c.echo {
		log.Println("No body supplied, enabling echo mode")
		c.echo = true
	}

	i := 0
	nc.QueueSubscribe(c.subject, c.queue, func(m *nats.Msg) {
		log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, string(m.Data))
		if c.echo {
			m.Respond(m.Data)
		} else {
			m.Respond([]byte(c.body))
		}
	})
	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	log.Printf("Listening on [%s]", c.subject)

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt)
	<-ic

	log.Printf("\nDraining...")
	nc.Drain()
	log.Fatalf("Exiting")

	return nil
}

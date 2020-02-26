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
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type subCmd struct {
	subject string
	queue   string
	raw     bool
}

func configureSubCommand(app *kingpin.Application) {
	c := &subCmd{}
	act := app.Command("sub", "Generic subscription client").Action(c.subscribe)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Flag("queue", "Subscribe to a named queue group").StringVar(&c.queue)
	act.Flag("raw", "Show the raw data received").Short('r').BoolVar(&c.raw)
}

func (c *subCmd) subscribe(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn(servers, natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	i := 0

	handler := func(m *nats.Msg) {
		i += 1
		if c.raw {
			fmt.Println(string(m.Data))
			return
		}

		log.Printf("[#%d] Received on [%s]: '%s'", i, m.Subject, string(m.Data))
	}

	if c.queue != "" {
		nc.QueueSubscribe(c.subject, c.queue, handler)
	} else {
		nc.Subscribe(c.subject, handler)
	}
	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	if !c.raw {
		log.Printf("Listening on [%s]", c.subject)
	}

	<-context.Background().Done()

	return nil
}

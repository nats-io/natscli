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
	"strings"
	"sync"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type subCmd struct {
	subject string
	queue   string
	raw     bool
	jsAck   bool
}

func configureSubCommand(app *kingpin.Application) {
	c := &subCmd{}
	act := app.Command("sub", "Generic subscription client").Action(c.subscribe)
	act.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	act.Flag("queue", "Subscribe to a named queue group").StringVar(&c.queue)
	act.Flag("raw", "Show the raw data received").Short('r').BoolVar(&c.raw)
	act.Flag("ack", "Acknowledge JetStream message that have the correct metadata").BoolVar(&c.jsAck)
}

func (c *subCmd) subscribe(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	i := 0
	mu := sync.Mutex{}

	handler := func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		i += 1
		if c.raw {
			fmt.Println(string(m.Data))
			return
		}

		var info *jsm.MsgInfo
		if m.Reply != "" {
			info, _ = jsm.ParseJSMsgMetadata(m)
		}

		if info == nil {
			log.Printf("[#%d] Received on %q", i, m.Subject)
		} else {
			log.Printf("[#%d] Received JetStream message: consumer: %s > %s / subject: %s / delivered: %d / consumer seq: %d / stream seq: %d / ack: %v", i, info.Stream(), info.Consumer(), m.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence(), c.jsAck)
		}

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

		if c.jsAck && info != nil {
			err = m.Respond(nil)
			if err != nil {
				fmt.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
			}
		}
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
		if c.jsAck {
			log.Printf("Subscribing on %s with acknowledgement of JetStream messages\n", c.subject)
		} else {
			log.Printf("Subscribing on %s\n", c.subject)
		}

	}

	<-context.Background().Done()

	return nil
}

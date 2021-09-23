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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	inbox   bool
	dump    string
	limit   uint
}

func configureSubCommand(app *kingpin.Application) {
	c := &subCmd{}
	act := app.Command("sub", "Generic subscription client").Action(c.subscribe)
	act.Arg("subject", "Subject to subscribe to").StringVar(&c.subject)
	act.Flag("queue", "Subscribe to a named queue group").StringVar(&c.queue)
	act.Flag("raw", "Show the raw data received").Short('r').BoolVar(&c.raw)
	act.Flag("ack", "Acknowledge JetStream message that have the correct metadata").BoolVar(&c.jsAck)
	act.Flag("inbox", "Subscribes to a generate inbox").Short('i').BoolVar(&c.inbox)
	act.Flag("count", "Quit after receiving this many messages").UintVar(&c.limit)
	act.Flag("dump", "Dump received messages to files, 1 file per message. Specify - for null terminated STDOUT for use with xargs -0").PlaceHolder("DIRECTORY").StringVar(&c.dump)

	cheats["sub"] = `# To subscribe to messages, in a queue group and acknowledge any JetStream ones
nats sub source.subject --queue work --ack

# To subscribe to a randomly generated inbox
nats sub --inbox

# To dump all messages to files, 1 file per message
nats sub --inbox --dump /tmp/archive

# To process all messages using xargs 1 message at a time through a shell command
nats sub subject --dump=- | xargs -0 -n 1 -I "{}" sh -c "echo '{}' | wc -c"
`
}

func (c *subCmd) subscribe(_ *kingpin.ParseContext) error {
	if c.subject == "" && c.inbox {
		c.subject = nats.NewInbox()
	} else if c.subject == "" {
		return fmt.Errorf("subject is required")
	}

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	var (
		sub         *nats.Subscription
		mu          = sync.Mutex{}
		dump        = c.dump != ""
		ctr         = uint(0)
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	if c.dump == "-" && c.inbox {
		return fmt.Errorf("generating inboxes is not compatible with dumping to stdout using null terminated strings")
	}

	handler := func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		var info *jsm.MsgInfo
		if m.Reply != "" {
			info, _ = jsm.ParseJSMsgMetadata(m)
		}

		if c.jsAck && info != nil {
			defer func() {
				err = m.Respond(nil)
				if err != nil && !dump && !c.raw {
					log.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
				}
			}()
		}

		// flow control
		if len(m.Data) == 0 && m.Header.Get("Status") == "100" {
			if m.Reply != "" {
				m.Respond(nil)
				log.Printf("Responding to Flow Control message")
			} else if stalled := m.Header.Get("Nats-Consumer-Stalled"); stalled != "" {
				nc.Publish(stalled, nil)
				log.Printf("Resuming stalled consumer")
			}
		}

		ctr++
		if dump {
			stdout := c.dump == "-"
			outFile := ""
			if !stdout {
				if info == nil {
					outFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", ctr))
				} else {
					outFile = filepath.Join(c.dump, fmt.Sprintf("%d.json", info.StreamSequence()))
				}
			}

			// dont want sub etc
			msg := nats.NewMsg(m.Subject)
			msg.Header = m.Header
			msg.Data = m.Data
			msg.Reply = m.Reply

			jm, err := json.Marshal(msg)
			if err != nil {
				log.Printf("Could not JSON encode message: %s", err)
				return
			}

			if stdout {
				os.Stdout.WriteString(fmt.Sprintf("%s\000", jm))
			} else {
				err = os.WriteFile(outFile, jm, 0600)
				if err != nil {
					log.Printf("Could not save message: %s", err)
				}

				if ctr%100 == 0 {
					fmt.Print(".")
				}
			}

			return
		} else if c.raw {
			fmt.Println(string(m.Data))
			return
		}

		if info == nil {
			if m.Reply != "" {
				fmt.Printf("[#%d] Received on %q with reply %q\n", ctr, m.Subject, m.Reply)
			} else {
				fmt.Printf("[#%d] Received on %q\n", ctr, m.Subject)
			}

		} else {
			fmt.Printf("[#%d] Received JetStream message: consumer: %s > %s / subject: %s / delivered: %d / consumer seq: %d / stream seq: %d / ack: %v\n", ctr, info.Stream(), info.Consumer(), m.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence(), c.jsAck)
		}

		if len(m.Header) > 0 {
			for h, vals := range m.Header {
				for _, val := range vals {
					fmt.Printf("%s: %s\n", h, val)
				}
			}

			fmt.Println()
		}

		fmt.Println(string(m.Data))
		if !strings.HasSuffix(string(m.Data), "\n") {
			fmt.Println()
		}

		if ctr == c.limit {
			sub.Unsubscribe()
			cancel()
		}
	}

	if (!c.raw && c.dump == "") || c.inbox {
		if c.jsAck {
			log.Printf("Subscribing on %s with acknowledgement of JetStream messages\n", c.subject)
		} else {
			log.Printf("Subscribing on %s\n", c.subject)
		}
	}

	if c.queue != "" {
		sub, err = nc.QueueSubscribe(c.subject, c.queue, handler)
	} else {
		sub, err = nc.Subscribe(c.subject, handler)
	}
	if err != nil {
		return err
	}

	nc.Flush()

	err = nc.LastError()
	if err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

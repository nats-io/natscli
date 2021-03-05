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
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvCheckCmd struct {
	connectWarning  time.Duration
	connectCritical time.Duration
	rttWarning      time.Duration
	rttCritical     time.Duration
	reqWarning      time.Duration
	reqCritical     time.Duration
}

func configureServerCheckCommand(srv *kingpin.CmdClause) {
	c := &SrvCheckCmd{}

	help := `Nagios protocol health check for NATS servers

Once connected a full round trip request-reply test is done
against the server using subjects in .INBOX.>
`
	check := srv.Command("check", help).Action(c.check)
	check.Flag("connect-warn", "Warning threshold to allow for establishing connections").Default("500ms").DurationVar(&c.connectWarning)
	check.Flag("connect-critical", "Critical threshold to allow for establishing connections").Default("1s").DurationVar(&c.connectCritical)
	check.Flag("rtt-warn", "Warning threshold to allow for server RTT").Default("500ms").DurationVar(&c.rttWarning)
	check.Flag("rtt-critical", "Critical threshold to allow for server RTT").Default("1s").DurationVar(&c.rttCritical)
	check.Flag("req-warn", "Warning threshold to allow for full round trip test").Default("500ms").DurationVar(&c.reqWarning)
	check.Flag("req-critical", "Critical threshold to allow for full round trip test").Default("1s").DurationVar(&c.reqCritical)
}

func (c *SrvCheckCmd) check(_ *kingpin.ParseContext) error {
	connStart := time.Now()
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		fmt.Printf("CRITICAL: connection failed: %s\n", err)
		os.Exit(2)
	}

	ct := time.Since(connStart)
	pd := fmt.Sprintf("connect_time=%fs;%.2f;%.2f ", ct.Seconds(), c.connectWarning.Seconds(), c.connectCritical.Seconds())

	if ct >= c.connectCritical {
		fmt.Printf("CRITICAL: connected to %s, connect time exceeded %v | %s\n", nc.ConnectedUrl(), c.connectCritical, pd)
		os.Exit(2)
	} else if ct >= c.connectWarning {
		fmt.Printf("WARNING: connected to %s, connect time exceeded %v | %s\n", nc.ConnectedUrl(), c.connectWarning, pd)
		os.Exit(1)
	}

	rtt, err := nc.RTT()
	if err != nil {
		fmt.Printf("CRITICAL: connected to %s, rtt failed | %s\n", nc.ConnectedUrl(), pd)
		os.Exit(2)
	}

	pd += fmt.Sprintf("rtt=%fs;%.2f;%.2f ", rtt.Seconds(), c.rttWarning.Seconds(), c.rttCritical.Seconds())
	if rtt >= c.rttCritical {
		fmt.Printf("CRITICAL: connect time exceeded %v | %s\n", c.connectCritical, pd)
		os.Exit(2)
	} else if rtt >= c.rttWarning {
		fmt.Printf("WARNING: rtt time exceeded %v | %s\n", c.connectCritical, pd)
		os.Exit(1)
	}

	msg := []byte(randomPassword(100))
	ib := nats.NewInbox()
	sub, err := nc.SubscribeSync(ib)
	if err != nil {
		fmt.Printf("CRITICAL: could not subscribe to %s: %s | %s\n", ib, err, pd)
		os.Exit(2)
	}
	sub.AutoUnsubscribe(1)

	start := time.Now()
	err = nc.Publish(ib, msg)
	if err != nil {
		fmt.Printf("CRITICAL: could not publish to %s: %s | %s\n", ib, err, pd)
		os.Exit(2)
	}

	received, err := sub.NextMsg(timeout)
	if err != nil {
		fmt.Printf("CRITICAL: did not receive from %s: %s | %s\n", ib, err, pd)
		os.Exit(2)
	}

	reqt := time.Since(start)
	pd += fmt.Sprintf("request_time=%fs;%.2f;%.2f", reqt.Seconds(), c.reqWarning.Seconds(), c.reqCritical.Seconds())

	if !bytes.Equal(received.Data, msg) {
		fmt.Printf("CRITICAL: did not receive expected message on %s | %s\n", nc.ConnectedUrl(), pd)
	}

	if reqt >= c.reqCritical {
		fmt.Printf("CRITICAL: round trip request took %f | %s", reqt.Seconds(), pd)
		os.Exit(2)
	} else if reqt >= c.reqWarning {
		fmt.Printf("WARNING: round trip request took %f | %s", reqt.Seconds(), pd)
		os.Exit(1)
	}

	fmt.Printf("OK: connected to %s | %s\n", nc.ConnectedUrl(), pd)

	return nil
}

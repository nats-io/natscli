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
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guptarohit/asciigraph"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvPingCmd struct {
	expect uint32
	graph  bool
	showId bool
}

func configureServerPingCommand(srv *kingpin.CmdClause) {
	c := &SrvPingCmd{}

	ls := srv.Command("ping", "Ping all servers").Action(c.ping)
	ls.Arg("expect", "How many servers to expect").Uint32Var(&c.expect)
	ls.Flag("graph", "Produce a response distribution graph").BoolVar(&c.graph)
	ls.Flag("id", "Include the Server ID in the output").BoolVar(&c.showId)
}

func (c *SrvPingCmd) ping(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	seen := uint32(0)
	mu := &sync.Mutex{}
	start := time.Now()
	times := []float64{}

	sub, err := nc.Subscribe(nc.NewRespInbox(), func(msg *nats.Msg) {
		if msg.Header != nil && msg.Header.Get("Status") != "" {
			fmt.Printf("%s status from $SYS.REQ.SERVER.PING, ensure a system account is used with appropriate permissions\n", msg.Header.Get("Status"))
			os.Exit(1)
		}

		ssm := &server.ServerStatsMsg{}
		err = json.Unmarshal(msg.Data, ssm)
		if err != nil {
			log.Printf("Could not decode response: %s", err)
			os.Exit(1)
		}

		mu.Lock()
		defer mu.Unlock()

		last := atomic.AddUint32(&seen, 1)

		if c.expect == 0 && ssm.Stats.ActiveServers > 0 && last == 1 {
			c.expect = uint32(ssm.Stats.ActiveServers)
		}

		since := time.Since(start)
		rtt := since.Milliseconds()
		times = append(times, float64(rtt))

		if c.showId {
			fmt.Printf("%s %-60s rtt=%s\n", ssm.Server.ID, ssm.Server.Name, since)
		} else {
			fmt.Printf("%-60s rtt=%s\n", ssm.Server.Name, since)
		}

		if last == c.expect {
			cancel()
		}
	})
	if err != nil {
		return err
	}

	err = nc.PublishRequest("$SYS.REQ.SERVER.PING", sub.Subject, nil)
	if err != nil {
		return err
	}

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt)

	select {
	case <-ic:
		cancel()
	case <-ctx.Done():
	}

	sub.Drain()

	c.summarize(times)

	if c.expect != 0 && c.expect != seen {
		fmt.Printf("\nMissing %d server(s)\n", c.expect-atomic.LoadUint32(&seen))
	}

	return nil
}

func (c *SrvPingCmd) summarize(times []float64) {
	fmt.Println()
	fmt.Println("---- ping statistics ----")

	if len(times) > 0 {
		sum := 0.0
		min := 999999.0
		max := -1.0
		avg := 0.0

		for _, value := range times {
			sum += value
			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
		}

		avg = sum / float64(len(times))

		fmt.Printf("%d replies max: %.2f min: %.2f avg: %.2f\n", len(times), max, min, avg)

		if c.graph {
			fmt.Println()
			fmt.Println(c.chart(times))
		}
		return
	}

	fmt.Println("no responses received")
}

func (c *SrvPingCmd) chart(times []float64) string {
	sort.Float64s(times)

	latest := times[len(times)-1]
	bcount := int(latest/25) + 1
	buckets := make([]float64, bcount)

	for _, t := range times {
		b := t / 25.0
		buckets[int(b)]++
	}

	return asciigraph.Plot(
		buckets,
		asciigraph.Height(15),
		asciigraph.Width(60),
		asciigraph.Offset(5),
		asciigraph.Caption("Responses per 25ms"),
	)
}

// Copyright 2024 The NATS Authors
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

package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	terminal "golang.org/x/term"
)

type SrvWatchServerCmd struct {
	topCount  int
	sort      string
	servers   map[string]*server.ServerStatsMsg
	sortNames map[string]string
	mu        sync.Mutex
}

func configureServerWatchServerCommand(watch *fisk.CmdClause) {
	c := &SrvWatchServerCmd{
		servers: map[string]*server.ServerStatsMsg{},
		sortNames: map[string]string{
			"conns": "Connections",
			"subs":  "Subscriptions",
			"sentb": "Sent Bytes",
			"sentm": "Sent Messages",
			"recvb": "Received Bytes",
			"recvm": "Received Messages",
			"slow":  "Slow Consumers",
			"route": "Routes",
			"gway":  "Gateways",
			"mem":   "Memory",
			"cpu":   "CPU",
		},
	}

	sortKeys := mapKeys(c.sortNames)
	sort.Strings(sortKeys)

	servers := watch.Command("servers", "Watch server statistics").Alias("server").Alias("srv").Action(c.serversAction)
	servers.HelpLong(`This waits for regular updates that each server sends and report seen totals

Since the updates are sent on a 30 second interval this is not a point in time view.
`)
	servers.Flag("sort", fmt.Sprintf("Sorts by a specific property (%s)", strings.Join(sortKeys, ", "))).Default("conns").EnumVar(&c.sort, sortKeys...)
	servers.Flag("number", "Amount of Accounts to show by the selected dimension").Default("10").Short('n').IntVar(&c.topCount)
}

func (c *SrvWatchServerCmd) serversAction(_ *fisk.ParseContext) error {
	_, h, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil && c.topCount == 0 {
		return fmt.Errorf("could not determine screen dimensions: %v", err)
	}

	if c.topCount == 0 {
		c.topCount = h - 6
	}

	if c.topCount < 1 {
		return fmt.Errorf("requested render limits exceed screen size")
	}

	if c.topCount > h-6 {
		c.topCount = h - 6
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	_, err = nc.Subscribe("$SYS.SERVER.*.STATSZ", c.handle)

	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second)
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	for {
		select {
		case <-tick.C:
			c.redraw()
		case sig := <-ctx.Done():
			fmt.Printf("Exiting on %v\n", sig)
			return nil
		}
	}
}

func (c *SrvWatchServerCmd) handle(msg *nats.Msg) {
	var stat server.ServerStatsMsg
	err := json.Unmarshal(msg.Data, &stat)
	if err != nil {
		return
	}

	c.mu.Lock()
	c.servers[stat.Server.ID] = &stat
	c.mu.Unlock()
}

func (c *SrvWatchServerCmd) redraw() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var servers []*server.ServerStatsMsg

	for srv := range c.servers {
		servers = append(servers, c.servers[srv])
	}

	sort.Slice(servers, func(i, j int) bool {
		si := servers[i].Stats
		sj := servers[j].Stats

		switch c.sort {
		case "subs":
			return si.NumSubs > sj.NumSubs
		case "sentb":
			return si.Sent.Bytes > sj.Sent.Bytes
		case "sentm":
			return si.Sent.Msgs > sj.Sent.Msgs
		case "recvb":
			return si.Received.Bytes > sj.Received.Bytes
		case "recvm":
			return si.Received.Msgs > sj.Received.Msgs
		case "slow":
			return si.SlowConsumers > sj.SlowConsumers
		case "route":
			return len(si.Routes) > len(sj.Routes)
		case "gway":
			return len(si.Gateways) > len(sj.Gateways)
		case "mem":
			return si.Mem > sj.Mem
		case "cpu":
			return si.CPU > sj.CPU
		default:
			return si.Connections > sj.Connections
		}
	})

	table := newTableWriter(fmt.Sprintf("Top %d Server activity by %s", c.topCount, c.sortNames[c.sort]))
	table.AddHeaders("Server", "Connections", "Subscription", "Slow", "Memory", "CPU", "Routes", "Gateways", "Sent", "Received")

	var matched []*server.ServerStatsMsg
	if len(servers) < c.topCount {
		matched = servers
	} else {
		matched = servers[:c.topCount]
	}

	for _, srv := range matched {
		st := srv.Stats
		table.AddRow(
			srv.Server.Name,
			f(st.Connections),
			f(st.NumSubs),
			f(st.SlowConsumers),
			fiBytes(uint64(st.Mem)),
			f(st.CPU),
			f(len(st.Routes)),
			f(len(st.Gateways)),
			fmt.Sprintf("%s / %s", f(st.Sent.Msgs), fiBytes(uint64(st.Sent.Bytes))),
			fmt.Sprintf("%s / %s", f(st.Received.Msgs), fiBytes(uint64(st.Received.Bytes))),
		)
	}

	clearScreen()
	fmt.Println(table.Render())
}

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
	top       int
	sort      string
	servers   map[string]*server.ServerStatsMsg
	sortNames map[string]string
	lastMsg   time.Time
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
	servers.Flag("number", "Amount of Accounts to show by the selected dimension").Default("0").Short('n').IntVar(&c.top)
}

func (c *SrvWatchServerCmd) updateSizes() error {
	c.topCount = c.top

	_, h, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil && c.topCount == 0 {
		return fmt.Errorf("could not determine screen dimensions: %v", err)
	}

	maxRows := h - 9

	if c.topCount == 0 {
		c.topCount = maxRows
	}

	if c.topCount > maxRows {
		c.topCount = maxRows
	}

	if c.topCount < 1 {
		return fmt.Errorf("requested render limits exceed screen size")
	}

	return nil
}
func (c *SrvWatchServerCmd) serversAction(_ *fisk.ParseContext) error {
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
			err = c.redraw()
			if err != nil {
				return err
			}
		case <-ctx.Done():
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
	c.lastMsg = time.Now()
	c.mu.Unlock()
}

func (c *SrvWatchServerCmd) redraw() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.updateSizes()
	if err != nil {
		return err
	}

	var servers []*server.ServerStatsMsg

	var (
		conns int
		subs  uint32
		slow  int64
		mem   int64
		sentB int64
		sentM int64
		recvB int64
		recvM int64
	)

	for _, srv := range c.servers {
		servers = append(servers, srv)
		conns += srv.Stats.Connections
		subs += srv.Stats.NumSubs
		slow += srv.Stats.SlowConsumers
		mem += srv.Stats.Mem
		sentB += srv.Stats.Sent.Bytes
		sentM += srv.Stats.Sent.Msgs
		recvB += srv.Stats.Received.Bytes
		recvM += srv.Stats.Received.Msgs
	}

	sort.Slice(servers, func(i, j int) bool {
		si := servers[i].Stats
		sj := servers[j].Stats
		iName := servers[i].Server.Name
		jName := servers[j].Server.Name

		switch c.sort {
		case "subs":
			return sortMultiSort(si.NumSubs, sj.NumSubs, iName, jName)
		case "sentb":
			return sortMultiSort(si.Sent.Bytes, sj.Sent.Bytes, iName, jName)
		case "sentm":
			return sortMultiSort(si.Sent.Msgs, sj.Sent.Msgs, iName, jName)
		case "recvb":
			return sortMultiSort(si.Received.Bytes, sj.Received.Bytes, iName, jName)
		case "recvm":
			return sortMultiSort(si.Received.Msgs, sj.Received.Msgs, iName, jName)
		case "slow":
			return sortMultiSort(si.SlowConsumers, sj.SlowConsumers, iName, jName)
		case "route":
			return sortMultiSort(len(si.Routes), len(sj.Routes), iName, jName)
		case "gway":
			return sortMultiSort(len(si.Gateways), len(sj.Gateways), iName, jName)
		case "mem":
			return sortMultiSort(si.Mem, sj.Mem, iName, jName)
		case "cpu":
			return sortMultiSort(si.CPU, sj.CPU, iName, jName)
		default:
			return sortMultiSort(si.Connections, sj.Connections, iName, jName)
		}
	})

	tc := fmt.Sprintf("%d", len(servers))
	if len(servers) > c.topCount {
		tc = fmt.Sprintf("%d / %d", c.topCount, len(servers))
	}

	table := newTableWriter(fmt.Sprintf("Top %s Server activity by %s at %s", tc, c.sortNames[c.sort], c.lastMsg.Format(time.DateTime)))
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

	table.AddFooter("Totals (All Servers)", f(conns), f(subs), f(slow), fiBytes(uint64(mem)), "", "", "", fmt.Sprintf("%s / %s", f(sentM), fiBytes(uint64(sentB))), fmt.Sprintf("%s / %s", f(recvM), fiBytes(uint64(recvB))))

	clearScreen()
	fmt.Print(table.Render())
	return nil
}

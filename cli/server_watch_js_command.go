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

type SrvWatchJSCmd struct {
	topCount  int
	sort      string
	servers   map[string]*server.ServerStatsMsg
	sortNames map[string]string
	mu        sync.Mutex
}

func configureServerWatchJSCommand(watch *fisk.CmdClause) {
	c := &SrvWatchJSCmd{
		servers: map[string]*server.ServerStatsMsg{},
		sortNames: map[string]string{
			"mem":    "Memory Used",
			"file":   "File Storage",
			"assets": "HA Asset",
			"api":    "API Requests",
			"err":    "API Errors",
		},
	}

	sortKeys := mapKeys(c.sortNames)
	sort.Strings(sortKeys)

	js := watch.Command("jetstream", "Watch JetStream statistics").Alias("js").Alias("jsz").Action(c.jetstreamAction)
	js.HelpLong(`This waits for regular updates that each server sends and report seen totals

Since the updates are sent on a 30 second interval this is not a point in time view.
`)
	js.Flag("sort", fmt.Sprintf("Sorts by a specific property (%s)", strings.Join(sortKeys, ", "))).Default("assets").EnumVar(&c.sort, sortKeys...)
	js.Flag("number", "Amount of Accounts to show by the selected dimension").Default("10").Short('n').IntVar(&c.topCount)
}

func (c *SrvWatchJSCmd) jetstreamAction(_ *fisk.ParseContext) error {
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
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *SrvWatchJSCmd) handle(msg *nats.Msg) {
	var stat server.ServerStatsMsg
	err := json.Unmarshal(msg.Data, &stat)
	if err != nil {
		return
	}

	if stat.Stats.JetStream == nil {
		return
	}

	c.mu.Lock()
	c.servers[stat.Server.ID] = &stat
	c.mu.Unlock()
}

func (c *SrvWatchJSCmd) redraw() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var servers []*server.ServerStatsMsg

	for srv := range c.servers {
		servers = append(servers, c.servers[srv])
	}

	sort.Slice(servers, func(i, j int) bool {
		si := servers[i].Stats.JetStream.Stats
		sj := servers[j].Stats.JetStream.Stats

		switch c.sort {
		case "mem":
			return sortMultiSort(si.Memory, sj.Memory, servers[i].Server.Name, servers[j].Server.Name)
		case "file":
			return sortMultiSort(si.Store, sj.Store, servers[i].Server.Name, servers[j].Server.Name)
		case "api":
			return sortMultiSort(si.API.Total, sj.API.Total, servers[i].Server.Name, servers[j].Server.Name)
		case "err":
			return sortMultiSort(si.API.Errors, sj.API.Errors, servers[i].Server.Name, servers[j].Server.Name)
		default:
			return sortMultiSort(si.HAAssets, sj.HAAssets, servers[i].Server.Name, servers[j].Server.Name)
		}
	})

	table := newTableWriter(fmt.Sprintf("Top %d Server activity by %s at %s", c.topCount, c.sortNames[c.sort], time.Now().Format(time.DateTime)))
	table.AddHeaders("Server", "HA Assets", "Memory", "File", "API", "API Errors")

	var matched []*server.ServerStatsMsg
	if len(servers) < c.topCount {
		matched = servers
	} else {
		matched = servers[:c.topCount]
	}

	for _, srv := range matched {
		js := srv.Stats.JetStream.Stats
		table.AddRow(
			srv.Server.Name,
			f(js.HAAssets),
			fiBytes(js.Memory),
			fiBytes(js.Store),
			f(js.API.Total),
			f(js.API.Errors),
		)
	}

	clearScreen()
	fmt.Println(table.Render())
}

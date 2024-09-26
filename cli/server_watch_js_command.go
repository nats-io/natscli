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
	iu "github.com/nats-io/natscli/internal/util"
	terminal "golang.org/x/term"
)

type SrvWatchJSCmd struct {
	top       int
	topCount  int
	sort      string
	servers   map[string]*server.ServerStatsMsg
	sortNames map[string]string
	lastMsg   time.Time
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

	sortKeys := iu.MapKeys(c.sortNames)
	sort.Strings(sortKeys)

	js := watch.Command("jetstream", "Watch JetStream statistics").Alias("js").Alias("jsz").Action(c.jetstreamAction)
	js.HelpLong(`This waits for regular updates that each server sends and report seen totals

Since the updates are sent on a 30 second interval this is not a point in time view.
`)
	js.Flag("sort", fmt.Sprintf("Sorts by a specific property (%s)", strings.Join(sortKeys, ", "))).Default("assets").EnumVar(&c.sort, sortKeys...)
	js.Flag("number", "Amount of Accounts to show by the selected dimension").Default("0").Short('n').IntVar(&c.top)
}

func (c *SrvWatchJSCmd) updateSizes() error {
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

func (c *SrvWatchJSCmd) prePing(nc *nats.Conn, h nats.MsgHandler) {
	sub, err := nc.Subscribe(nc.NewRespInbox(), h)
	if err != nil {
		return
	}

	time.AfterFunc(2*time.Second, func() { sub.Unsubscribe() })

	msg := nats.NewMsg("$SYS.REQ.SERVER.PING")
	msg.Reply = sub.Subject
	nc.PublishMsg(msg)
}

func (c *SrvWatchJSCmd) jetstreamAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	c.prePing(nc, c.handle)

	_, err = nc.Subscribe("$SYS.SERVER.*.STATSZ", c.handle)
	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second)
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// TODO: remove after 2.12 is out
	drawPending := iu.ServerMinVersion(nc, 2, 10, 21)

	for {
		select {
		case <-tick.C:
			err = c.redraw(drawPending)
			if err != nil {
				return err
			}
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
	c.lastMsg = time.Now()
	c.mu.Unlock()
}

func (c *SrvWatchJSCmd) redraw(drawPending bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.updateSizes()
	if err != nil {
		return err
	}

	var (
		servers    []*server.ServerStatsMsg
		assets     int
		mem        uint64
		store      uint64
		api        uint64
		apiError   uint64
		apiPending int
	)

	for _, srv := range c.servers {
		if srv.Stats.JetStream == nil {
			continue
		}

		servers = append(servers, srv)

		assets += srv.Stats.JetStream.Stats.HAAssets
		mem += srv.Stats.JetStream.Stats.Memory
		store += srv.Stats.JetStream.Stats.Store
		api += srv.Stats.JetStream.Stats.API.Total
		apiError += srv.Stats.JetStream.Stats.API.Errors
		if srv.Stats.JetStream.Meta != nil {
			apiPending += srv.Stats.JetStream.Meta.Pending
		}
	}

	sort.Slice(servers, func(i, j int) bool {
		si := servers[i].Stats.JetStream.Stats
		sj := servers[j].Stats.JetStream.Stats

		switch c.sort {
		case "mem":
			return iu.SortMultiSort(si.Memory, sj.Memory, servers[i].Server.Name, servers[j].Server.Name)
		case "file":
			return iu.SortMultiSort(si.Store, sj.Store, servers[i].Server.Name, servers[j].Server.Name)
		case "api":
			return iu.SortMultiSort(si.API.Total, sj.API.Total, servers[i].Server.Name, servers[j].Server.Name)
		case "err":
			return iu.SortMultiSort(si.API.Errors, sj.API.Errors, servers[i].Server.Name, servers[j].Server.Name)
		default:
			return iu.SortMultiSort(si.HAAssets, sj.HAAssets, servers[i].Server.Name, servers[j].Server.Name)
		}
	})

	tc := fmt.Sprintf("%d", len(servers))
	if len(servers) > c.topCount {
		tc = fmt.Sprintf("%d / %d", c.topCount, len(servers))
	}

	table := newTableWriter(fmt.Sprintf("Top %s Server activity by %s at %s", tc, c.sortNames[c.sort], c.lastMsg.Format(time.DateTime)))

	if drawPending {
		table.AddHeaders("Server", "HA Assets", "Memory", "File", "API", "API Errors", "API Pending")
	} else {
		table.AddHeaders("Server", "HA Assets", "Memory", "File", "API", "API Errors")
	}

	var matched []*server.ServerStatsMsg
	if len(servers) < c.topCount {
		matched = servers
	} else {
		matched = servers[:c.topCount]
	}

	for _, srv := range matched {
		js := srv.Stats.JetStream.Stats
		pending := 0
		if srv.Stats.JetStream.Meta != nil {
			pending = srv.Stats.JetStream.Meta.Pending
		}

		row := []any{srv.Server.Name,
			f(js.HAAssets),
			fiBytes(js.Memory),
			fiBytes(js.Store),
			f(js.API.Total),
			f(js.API.Errors),
		}
		if drawPending {
			row = append(row, f(pending))
		}

		table.AddRow(row...)
	}
	row := []any{fmt.Sprintf("Totals (%d Servers)", len(matched)), f(assets), fiBytes(mem), fiBytes(store), f(api), f(apiError)}
	if drawPending {
		row = append(row, f(apiPending))
	}
	table.AddFooter(row...)

	iu.ClearScreen()
	fmt.Print(table.Render())

	return nil
}

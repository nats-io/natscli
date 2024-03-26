// Copyright 2023 The NATS Authors
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
	"fmt"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/mprimi/natscli/top"
	ui "gopkg.in/gizak/termui.v1"
)

type topCmd struct {
	host            string
	conns           int
	delay           int
	sort            string
	lookup          bool
	output          string
	outputDelimiter string
	raw             bool
	maxRefresh      int
	showSubs        bool
}

func configureTopCommand(app commandHost) {
	c := &topCmd{}

	top := app.Command("top", "Shows top-like statistic for connections on a specific server").Action(c.topAction)
	top.Arg("name", "The server name to gather statistics for").Required().StringVar(&c.host)
	top.Flag("conns", "Maximum number of connections to show").Default("1024").Short('n').IntVar(&c.conns)
	top.Flag("interval", "Refresh interval").Default("1").Short('d').IntVar(&c.delay)
	top.Flag("sort", "Sort connections by").Default("cid").EnumVar(&c.sort, "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason", "rtt")
	top.Flag("lookup", "Looks up client addresses in DNS").Default("false").UnNegatableBoolVar(&c.lookup)
	top.Flag("output", "Saves the first snapshot to a file").Short('o').StringVar(&c.output)
	top.Flag("delimiter", "Specifies a output delimiter, defaults to grid-like text").StringVar(&c.outputDelimiter)
	top.Flag("raw", "Show raw bytes").Short('b').Default("false").UnNegatableBoolVar(&c.raw)
	top.Flag("max-refresh", "Maximum refreshes").Short('r').Default("-1").IntVar(&c.maxRefresh)
	top.Flag("subs", "Shows the subscriptions column").Default("false").UnNegatableBoolVar(&c.showSubs)
}

func init() {
	registerCommand("top", 17, configureTopCommand)
}

func (c *topCmd) topAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	engine := top.NewEngine(nc, c.host, c.conns, c.delay, opts.Trace)

	_, err = engine.Request("VARZ")
	if err != nil {
		return fmt.Errorf("initial test request failed: %v", err)
	}

	sortOpt := server.SortOpt(c.sort)
	if !sortOpt.IsValid() {
		return fmt.Errorf("invalid sort option: %s", c.sort)
	}
	engine.SortOpt = sortOpt
	engine.DisplaySubs = c.showSubs

	if c.output != "" {
		return top.SaveStatsSnapshotToFile(engine, c.output, c.outputDelimiter)
	}

	err = ui.Init()
	if err != nil {
		panic(err)
	}
	defer ui.Close()

	go engine.MonitorStats()

	top.StartUI(engine, c.lookup, c.raw, c.maxRefresh)

	return nil
}

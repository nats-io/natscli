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
	"os"
	"os/signal"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/xlab/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvLsCmd struct {
	expect uint32
	json   bool
	filter string
}

func configureServerListCommand(srv *kingpin.CmdClause) {
	c := &SrvLsCmd{}

	ls := srv.Command("list", "List known servers").Alias("ls").Action(c.list)
	ls.Arg("expect", "How many servers to expect").Uint32Var(&c.expect)
	ls.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	ls.Flag("filter", "Regular expression filter on server name").Short('f').StringVar(&c.filter)
}

func (c *SrvLsCmd) list(_ *kingpin.ParseContext) error {
	if creds == "" {
		return fmt.Errorf("listing servers requires credentials supplied with --creds")
	}

	nc, err := newNatsConn(servers, natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	seen := uint32(0)
	var results []*server.ServerStatsMsg
	mu := &sync.Mutex{}
	filter, err := regexp.Compile(c.filter)
	if err != nil {
		return err
	}

	table := tablewriter.CreateTable()
	table.AddHeaders("Name", "Cluster", "IP", "Version", "Conns", "Routes", "GWs", "Mem", "CPU", "Slow", "Uptime")

	sub, err := ec.Subscribe(nc.NewRespInbox(), func(ssm *server.ServerStatsMsg) {
		last := atomic.AddUint32(&seen, 1)

		if !filter.MatchString(ssm.Server.Name) {
			return
		}

		mu.Lock()
		results = append(results, ssm)
		mu.Unlock()

		table.AddRow(ssm.Server.Name, ssm.Server.Cluster, ssm.Server.Host, ssm.Server.Version, int(ssm.Stats.Connections), len(ssm.Stats.Routes), len(ssm.Stats.Gateways), humanize.IBytes(uint64(ssm.Stats.Mem)), fmt.Sprintf("%.1f", ssm.Stats.CPU), ssm.Stats.SlowConsumers, humanizeTime(ssm.Stats.Start))

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

	if c.json {
		j, err := json.Marshal(results)
		if err != nil {
			return err
		}

		fmt.Println(string(j))
		return nil
	}

	fmt.Print(table.Render())

	if c.expect != 0 && c.expect != seen {
		fmt.Printf("\nMissing %d server(s)\n", c.expect-atomic.LoadUint32(&seen))
	}

	return nil
}

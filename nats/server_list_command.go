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
	"os"
	"os/signal"
	"regexp"
	"sort"
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

type srvListCluster struct {
	name  string
	nodes []string
	gwOut int
	gwIn  int
}

func configureServerListCommand(srv *kingpin.CmdClause) {
	c := &SrvLsCmd{}

	ls := srv.Command("list", "List known servers").Alias("ls").Action(c.list)
	ls.Arg("expect", "How many servers to expect").Uint32Var(&c.expect)
	ls.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	ls.Flag("filter", "Regular expression filter on server name").Short('f').StringVar(&c.filter)
}

func (c *SrvLsCmd) list(_ *kingpin.ParseContext) error {
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

	var (
		clusters          = make(map[string]*srvListCluster)
		servers           = 0
		connections       = 0
		memory      int64 = 0
		slow        int64 = 0
	)

	table := tablewriter.CreateTable()
	table.AddTitle("Server Overview")
	table.AddHeaders("Name", "Cluster", "IP", "Version", "Conns", "Routes", "GWs", "Mem", "CPU", "Slow", "Uptime")

	sub, err := ec.Subscribe(nc.NewRespInbox(), func(ssm *server.ServerStatsMsg) {
		last := atomic.AddUint32(&seen, 1)

		if !filter.MatchString(ssm.Server.Name) {
			return
		}

		servers++
		connections += ssm.Stats.Connections
		memory += ssm.Stats.Mem
		slow += ssm.Stats.SlowConsumers

		cluster := ssm.Server.Cluster
		if cluster != "" {
			_, ok := clusters[cluster]
			if !ok {
				clusters[cluster] = &srvListCluster{cluster, []string{}, 0, 0}
			}

			clusters[cluster].nodes = append(clusters[cluster].nodes, ssm.Server.Name)
			clusters[cluster].gwOut += len(ssm.Stats.Gateways)
			for _, g := range ssm.Stats.Gateways {
				clusters[cluster].gwIn += g.NumInbound
			}
		}

		mu.Lock()
		results = append(results, ssm)
		mu.Unlock()

		table.AddRow(ssm.Server.Name, ssm.Server.Cluster, ssm.Server.Host, ssm.Server.Version, ssm.Stats.Connections, len(ssm.Stats.Routes), len(ssm.Stats.Gateways), humanize.IBytes(uint64(ssm.Stats.Mem)), fmt.Sprintf("%.1f", ssm.Stats.CPU), ssm.Stats.SlowConsumers, humanizeTime(ssm.Stats.Start))

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
		printJSON(results)
		return nil
	}

	table.AddSeparator()
	table.AddRow("", fmt.Sprintf("%d Clusters", len(clusters)), fmt.Sprintf("%d Servers", servers), "", connections, "", "", humanize.IBytes(uint64(memory)), "", slow, "")
	fmt.Print(table.Render())

	if c.expect != 0 && c.expect != seen {
		fmt.Printf("\nMissing %d server(s)\n", c.expect-atomic.LoadUint32(&seen))
	}

	if len(clusters) > 0 {
		c.showClusters(clusters)
	}

	return nil
}

func (c *SrvLsCmd) showClusters(cl map[string]*srvListCluster) {
	fmt.Println()
	table := tablewriter.CreateTable()
	table.AddTitle("Cluster Overview")
	table.AddHeaders("Cluster", "Node Count", "Outgoing Gateways", "Incoming Gateways")

	var clusters []*srvListCluster
	for c := range cl {
		clusters = append(clusters, cl[c])
	}

	sort.Slice(clusters, func(i, j int) bool {
		return len(clusters[i].nodes) > len(clusters[j].nodes)
	})

	in := 0
	out := 0
	nodes := 0

	for _, c := range clusters {
		in += c.gwIn
		out += c.gwOut
		nodes += len(c.nodes)
		table.AddRow(c.name, len(c.nodes), c.gwOut, c.gwIn)
	}
	table.AddSeparator()
	table.AddRow("", nodes, out, in)

	fmt.Print(table.Render())
}

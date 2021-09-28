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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvLsCmd struct {
	expect  uint32
	json    bool
	sort    string
	reverse bool
	compact bool
}

type srvListCluster struct {
	name  string
	nodes []string
	gwOut int
	gwIn  int
	conns int
}

func configureServerListCommand(srv *kingpin.CmdClause) {
	c := &SrvLsCmd{}

	ls := srv.Command("list", "List known servers").Alias("ls").Action(c.list)
	ls.Arg("expect", "How many servers to expect").Uint32Var(&c.expect)
	ls.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	ls.Flag("sort", "Sort servers by a specific key (name,conns,subs,routes,gws,mem,cpu,slow,uptime,rtt").Default("rtt").EnumVar(&c.sort, strings.Split("name,conns,conn,subs,sub,routes,route,gw,mem,cpu,slow,uptime,rtt", ",")...)
	ls.Flag("reverse", "Reverse sort servers").Short('R').Default("false").BoolVar(&c.reverse)
	ls.Flag("compact", "Compact server names").Default("true").BoolVar(&c.compact)
}

func (c *SrvLsCmd) list(_ *kingpin.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	type result struct {
		*server.ServerStatsMsg
		rtt time.Duration
	}

	var (
		results     []*result
		names       []string
		clusters    = make(map[string]*srvListCluster)
		servers     int
		connections int
		memory      int64
		slow        int64
		subs        uint32
		js          int
		start       = time.Now()
		mu          sync.Mutex
	)

	doReqAsync(nil, "$SYS.REQ.SERVER.PING", int(c.expect), nc, func(data []byte) {
		ssm := &server.ServerStatsMsg{}
		err = json.Unmarshal(data, ssm)
		if err != nil {
			log.Printf("Could not decode response: %s", err)
			os.Exit(1)
		}

		mu.Lock()
		defer mu.Unlock()

		servers++
		connections += ssm.Stats.Connections
		memory += ssm.Stats.Mem
		slow += ssm.Stats.SlowConsumers
		subs += ssm.Stats.NumSubs
		if ssm.Server.JetStream {
			js++
		}

		cluster := ssm.Server.Cluster
		if cluster != "" {
			_, ok := clusters[cluster]
			if !ok {
				clusters[cluster] = &srvListCluster{cluster, []string{}, 0, 0, 0}
			}

			clusters[cluster].conns += ssm.Stats.Connections
			clusters[cluster].nodes = append(clusters[cluster].nodes, ssm.Server.Name)
			clusters[cluster].gwOut += len(ssm.Stats.Gateways)
			for _, g := range ssm.Stats.Gateways {
				clusters[cluster].gwIn += g.NumInbound
			}
		}

		results = append(results, &result{
			ServerStatsMsg: ssm,
			rtt:            time.Since(start),
		})
	})

	if len(results) == 0 {
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}

	if c.json {
		printJSON(results)
		return nil
	}

	// we reverse sort by default now, setting reverse=true means
	// do not reverse, so this function seems really weird but its right
	rev := func(v bool) bool {
		if !c.reverse {
			return !v
		}
		return v
	}

	sort.Slice(results, func(i int, j int) bool {
		stati := results[i].Stats
		statj := results[j].Stats

		switch c.sort {
		case "name":
			return results[i].Server.Name < results[j].Server.Name
		case "conns", "conn":
			return rev(stati.Connections < statj.Connections)
		case "subs", "sub":
			return rev(stati.NumSubs < statj.NumSubs)
		case "routes", "route":
			return rev(len(stati.Routes) < len(statj.Routes))
		case "gws", "gw":
			return rev(len(stati.Gateways) < len(statj.Gateways))
		case "mem":
			return rev(stati.Mem < statj.Mem)
		case "cpu":
			return rev(stati.CPU < statj.CPU)
		case "slow":
			return rev(stati.SlowConsumers < statj.SlowConsumers)
		case "uptime":
			return rev(stati.Start.UnixNano() > statj.Start.UnixNano())
		default:
			return rev(results[i].rtt > results[j].rtt)
		}
	})

	table := newTableWriter("Server Overview")
	table.AddHeaders("Name", "Cluster", "IP", "Version", "JS", "Conns", "Subs", "Routes", "GWs", "Mem", "CPU", "Slow", "Uptime", "RTT")

	// here so its after the sort
	for _, ssm := range results {
		names = append(names, ssm.Server.Name)
	}
	cNames := names
	if c.compact {
		cNames = compactStrings(names)
	}

	for i, ssm := range results {
		jsEnabled := "no"
		if ssm.Server.JetStream {
			jsEnabled = "yes"
		}
		table.AddRow(cNames[i], ssm.Server.Cluster, ssm.Server.Host, ssm.Server.Version, jsEnabled, ssm.Stats.Connections, ssm.Stats.NumSubs, len(ssm.Stats.Routes), len(ssm.Stats.Gateways), humanize.IBytes(uint64(ssm.Stats.Mem)), fmt.Sprintf("%.1f", ssm.Stats.CPU), ssm.Stats.SlowConsumers, humanizeTime(ssm.Stats.Start), ssm.rtt)
	}

	table.AddSeparator()
	table.AddRow("", fmt.Sprintf("%d Clusters", len(clusters)), fmt.Sprintf("%d Servers", servers), "", js, connections, subs, "", "", humanize.IBytes(uint64(memory)), "", slow, "", "")
	fmt.Print(table.Render())

	if len(clusters) > 0 {
		c.showClusters(clusters)
	}

	return nil
}

func (c *SrvLsCmd) showClusters(cl map[string]*srvListCluster) {
	fmt.Println()
	table := newTableWriter("Cluster Overview")
	table.AddHeaders("Cluster", "Node Count", "Outgoing Gateways", "Incoming Gateways", "Connections")

	var clusters []*srvListCluster
	for c := range cl {
		clusters = append(clusters, cl[c])
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].conns < clusters[j].conns
	})

	in := 0
	out := 0
	nodes := 0
	conns := 0

	for _, c := range clusters {
		in += c.gwIn
		out += c.gwOut
		nodes += len(c.nodes)
		conns += c.conns
		table.AddRow(c.name, len(c.nodes), c.gwOut, c.gwIn, c.conns)
	}
	table.AddSeparator()
	table.AddRow("", nodes, out, in, conns)

	fmt.Print(table.Render())
}

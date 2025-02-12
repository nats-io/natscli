// Copyright 2020-2025 The NATS Authors
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
	iu "github.com/nats-io/natscli/internal/util"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
)

type SrvLsCmd struct {
	expect  uint32
	json    bool
	sort    string
	reverse bool
	compact bool
}

type srvListCluster struct {
	name       string
	nodes      []string
	gwOut      int
	gwIn       int
	conns      int
	routeSizes []int
}

func configureServerListCommand(srv *fisk.CmdClause) {
	c := &SrvLsCmd{}

	ls := srv.Command("list", "List known servers").Alias("ls").Action(c.list)
	ls.Arg("expect", "How many servers to expect").Uint32Var(&c.expect)
	ls.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	ls.Flag("sort", "Sort servers by a specific key (name,cluster,conns,subs,routes,gws,mem,cpu,slow,uptime,rtt").Default("rtt").EnumVar(&c.sort, strings.Split("name,cluster,conns,conn,subs,sub,routes,route,gw,mem,cpu,slow,uptime,rtt", ",")...)
	ls.Flag("reverse", "Reverse sort servers").Short('R').UnNegatableBoolVar(&c.reverse)
	ls.Flag("compact", "Compact server names").Default("true").BoolVar(&c.compact)
}

func (c *SrvLsCmd) list(_ *fisk.ParseContext) error {
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
		hosts       []string
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
				clusters[cluster] = &srvListCluster{name: cluster}
			}

			clusters[cluster].conns += ssm.Stats.Connections
			clusters[cluster].nodes = append(clusters[cluster].nodes, ssm.Server.Name)
			clusters[cluster].gwOut += len(ssm.Stats.Gateways)
			clusters[cluster].routeSizes = append(clusters[cluster].routeSizes, len(ssm.Stats.Routes))

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
		iu.PrintJSON(results)
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
			return rev(results[i].Server.Name > results[j].Server.Name)
		case "conns", "conn":
			return rev(stati.Connections < statj.Connections)
		case "subs", "sub":
			return rev(stati.NumSubs < statj.NumSubs)
		case "routes", "route":
			// if routes are the same, most typical, we sort by name
			il := len(stati.Routes)
			jl := len(statj.Routes)

			if il != jl {
				return rev(il < jl)
			}

			return rev(results[i].Server.Name > results[j].Server.Name)
		case "gws", "gw":
			// if gateways are the same, most typical, we sort by name
			il := len(stati.Gateways)
			jl := len(statj.Gateways)

			if il != jl {
				return rev(il < jl)
			}

			return rev(results[i].Server.Name > results[j].Server.Name)
		case "mem":
			return rev(stati.Mem < statj.Mem)
		case "cpu":
			return rev(stati.CPU < statj.CPU)
		case "slow":
			return rev(stati.SlowConsumers < statj.SlowConsumers)
		case "uptime":
			return rev(stati.Start.UnixNano() > statj.Start.UnixNano())
		case "cluster":
			// we default to reverse, so we swap this since alpha is better by default
			if results[i].Server.Cluster != results[j].Server.Cluster {
				return !rev(results[i].Server.Cluster < results[j].Server.Cluster)
			}

			return !rev(results[i].Server.Name > results[j].Server.Name)
		default:
			return rev(results[i].rtt > results[j].rtt)
		}
	})

	table := iu.NewTableWriter(opts(), "Server Overview")
	table.AddHeaders("Name", "Cluster", "Host", "Version", "JS", "Conns", "Subs", "Routes", "GWs", "Mem", "CPU %", "Cores", "Slow", "Uptime", "RTT")

	// here so its after the sort
	for _, ssm := range results {
		names = append(names, ssm.Server.Name)
		hosts = append(hosts, ssm.Server.Host)
	}
	cNames := names
	cHosts := hosts
	if c.compact {
		cNames = iu.CompactStrings(names)
		cHosts = iu.CompactStrings(hosts)
	}

	versionsOk := ""
	gwaysOk := ""
	routesOk := ""

	// handle asymmetric clusters by ensuring each cluster has same route count
	// rather than all nodes in all clusters having the same route count
	for _, v := range clusters {
		for _, s := range v.routeSizes {
			if s != v.routeSizes[0] {
				routesOk = "X"
			}
		}
	}

	for i, ssm := range results {
		cluster := ssm.Server.Cluster
		jsEnabled := "no"
		if ssm.Server.JetStream {
			if ssm.Server.Domain != "" {
				jsEnabled = ssm.Server.Domain
			} else {
				jsEnabled = "yes"
			}
		}

		if ssm.Server.Version != results[0].ServerStatsMsg.Server.Version {
			versionsOk = "X"
		}

		if len(ssm.Stats.Gateways) != len(results[0].ServerStatsMsg.Stats.Gateways) {
			gwaysOk = "X"
		}

		var slow []string
		if ssm.Stats.SlowConsumersStats != nil {
			sstat := ssm.Stats.SlowConsumersStats
			if sstat.Clients > 0 {
				slow = append(slow, fmt.Sprintf("c: %s", f(sstat.Clients)))
			}
			if sstat.Routes > 0 {
				slow = append(slow, fmt.Sprintf("r: %s", f(sstat.Routes)))
			}
			if sstat.Gateways > 0 {
				slow = append(slow, fmt.Sprintf("g: %s", f(sstat.Gateways)))
			}
			if sstat.Leafs > 0 {
				slow = append(slow, fmt.Sprintf("l: %s", f(sstat.Leafs)))
			}

			// only print details if non clients also had slow consumers
			if len(slow) == 1 && sstat.Clients > 0 {
				slow = []string{}
			}
		}

		sc := f(ssm.Stats.SlowConsumers)
		if len(slow) > 0 {
			sc = fmt.Sprintf("%s (%s)", sc, strings.Join(slow, " "))
		}

		table.AddRow(
			cNames[i],
			cluster,
			cHosts[i],
			ssm.Server.Version,
			jsEnabled,
			f(ssm.Stats.Connections),
			f(ssm.Stats.NumSubs),
			len(ssm.Stats.Routes),
			len(ssm.Stats.Gateways),
			humanize.IBytes(uint64(ssm.Stats.Mem)),
			fmt.Sprintf("%.0f", ssm.Stats.CPU),
			ssm.Stats.Cores,
			sc,
			f(ssm.Server.Time.Sub(ssm.Stats.Start)),
			f(ssm.rtt.Round(time.Millisecond)))
	}

	table.AddFooter(
		"",
		len(clusters),
		servers,
		versionsOk,
		js,
		f(connections),
		f(subs),
		routesOk,
		gwaysOk,
		humanize.IBytes(uint64(memory)),
		"",
		"",
		f(slow),
		"",
		"")

	fmt.Print(table.Render())

	if len(clusters) > 0 {
		c.showClusters(clusters)
	}

	return nil
}

func (c *SrvLsCmd) showClusters(cl map[string]*srvListCluster) {
	fmt.Println()
	table := iu.NewTableWriter(opts(), "Cluster Overview")
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

	table.AddFooter("", nodes, out, in, conns)

	fmt.Print(table.Render())
}

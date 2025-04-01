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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type SrvReportCmd struct {
	json bool

	filterExpression        string
	account                 string
	user                    string
	waitFor                 int
	sort                    string
	topk                    int
	reverse                 bool
	compact                 bool
	subject                 string
	server                  string
	cluster                 string
	tags                    []string
	stateFilter             string
	filterReason            string
	skipDiscoverClusterSize bool
	gatewayName             string
	jsEnabled               bool
	jsServerOnly            bool
	stream                  string
	consumer                string
	watchInterval           int
	nc                      *nats.Conn
}

type srvReportAccountInfo struct {
	Account     string               `json:"account"`
	Connections int                  `json:"connections"`
	ConnInfo    []connInfo           `json:"connection_info"`
	InMsgs      int64                `json:"in_msgs"`
	OutMsgs     int64                `json:"out_msgs"`
	InBytes     int64                `json:"in_bytes"`
	OutBytes    int64                `json:"out_bytes"`
	Subs        int                  `json:"subscriptions"`
	Server      []*server.ServerInfo `json:"server"`
}

func configureServerReportCommand(srv *fisk.CmdClause) {
	c := &SrvReportCmd{}

	report := srv.Command("report", "Report on various server metrics").Alias("rep")
	report.Flag("reverse", "Reverse sort connections").Short('R').UnNegatableBoolVar(&c.reverse)

	addFilterOpts := func(cmd *fisk.CmdClause) {
		cmd.Flag("host", "Limit the report to a specific NATS server").StringVar(&c.server)
		cmd.Flag("cluster", "Limit the report to a specific Cluster").StringVar(&c.cluster)
		cmd.Flag("tags", "Limit the report to nodes matching certain tags").StringsVar(&c.tags)
		cmd.Flag("watch", "Display the results and update it every (WATCH) seconds").IntVar(&c.watchInterval)
	}

	acct := report.Command("accounts", "Report on account activity").Alias("acct").Action(c.withWatcher(c.reportAccount))
	acct.Arg("account", "Account to produce a report for").StringVar(&c.account)
	acct.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	acct.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,conns,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "conns", "subs")
	acct.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	addFilterOpts(acct)
	acct.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.withWatcher(c.reportConnections))
	conns.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	conns.Flag("account", "Limit report to a specific account").StringVar(&c.account)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	conns.Flag("subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subject)
	conns.Flag("username", "Limits responses only to those connections for a specific authentication username").StringVar(&c.user)
	conns.Flag("state", "Limits responses only to those connections that are in a specific state (open, closed, all)").PlaceHolder("STATE").Default("open").EnumVar(&c.stateFilter, "open", "closed", "all")
	conns.Flag("closed-reason", "Filter results based on a closed reason").PlaceHolder("REASON").StringVar(&c.filterReason)
	conns.Flag("filter", "Expression based filter for connections").StringVar(&c.filterExpression)
	addFilterOpts(conns)
	conns.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	cpu := report.Command("cpu", "Report on CPU usage").Action(c.withWatcher(c.reportCPU))
	cpu.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	addFilterOpts(cpu)
	cpu.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	gateways := report.Command("gateways", "Repost on Gateway (Super Cluster) connections").Alias("super").Alias("gateway").Action(c.withWatcher(c.reportGateway))
	gateways.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	gateways.Flag("filter-name", "Limits responses to a certain name").StringVar(&c.gatewayName)
	gateways.Flag("sort", "Sorts by a specific property (server,cluster)").Default("cluster").EnumVar(&c.sort, "server", "cluster")
	addFilterOpts(gateways)

	health := report.Command("health", "Report on Server health").Action(c.withWatcher(c.reportHealth))
	health.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	health.Flag("js-enabled", "Checks that JetStream should be enabled on all servers").Short('J').BoolVar(&c.jsEnabled)
	health.Flag("server-only", "Restricts the health check to the JetStream server only, do not check streams and consumers").Short('S').BoolVar(&c.jsServerOnly)
	health.Flag("account", "Check only a specific Account").StringVar(&c.account)
	health.Flag("stream", "Check only a specific Stream").StringVar(&c.stream)
	health.Flag("consumer", "Check only a specific Consumer").StringVar(&c.consumer)
	addFilterOpts(health)

	jsz := report.Command("jetstream", "Report on JetStream activity").Alias("jsz").Alias("js").Action(c.withWatcher(c.reportJetStream))
	jsz.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	jsz.Flag("account", "Produce the report for a specific account").StringVar(&c.account)
	jsz.Flag("sort", "Sort by a specific property (name,cluster,streams,consumers,msgs,mbytes,mem,file,api,err").Default("cluster").EnumVar(&c.sort, "name", "cluster", "streams", "consumers", "msgs", "mbytes", "bytes", "mem", "file", "store", "api", "err")
	jsz.Flag("compact", "Compact server names").Default("true").BoolVar(&c.compact)
	addFilterOpts(jsz)

	leafs := report.Command("leafnodes", "Report on Leafnode connections").Alias("leaf").Alias("leafz").Action(c.withWatcher(c.reportLeafs))
	leafs.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	leafs.Flag("account", "Produce the report for a specific account").StringVar(&c.account)
	leafs.Flag("sort", "Sort by a specific property (server,name,account,subs,in-bytes,out-bytes,in-msgs,out-msgs)").EnumVar(&c.sort, "server", "name", "account", "subs", "in-bytes", "out-bytes", "in-msgs", "out-msgs")
	addFilterOpts(leafs)

	mem := report.Command("mem", "Report on Memory usage").Action(c.withWatcher(c.reportMem))
	mem.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	addFilterOpts(mem)
	mem.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	routes := report.Command("routes", "Report on Route (Cluster) connections").Alias("route").Action(c.withWatcher(c.reportRoute))
	routes.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	routes.Flag("sort", "Sort by a specific property (server,cluster,name,account,subs,in-bytes,out-bytes)").EnumVar(&c.sort, "server", "cluster", "name", "account", "subs", "in-bytes", "out-bytes")
	addFilterOpts(routes)
}

func (c *SrvReportCmd) withWatcher(fn func(*fisk.ParseContext) error) func(*fisk.ParseContext) error {
	return func(fctx *fisk.ParseContext) error {
		nc, _, err := prepareHelper("", natsOpts()...)
		if err != nil {
			return err
		}

		c.nc = nc

		if c.watchInterval <= 0 {
			return fn(fctx)
		}

		tick := time.NewTicker(time.Second * time.Duration(c.watchInterval))
		ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
		defer cancel()

		fn(fctx)

		for {
			select {
			case <-tick.C:
				fn(fctx)
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (c *SrvReportCmd) reportLeafs(_ *fisk.ParseContext) error {
	req := server.LeafzEventOptions{
		LeafzOptions: server.LeafzOptions{
			Account: c.account,
		},
		EventFilterOptions: c.reqFilter(),
	}

	results, err := doReq(req, "$SYS.REQ.SERVER.PING.LEAFZ", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	type leaf struct {
		server *server.ServerInfo
		leafs  *server.LeafInfo
	}

	var leafs []*leaf
	for _, result := range results {
		s := &server.ServerAPILeafzResponse{}
		err := json.Unmarshal(result, s)
		if err != nil {
			return err
		}

		if s.Error != nil {
			return fmt.Errorf("%v", s.Error.Error())
		}

		for _, l := range s.Data.Leafs {
			leafs = append(leafs, &leaf{
				server: s.Server,
				leafs:  l,
			})
		}
	}

	sort.Slice(leafs, func(i, j int) bool {
		switch c.sort {
		case "name":
			return c.boolReverse(leafs[i].leafs.Name < leafs[j].leafs.Name)
		case "account":
			return c.boolReverse(leafs[i].leafs.Account < leafs[j].leafs.Account)
		case "subs":
			return c.boolReverse(leafs[i].leafs.NumSubs < leafs[j].leafs.NumSubs)
		case "in-bytes":
			return c.boolReverse(leafs[i].leafs.InBytes < leafs[j].leafs.InBytes)
		case "out-bytes":
			return c.boolReverse(leafs[i].leafs.OutBytes < leafs[j].leafs.OutBytes)
		case "in-msgs":
			return c.boolReverse(leafs[i].leafs.InMsgs < leafs[j].leafs.InMsgs)
		case "out-msgs":
			return c.boolReverse(leafs[i].leafs.OutMsgs < leafs[j].leafs.OutMsgs)
		default:
			return c.boolReverse(leafs[i].server.Name < leafs[j].server.Name)
		}
	})

	tbl := iu.NewTableWriter(opts(), "Leafnode Report")
	tbl.AddHeaders("Server", "Name", "Account", "Address", "RTT", "Msgs In", "Msgs Out", "Bytes In", "Bytes Out", "Subs", "Compressed", "Spoke")

	for _, lz := range leafs {
		acct := lz.leafs.Account
		if len(acct) > 23 {
			acct = fmt.Sprintf("%s...%s", acct[0:10], acct[len(acct)-10:])
		}

		tbl.AddRow(
			lz.server.Name,
			lz.leafs.Name,
			acct,
			fmt.Sprintf("%s:%d", lz.leafs.IP, lz.leafs.Port),
			lz.leafs.RTT,
			f(lz.leafs.InMsgs),
			f(lz.leafs.OutMsgs),
			fiBytes(uint64(lz.leafs.InBytes)),
			fiBytes(uint64(lz.leafs.OutBytes)),
			f(lz.leafs.NumSubs),
			f(lz.leafs.Compression),
			f(lz.leafs.IsSpoke),
		)
	}

	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) parseRtt(rtt string, crit time.Duration) string {
	d, err := time.ParseDuration(rtt)
	if err != nil {
		return rtt
	}

	if d < crit {
		return f(d)
	}

	return color.RedString(f(d))
}

func (c *SrvReportCmd) reportHealth(_ *fisk.ParseContext) error {
	req := server.HealthzEventOptions{
		HealthzOptions: server.HealthzOptions{
			JSEnabledOnly: c.jsEnabled,
			JSServerOnly:  c.jsServerOnly,
			Account:       c.account,
			Stream:        c.stream,
			Consumer:      c.consumer,
			Details:       true,
		},
		EventFilterOptions: c.reqFilter(),
	}
	results, err := doReq(req, "$SYS.REQ.SERVER.PING.HEALTHZ", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	var servers []server.ServerAPIHealthzResponse
	for _, result := range results {
		s := &server.ServerAPIHealthzResponse{}
		err := json.Unmarshal(result, s)
		if err != nil {
			return err
		}

		if s.Error != nil {
			return fmt.Errorf("%v", s.Error.Error())
		}

		servers = append(servers, *s)
	}

	sort.Slice(servers, func(i, j int) bool {
		return c.boolReverse(servers[i].Server.Name < servers[j].Server.Name)
	})

	tbl := iu.NewTableWriter(opts(), "Health Report")
	tbl.AddHeaders("Server", "Cluster", "Domain", "Status", "Type", "Error")

	var ok, notok, totalErrors int
	totalClusters := map[string]struct{}{}

	for _, srv := range servers {
		tbl.AddRow(
			srv.Server.Name,
			srv.Server.Cluster,
			srv.Server.Domain,
			fmt.Sprintf("%s (%d)", srv.Data.Status, srv.Data.StatusCode),
		)

		if srv.Data.StatusCode == 200 {
			ok += 1
		} else {
			notok += 1
		}

		totalClusters[srv.Server.Cluster] = struct{}{}

		ecnt := len(srv.Data.Errors)
		if ecnt == 0 {
			continue
		}

		totalErrors += ecnt

		show := ecnt
		if ecnt > 10 {
			show = 9
		}

		for _, errStatus := range srv.Data.Errors[0:show] {
			tbl.AddRow(
				"", "", "", "",
				errStatus.Type.String(),
				errStatus.Error,
			)
		}
		if show != ecnt {
			tbl.AddRow("", "", "", fmt.Sprintf("%d more errors", ecnt-show))
		}
	}

	//tbl.AddSeparator()
	tbl.AddFooter(f(len(servers)), f(len(totalClusters)), "", f(fmt.Sprintf("ok: %d / err: %d", ok, notok)), "", f(totalErrors))

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportGateway(_ *fisk.ParseContext) error {
	req := &server.GatewayzEventOptions{
		EventFilterOptions: c.reqFilter(),
		GatewayzOptions: server.GatewayzOptions{
			Name:     c.gatewayName,
			Accounts: true,
		},
	}

	results, err := doReq(req, "$SYS.REQ.SERVER.PING.GATEWAYZ", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	var gateways []server.ServerAPIGatewayzResponse
	for _, result := range results {
		g := &server.ServerAPIGatewayzResponse{}
		err := json.Unmarshal(result, g)
		if err != nil {
			return err
		}

		if g.Error != nil {
			return fmt.Errorf("%v", g.Error.Error())
		}

		gateways = append(gateways, *g)
	}

	sort.Slice(gateways, func(i, j int) bool {
		switch c.sort {
		case "cluster":
			if gateways[i].Server.Cluster == gateways[j].Server.Cluster {
				return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)
			}
			return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)

		case "server":
			if gateways[i].Server.Name == gateways[j].Server.Name {
				return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)
			}
			return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)

		default:
			if gateways[i].Server.Name == gateways[j].Server.Name {
				return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)
			}
			return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)
		}
	})

	tbl := iu.NewTableWriter(opts(), "Super Cluster Report")
	tbl.AddHeaders("Server", "Name", "Port", "Kind", "Connection", "ID", "Uptime", "RTT", "Bytes", "Accounts")

	var lastServer, lastName, lastDirection string
	var totalBytes int64
	var totalServers, totalGateways int
	totalClusters := map[string]struct{}{}

	for _, g := range gateways {
		sname := g.Server.Name
		totalServers++

		if sname == lastServer {
			sname = ""
		}
		lastServer = g.Server.Name

		cname := g.Server.Cluster
		if cname == lastName {
			cname = ""
		}
		lastName = g.Server.Name
		totalClusters[cname] = struct{}{}

		tbl.AddRow(
			sname,
			cname,
			g.Data.Port,
			"", "", "", "", "", "", "",
		)

		lastDirection = ""
		for gname, conns := range g.Data.InboundGateways {
			for _, conn := range conns {
				totalGateways++
				direction := "Inbound"
				if direction == lastDirection {
					direction = ""
				} else {
					lastDirection = "Inbound"
				}

				uptime := conn.Connection.Uptime
				if d, err := time.ParseDuration(uptime); err == nil {
					uptime = f(d)
				}

				totalBytes += conn.Connection.InBytes
				tbl.AddRow(
					"", "", "",
					direction,
					fmt.Sprintf("%s %s:%d", gname, conn.Connection.IP, conn.Connection.Port),
					fmt.Sprintf("gid:%d", conn.Connection.Cid),
					uptime,
					c.parseRtt(conn.Connection.RTT, 300*time.Millisecond),
					fiBytes(uint64(conn.Connection.InBytes)),
					f(len(conn.Accounts)),
				)
			}
		}

		for gname, conn := range g.Data.OutboundGateways {
			totalGateways++
			direction := "Outbound"
			if direction == lastDirection {
				direction = ""
			} else {
				lastDirection = "Outbound"
			}

			totalBytes += conn.Connection.OutBytes
			tbl.AddRow(
				"", "", "",
				direction,
				fmt.Sprintf("%s %s:%d", gname, conn.Connection.IP, conn.Connection.Port),
				fmt.Sprintf("gid:%d", conn.Connection.Cid),
				conn.Connection.Uptime,
				c.parseRtt(conn.Connection.RTT, 300*time.Millisecond),
				fiBytes(uint64(conn.Connection.OutBytes)),
				f(len(conn.Accounts)),
			)
		}
	}

	tbl.AddFooter(f(totalServers), len(totalClusters), "", "", f(totalGateways), "", "", "", fiBytes(uint64(totalBytes)), "")

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportRoute(_ *fisk.ParseContext) error {
	req := &server.RoutezEventOptions{
		EventFilterOptions: c.reqFilter(),
	}
	results, err := doReq(req, "$SYS.REQ.SERVER.PING.ROUTEZ", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	type routeData struct {
		server *server.ServerInfo
		routes *server.RouteInfo
	}

	routes := []routeData{}
	for _, result := range results {
		r := &server.ServerAPIRoutezResponse{}
		err := json.Unmarshal(result, r)
		if err != nil {
			return err
		}

		if r.Error != nil {
			return fmt.Errorf("%v", r.Error.Error())
		}

		if len(r.Data.Routes) > 0 {
			for _, route := range r.Data.Routes {
				routes = append(routes, routeData{r.Server, route})
			}
		}
	}

	sort.Slice(routes, func(i, j int) bool {
		a, b := routes[i], routes[j]

		var wasSorted bool
		switch c.sort {
		case "cluster":
			if a.server.Cluster != b.server.Cluster {
				wasSorted = a.server.Cluster < b.server.Cluster
			}
		case "name":
			if a.routes.RemoteName != b.routes.RemoteName {
				wasSorted = a.routes.RemoteName < b.routes.RemoteName
			}
		case "account", "acct":
			if a.routes.Account != b.routes.Account {
				wasSorted = a.routes.Account < b.routes.Account
			}
		case "subs":
			if a.routes.NumSubs != b.routes.NumSubs {
				wasSorted = a.routes.NumSubs < b.routes.NumSubs
			}
		case "in-bytes":
			if a.routes.InBytes != b.routes.InBytes {
				wasSorted = a.routes.InBytes < b.routes.InBytes
			}
		case "out-bytes":
			if a.routes.OutBytes != b.routes.OutBytes {
				wasSorted = a.routes.OutBytes < b.routes.OutBytes
			}
		case "server":
			if a.server.Name != b.server.Name {
				wasSorted = a.server.Name < b.server.Name
			}
		default:
			if a.server.Name != b.server.Name {
				wasSorted = a.server.Name < b.server.Name
			}
		}

		// Enforce consistent ordering when primary values are equal
		if !wasSorted && !c.boolReverse(wasSorted) {
			switch {
			case a.server.Name != b.server.Name:
				wasSorted = a.server.Name < b.server.Name
			case a.server.Cluster != b.server.Cluster:
				wasSorted = a.server.Cluster < b.server.Cluster
			case a.routes.RemoteName != b.routes.RemoteName:
				wasSorted = a.routes.RemoteName < b.routes.RemoteName
			default:
				wasSorted = a.routes.Account < b.routes.Account
			}
		}

		return c.boolReverse(wasSorted)
	})

	tbl := iu.NewTableWriter(opts(), "Cluster Report")
	tbl.AddHeaders("Server", "Cluster", "Name", "Account", "Address", "ID", "Uptime", "RTT", "Subs", "Bytes In", "Bytes Out")

	var lastServer, lastCluster, lastRemote string
	var subs, bytesIn, bytesOut, totalRoutes int64
	totalServers := map[string]struct{}{}
	totalClusters := map[string]struct{}{}

	for _, route := range routes {
		r := route.routes

		totalServers[route.server.Name] = struct{}{}
		totalClusters[route.server.Cluster] = struct{}{}

		totalRoutes++

		uptime := route.server.Time.Sub(r.Start)
		sname := route.server.Name
		if sname == lastServer {
			sname = ""
		} else {
			lastRemote = ""
			lastCluster = ""
		}
		lastServer = route.server.Name

		cname := route.server.Cluster
		if cname == lastCluster {
			cname = ""
		}
		lastCluster = route.server.Cluster

		rname := r.RemoteName
		if rname == lastRemote {
			rname = ""
		}
		lastRemote = r.RemoteName

		acct := r.Account
		if len(acct) > 23 {
			acct = fmt.Sprintf("%s...%s", acct[0:10], acct[len(acct)-10:])
		}
		subs += int64(r.NumSubs)
		bytesIn += r.InBytes
		bytesOut += r.OutBytes

		tbl.AddRow(
			sname,
			cname,
			rname,
			acct,
			fmt.Sprintf("%s:%d", r.IP, r.Port),
			fmt.Sprintf("rid:%d", r.Rid),
			f(uptime),
			c.parseRtt(r.RTT, 100*time.Millisecond),
			f(r.NumSubs),
			fiBytes(uint64(r.InBytes)),
			fiBytes(uint64(r.OutBytes)),
		)
	}

	tbl.AddFooter(f(len(totalServers)), f(len(totalClusters)), "", "", f(totalRoutes), "", "", "", f(subs), fiBytes(uint64(bytesIn)), fiBytes(uint64(bytesOut)))

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportMem(_ *fisk.ParseContext) error {
	return c.reportCpuOrMem(true)
}

func (c *SrvReportCmd) reportCPU(_ *fisk.ParseContext) error {
	return c.reportCpuOrMem(false)
}

func (c *SrvReportCmd) reportCpuOrMem(mem bool) error {
	req := &server.StatszEventOptions{EventFilterOptions: c.reqFilter()}
	results, err := doReq(req, "$SYS.REQ.SERVER.PING", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	usage := map[string]float64{}

	for _, result := range results {
		sr := &server.ServerStatsMsg{}
		err := json.Unmarshal(result, sr)
		if err != nil {
			return err
		}

		if mem {
			usage[sr.Server.Name] = float64(sr.Stats.Mem)
		} else {
			usage[sr.Server.Name] = sr.Stats.CPU
		}
	}

	if c.json {
		return iu.PrintJSON(usage)
	}

	width := iu.ProgressWidth() / 2
	if width > 30 {
		width = 30
	}

	if mem {
		return iu.BarGraph(os.Stdout, usage, "Memory Usage", width, true)
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	return iu.BarGraph(os.Stdout, usage, "CPU Usage", width, false)
}

func (c *SrvReportCmd) reportJetStream(_ *fisk.ParseContext) error {
	jszOpts := server.JSzOptions{}
	if c.account != "" {
		jszOpts.Account = c.account
		jszOpts.Streams = true
		jszOpts.Consumer = true
		jszOpts.Limit = 10000
	}

	req := &server.JszEventOptions{JSzOptions: jszOpts, EventFilterOptions: c.reqFilter()}
	res, err := doReq(req, "$SYS.REQ.SERVER.PING.JSZ", c.waitFor, c.nc)
	if err != nil {
		return err
	}

	var (
		names               []string
		jszResponses        []*server.ServerAPIJszResponse
		apiErrTotal         uint64
		apiTotal            uint64
		pendingTotal        int
		memoryTotal         uint64
		storeTotal          uint64
		consumersTotal      int
		streamsTotal        int
		bytesTotal          uint64
		msgsTotal           uint64
		cluster             *server.MetaClusterInfo
		expectedClusterSize int
	)

	// TODO: remove after 2.12 is out
	renderPending := iu.ServerMinVersion(c.nc, 2, 10, 21)
	renderDomain := false
	for _, r := range res {
		response := &server.ServerAPIJszResponse{}

		err = json.Unmarshal(r, &response)
		if err != nil {
			return err
		}

		if response.Data.Config.Domain != "" {
			renderDomain = true
		}

		jszResponses = append(jszResponses, response)
	}

	sort.Slice(jszResponses, func(i, j int) bool {
		switch c.sort {
		case "name":
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)
		case "streams":
			if jszResponses[i].Data.Streams != jszResponses[j].Data.Streams {
				return c.boolReverse(jszResponses[i].Data.Streams < jszResponses[j].Data.Streams)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)

		case "consumers":
			if jszResponses[i].Data.Consumers != jszResponses[j].Data.Consumers {
				return c.boolReverse(jszResponses[i].Data.Consumers < jszResponses[j].Data.Consumers)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)

		case "msgs":
			return c.boolReverse(jszResponses[i].Data.Messages < jszResponses[j].Data.Messages)
		case "mbytes", "bytes":
			return c.boolReverse(jszResponses[i].Data.Bytes < jszResponses[j].Data.Bytes)
		case "mem":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.Memory < jszResponses[j].Data.JetStreamStats.Memory)
		case "store", "file":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.Store < jszResponses[j].Data.JetStreamStats.Store)
		case "api":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.API.Total < jszResponses[j].Data.JetStreamStats.API.Total)
		case "err":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.API.Errors < jszResponses[j].Data.JetStreamStats.API.Errors)
		default:
			if jszResponses[i].Server.Cluster != jszResponses[j].Server.Cluster {
				return c.boolReverse(jszResponses[i].Server.Cluster < jszResponses[j].Server.Cluster)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)
		}
	})

	if len(jszResponses) == 0 {
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}

	// here so it's after the sort
	for _, js := range jszResponses {
		names = append(names, js.Server.Name)
	}
	var cNames []string
	if c.compact {
		cNames = iu.CompactStrings(names)
	} else {
		cNames = names
	}

	var table *iu.Table
	if c.account != "" {
		table = iu.NewTableWriter(opts(), fmt.Sprintf("JetStream Summary for Account %s", c.account))
	} else {
		table = iu.NewTableWriter(opts(), "JetStream Summary")
	}

	hdrs := []any{"Server", "Cluster"}
	if renderDomain {
		hdrs = append(hdrs, "Domain")
	}
	hdrs = append(hdrs, "Streams", "Consumers", "Messages", "Bytes", "Memory", "File", "API Req", "API Err")
	if renderPending {
		hdrs = append(hdrs, "Pending")
	}
	table.AddHeaders(hdrs...)

	for i, js := range jszResponses {
		jss := js.Data.JetStreamStats
		var acc *server.AccountDetail
		var doAccountStats bool

		if c.account != "" && len(js.Data.AccountDetails) == 1 {
			acc = js.Data.AccountDetails[0]
			jss = acc.JetStreamStats
			doAccountStats = true
		}

		apiErrTotal += jss.API.Errors
		apiTotal += jss.API.Total
		memoryTotal += jss.Memory
		storeTotal += jss.Store

		rPending := 0
		rStreams := 0
		rConsumers := 0
		rMessages := uint64(0)
		rBytes := uint64(0)

		if doAccountStats {
			rBytes = acc.Memory + acc.Store
			bytesTotal += rBytes
			rStreams = len(acc.Streams)
			streamsTotal += rStreams

			for _, sd := range acc.Streams {
				consumersTotal += sd.State.Consumers
				rConsumers += sd.State.Consumers
				msgsTotal += sd.State.Msgs
				rMessages += sd.State.Msgs
			}
		} else {
			consumersTotal += js.Data.Consumers
			rConsumers = js.Data.Consumers
			streamsTotal += js.Data.Streams
			rStreams = js.Data.Streams
			bytesTotal += js.Data.Bytes
			rBytes = js.Data.Bytes
			msgsTotal += js.Data.Messages
			rMessages = js.Data.Messages
		}

		leader := ""
		if js.Data.Meta != nil {
			if js.Data.Meta.Leader == js.Server.Name {
				leader = "*"
				cluster = js.Data.Meta
			}
			if expectedClusterSize < js.Data.Meta.Size {
				expectedClusterSize = js.Data.Meta.Size
			}
			rPending = js.Data.Meta.Pending
			pendingTotal += rPending
		}

		row := []any{cNames[i] + leader, js.Server.Cluster}
		if renderDomain {
			row = append(row, js.Data.Config.Domain)
		}
		errCol := f(jss.API.Errors)
		if jss.API.Total > 0 && jss.API.Errors > 0 {
			errRate := float64(jss.API.Errors) * 100 / float64(jss.API.Total)
			errCol += " / " + f(errRate) + "%"
		}
		row = append(row,
			f(rStreams),
			f(rConsumers),
			f(rMessages),
			humanize.IBytes(rBytes),
			humanize.IBytes(jss.Memory),
			humanize.IBytes(jss.Store),
			f(jss.API.Total),
			errCol,
		)
		if renderPending {
			row = append(row, rPending)
		}

		table.AddRow(row...)
	}

	row := []any{"", ""}
	if renderDomain {
		row = append(row, "")
	}
	row = append(row, f(streamsTotal), f(consumersTotal), f(msgsTotal), humanize.IBytes(bytesTotal), humanize.IBytes(memoryTotal), humanize.IBytes(storeTotal), f(apiTotal), f(apiErrTotal))
	if renderPending {
		row = append(row, pendingTotal)
	}
	table.AddFooter(row...)

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())
	fmt.Println()

	switch {
	case c.isFiltered():
	case expectedClusterSize == 0:
	case len(jszResponses) > 0 && cluster == nil:
		fmt.Println()
		fmt.Printf("WARNING: No cluster meta leader found. The cluster expects %d nodes but only %d responded. JetStream operation requires at least %d up nodes.", expectedClusterSize, len(jszResponses), expectedClusterSize/2+1)
		fmt.Println()
	default:
		cluster.Replicas = append(cluster.Replicas, &server.PeerInfo{
			Name:    cluster.Leader,
			Current: true,
			Offline: false,
			Active:  0,
			Lag:     0,
		})

		sort.Slice(cluster.Replicas, func(i, j int) bool {
			return cluster.Replicas[i].Name < cluster.Replicas[j].Name
		})

		names := []string{}
		for _, r := range cluster.Replicas {
			names = append(names, r.Name)
		}
		if c.compact {
			cNames = iu.CompactStrings(names)
		} else {
			cNames = names
		}

		table := iu.NewTableWriter(opts(), "RAFT Meta Group Information")
		table.AddHeaders("Connection Name", "ID", "Leader", "Current", "Online", "Active", "Lag")
		for i, replica := range cluster.Replicas {
			leader := ""
			peer := replica.Peer
			if replica.Name == cluster.Leader {
				leader = "yes"
				peer = cluster.Peer
			}

			online := "true"
			if replica.Offline {
				online = color.New(color.Bold).Sprint("false")
			}

			table.AddRow(cNames[i], peer, leader, replica.Current, online, f(replica.Active), f(replica.Lag))
		}

		fmt.Print(table.Render())
	}

	return nil
}

func (c *SrvReportCmd) reportAccount(_ *fisk.ParseContext) error {
	connz, err := c.getConnz(0, c.nc)
	if err != nil {
		return err
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	if c.account != "" {
		accounts := c.accountInfo(connz)
		if len(accounts) != 1 {
			return fmt.Errorf("received results for multiple accounts, expected %v", c.account)
		}

		account, ok := accounts[c.account]
		if !ok {
			return fmt.Errorf("did not receive any results for account %s", c.account)
		}

		if c.json {
			iu.PrintJSON(account)
			return nil
		}

		if len(account.ConnInfo) > 0 {
			report := account.ConnInfo
			c.renderConnections(report)
		}
		return nil
	}

	accountsMap := c.accountInfo(connz)
	var accounts []*srvReportAccountInfo
	for _, v := range accountsMap {
		accounts = append(accounts, v)
	}

	sort.Slice(accounts, func(i int, j int) bool {
		switch c.sort {
		case "in-bytes":
			return c.boolReverse(accounts[i].InBytes < accounts[j].InBytes)
		case "out-bytes":
			return c.boolReverse(accounts[i].OutBytes < accounts[j].OutBytes)
		case "in-msgs":
			return c.boolReverse(accounts[i].InMsgs < accounts[j].InMsgs)
		case "out-msgs":
			return c.boolReverse(accounts[i].OutMsgs < accounts[j].OutMsgs)
		case "conns":
			return c.boolReverse(accounts[i].Connections < accounts[j].Connections)
		default:
			return c.boolReverse(accounts[i].Subs < accounts[j].Subs)
		}
	})

	if c.topk > 0 && c.topk < len(accounts) {
		if c.reverse {
			accounts = accounts[0:c.topk]
		} else {
			accounts = accounts[len(accounts)-c.topk:]
		}
	}

	if c.json {
		iu.PrintJSON(accounts)
		return nil
	}

	table := iu.NewTableWriter(opts(), fmt.Sprintf("%d Accounts Overview", len(accounts)))
	table.AddHeaders("Account", "Connections", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	for _, acct := range accounts {
		table.AddRow(acct.Account, f(acct.Connections), f(acct.InMsgs), f(acct.OutMsgs), humanize.IBytes(uint64(acct.InBytes)), humanize.IBytes(uint64(acct.OutBytes)), f(acct.Subs))
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())

	return nil
}

func (c *SrvReportCmd) accountInfo(connz connzList) map[string]*srvReportAccountInfo {
	result := make(map[string]*srvReportAccountInfo)

	for _, conn := range connz {
		for _, info := range conn.Data.Conns {
			account, ok := result[info.Account]
			if !ok {
				result[info.Account] = &srvReportAccountInfo{Account: info.Account}
				account = result[info.Account]
			}

			account.ConnInfo = append(account.ConnInfo, connInfo{info, conn.Server})
			account.Connections++
			account.InBytes += info.InBytes
			account.OutBytes += info.OutBytes
			account.InMsgs += info.InMsgs
			account.OutMsgs += info.OutMsgs
			account.Subs += len(info.Subs)

			// make sure we only store one server info per unique server
			found := false
			for _, s := range account.Server {
				if s.ID == conn.Server.ID {
					found = true
					break
				}
			}
			if !found {
				account.Server = append(account.Server, conn.Server)
			}
		}
	}

	return result
}

type connInfo struct {
	*server.ConnInfo
	Info *server.ServerInfo `json:"server"`
}

func (c *SrvReportCmd) reportConnections(_ *fisk.ParseContext) error {
	connz, err := c.getConnz(0, c.nc)
	if err != nil {
		return err
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	conns := connz.flatConnInfo()

	if c.json {
		iu.PrintJSON(conns)
		return nil
	}

	c.renderConnections(conns)

	return nil
}

func (c *SrvReportCmd) boolReverse(v bool) bool {
	if c.reverse {
		return !v
	}

	return v
}

func (c *SrvReportCmd) sortConnections(conns []connInfo) {
	sort.Slice(conns, func(i int, j int) bool {
		switch c.sort {
		case "in-bytes":
			return c.boolReverse(conns[i].InBytes < conns[j].InBytes)
		case "out-bytes":
			return c.boolReverse(conns[i].OutBytes < conns[j].OutBytes)
		case "in-msgs":
			return c.boolReverse(conns[i].InMsgs < conns[j].InMsgs)
		case "out-msgs":
			return c.boolReverse(conns[i].OutMsgs < conns[j].OutMsgs)
		case "uptime":
			return c.boolReverse(conns[i].Start.After(conns[j].Start))
		case "cid":
			return c.boolReverse(conns[i].Cid < conns[j].Cid)
		default:
			return c.boolReverse(len(conns[i].Subs) < len(conns[j].Subs))
		}
	})
}

func (c *SrvReportCmd) renderConnections(report []connInfo) {
	c.sortConnections(report)

	total := len(report)
	limit := total
	if c.topk > 0 && c.topk <= total {
		limit = c.topk
	}

	table := iu.NewTableWriter(opts(), fmt.Sprintf("Top %d Connections out of %s by %s", limit, f(total), c.sort))
	showReason := c.stateFilter == "closed" || c.stateFilter == "all"
	headers := []any{"CID", "Name", "Server", "Cluster", "IP", "Account", "Uptime", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs"}
	if showReason {
		headers = append(headers, "Reason")
	}

	table.AddHeaders(headers...)

	var oMsgs int64
	var iMsgs int64
	var oBytes int64
	var iBytes int64
	var subs uint32

	type srvInfo struct {
		cluster string
		conns   int
	}
	servers := make(map[string]*srvInfo)
	var serverNames []string

	for i, info := range report {
		name := info.Name
		if len(name) == 0 && len(info.MQTTClient) > 0 {
			name = info.MQTTClient
		}
		if len(name) > 40 {
			name = name[:40] + " .."
		}

		oMsgs += info.OutMsgs
		iMsgs += info.InMsgs
		oBytes += info.OutBytes
		iBytes += info.InBytes
		subs += info.NumSubs

		srvName := info.Info.Name
		cluster := info.Info.Cluster

		srv, ok := servers[srvName]
		if !ok {
			servers[srvName] = &srvInfo{cluster, 0}
			srv = servers[srvName]
			serverNames = append(serverNames, srvName)
		}
		srv.conns++

		acc := info.Account
		if len(info.Account) > 46 {
			acc = info.Account[0:12] + " .."
		}

		cid := fmt.Sprintf("%d", info.Cid)
		if info.Kind != "Client" {
			cid = fmt.Sprintf("%s%d", string(info.Kind[0]), info.Cid)
		}

		if i < limit {
			values := []any{cid, name, srvName, cluster, fmt.Sprintf("%s:%d", info.IP, info.Port), acc, info.Uptime, f(info.InMsgs), f(info.OutMsgs), humanize.IBytes(uint64(info.InBytes)), humanize.IBytes(uint64(info.OutBytes)), f(len(info.Subs))}
			if showReason {
				values = append(values, info.Reason)
			}
			table.AddRow(values...)
		}
	}

	if len(report) > 1 {
		values := []any{"", fmt.Sprintf("Totals for %s connections", humanize.Comma(int64(total))), "", "", "", "", "", f(iMsgs), f(oMsgs), humanize.IBytes(uint64(iBytes)), humanize.IBytes(uint64(oBytes)), f(subs)}
		if showReason {
			values = append(values, "")
		}
		table.AddFooter(values...)
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())

	if len(serverNames) > 0 {
		fmt.Println()

		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		table := iu.NewTableWriter(opts(), "Connections per server")
		table.AddHeaders("Server", "Cluster", "Connections")
		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		for _, n := range serverNames {
			table.AddRow(n, servers[n].cluster, servers[n].conns)
		}
		fmt.Print(table.Render())
	}
}

type connzList []*server.ServerAPIConnzResponse

func (c connzList) flatConnInfo() []connInfo {
	var conns []connInfo

	for _, conn := range c {
		for _, c := range conn.Data.Conns {
			conns = append(conns, connInfo{c, conn.Server})
		}
	}

	return conns
}

func parseConnzResp(resp []byte) (*server.ServerAPIConnzResponse, error) {
	reqresp := server.ServerAPIConnzResponse{}

	err := json.Unmarshal(resp, &reqresp)
	if err != nil {
		return nil, err
	}

	if reqresp.Error != nil {
		return nil, fmt.Errorf("invalid response received: %v", reqresp.Error)
	}

	if reqresp.Data == nil {
		return nil, fmt.Errorf("no data received in response: %s", string(resp))
	}

	return &reqresp, nil
}

func (c *SrvReportCmd) getConnz(limit int, nc *nats.Conn) (connzList, error) {
	result := connzList{}
	found := 0

	var program *vm.Program
	var err error
	env := map[string]any{}

	if !c.skipDiscoverClusterSize && c.waitFor == 0 {
		c.waitFor, err = currentActiveServers(nc)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case c.filterReason != "" && c.filterExpression != "":
		return nil, fmt.Errorf("cannot filter for closed reason and use a filter expression at the same time")
	case c.filterReason != "":
		c.filterExpression = fmt.Sprintf("lower(Conn.Reason) matches '%s'", c.filterReason)
		fallthrough
	case c.filterExpression != "":
		program, err = expr.Compile(c.filterExpression, expr.Env(map[string]any{}), expr.AsBool(), expr.AllowUndefinedVariables())
		fisk.FatalIfError(err, "Invalid expression: %v", err)
	}

	removeFilteredConns := func(co *server.ServerAPIConnzResponse) error {
		conns := make([]*server.ConnInfo, len(co.Data.Conns))
		copy(conns, co.Data.Conns)
		co.Data.Conns = []*server.ConnInfo{}
		srv := iu.StructWithoutOmitEmpty(*co.Server)

		for _, conn := range conns {
			env["server"] = srv
			env["Server"] = co.Server
			env["conn"] = iu.StructWithoutOmitEmpty(*conn)
			env["Conn"] = conn

			// backward compat, the `s` here is a mistake
			env["Conns"] = conn
			env["conns"] = env["conn"]

			out, err := expr.Run(program, env)
			if err != nil {
				fisk.FatalIfError(err, "Invalid expression: %v", err)
			}

			should, ok := out.(bool)
			if !ok {
				fisk.FatalIfError(err, "expression did not return a boolean")
			}

			if should {
				co.Data.Conns = append(co.Data.Conns, conn)
			}
		}

		return nil
	}

	state := server.ConnOpen
	switch c.stateFilter {
	case "all":
		state = server.ConnAll
	case "closed":
		state = server.ConnClosed
	}

	offset := 0
	more := false

	req := &server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Subscriptions:       true,
			SubscriptionsDetail: false,
			Username:            true,
			User:                c.user,
			Account:             c.account,
			State:               state,
			FilterSubject:       c.subject,
			Limit:               1024,
			Offset:              offset,
		},
		EventFilterOptions: c.reqFilter(),
	}
	results, err := doReq(req, "$SYS.REQ.SERVER.PING.CONNZ", c.waitFor, nc)
	if err != nil {
		return nil, err
	}

	for _, res := range results {
		co, err := parseConnzResp(res)
		if err != nil {
			return nil, err
		}
		found += len(co.Data.Conns)

		if c.filterExpression != "" {
			err = removeFilteredConns(co)
			if err != nil {
				return nil, err
			}
		}

		if len(co.Data.Conns) > 0 {
			result = append(result, co)
		}
	}

	if limit != 0 && found > limit {
		return result[:limit], nil
	}

	for _, conn := range result {
		if conn.Data.Offset+conn.Data.Limit < conn.Data.Total {
			more = true
		}
	}

	if more && !c.json {
		fmt.Printf("Gathering paged connection information")
	}

	for {
		if !more {
			break
		}

		if limit != 0 && found > limit {
			break
		}

		// Show visual progress if JSON is not requested.
		if !c.json {
			fmt.Print(".")
		}

		offset += 1025

		// get on offset
		// iterate and add to results
		req := &server.ConnzEventOptions{
			ConnzOptions: server.ConnzOptions{
				Subscriptions:       true,
				SubscriptionsDetail: false,
				Username:            true,
				User:                c.user,
				Account:             c.account,
				State:               state,
				FilterSubject:       c.subject,
				Limit:               1024,
				Offset:              offset,
			},
			EventFilterOptions: c.reqFilter(),
		}

		res, err := doReq(req, "$SYS.REQ.SERVER.PING.CONNZ", c.waitFor, nc)
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
		} else if err != nil {
			return nil, err
		}

		more = false

		for _, res := range res {
			co, err := parseConnzResp(res)
			if err != nil {
				return nil, err
			}
			found += len(co.Data.Conns)

			if len(co.Data.Conns) == 0 {
				continue
			}

			if c.filterExpression != "" {
				err = removeFilteredConns(co)
				if err != nil {
					return nil, err
				}
			}

			result = append(result, co)

			if !more && co.Data.Offset+co.Data.Limit < co.Data.Total {
				more = true
			}
		}
	}

	if !c.json {
		fmt.Println()
	}

	if limit > 0 {
		result = result[:limit]
	}

	return result, nil
}

func (c *SrvReportCmd) isFiltered() bool {
	return c.server != "" || len(c.tags) > 0 || c.cluster != ""
}

func (c *SrvReportCmd) reqFilter() server.EventFilterOptions {
	return server.EventFilterOptions{
		Domain:  opts().Config.JSDomain(),
		Name:    c.server,
		Cluster: c.cluster,
		Tags:    c.tags,
	}
}

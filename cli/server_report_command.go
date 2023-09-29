// Copyright 2020-2022 The NATS Authors
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
	"sort"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type SrvReportCmd struct {
	json bool

	filterExpression string
	account          string
	user             string
	waitFor          int
	sort             string
	topk             int
	reverse          bool
	compact          bool
	subject          string
	server           string
	cluster          string
	tags             []string
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
	}

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.reportConnections)
	conns.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	conns.Flag("account", "Limit report to a specific account").StringVar(&c.account)
	addFilterOpts(conns)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	conns.Flag("subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subject)
	conns.Flag("username", "Limits responses only to those connections for a specific authentication username").StringVar(&c.user)
	conns.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	conns.Flag("filter", "Expression based filter for connections").StringVar(&c.filterExpression)

	acct := report.Command("accounts", "Report on account activity").Alias("acct").Action(c.reportAccount)
	acct.Arg("account", "Account to produce a report for").StringVar(&c.account)
	acct.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	addFilterOpts(acct)
	acct.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,conns,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "conns", "subs")
	acct.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	acct.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	jsz := report.Command("jetstream", "Report on JetStream activity").Alias("jsz").Alias("js").Action(c.reportJetStream)
	jsz.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	addFilterOpts(jsz)
	jsz.Flag("account", "Produce the report for a specific account").StringVar(&c.account)
	jsz.Flag("sort", "Sort by a specific property (name,cluster,streams,consumers,msgs,mbytes,mem,file,api,err").Default("cluster").EnumVar(&c.sort, "name", "cluster", "streams", "consumers", "msgs", "mbytes", "bytes", "mem", "file", "store", "api", "err")
	jsz.Flag("compact", "Compact server names").Default("true").BoolVar(&c.compact)
}

func (c *SrvReportCmd) reportJetStream(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	jszOpts := server.JSzOptions{}
	if c.account != "" {
		jszOpts.Account = c.account
		jszOpts.Streams = true
		jszOpts.Consumer = true
		jszOpts.Limit = 10000
	}

	req := &server.JszEventOptions{JSzOptions: jszOpts, EventFilterOptions: c.reqFilter()}
	res, err := doReq(req, "$SYS.REQ.SERVER.PING.JSZ", c.waitFor, nc)
	if err != nil {
		return err
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	var (
		names               []string
		jszResponses        []*jszr
		apiErr              uint64
		apiTotal            uint64
		memory              uint64
		store               uint64
		consumers           int
		streams             int
		bytes               uint64
		msgs                uint64
		cluster             *server.MetaClusterInfo
		expectedClusterSize int
	)

	renderDomain := false
	for _, r := range res {
		response := jszr{}

		err = json.Unmarshal(r, &response)
		if err != nil {
			return err
		}

		if response.Data.Config.Domain != "" {
			renderDomain = true
		}

		jszResponses = append(jszResponses, &response)
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
		cNames = compactStrings(names)
	} else {
		cNames = names
	}

	var table *tbl
	if c.account != "" {
		table = newTableWriter(fmt.Sprintf("JetStream Summary for Account %s", c.account))
	} else {
		table = newTableWriter("JetStream Summary")
	}

	if renderDomain {
		table.AddHeaders("Server", "Cluster", "Domain", "Streams", "Consumers", "Messages", "Bytes", "Memory", "File", "API Req", "API Err")
	} else {
		table.AddHeaders("Server", "Cluster", "Streams", "Consumers", "Messages", "Bytes", "Memory", "File", "API Req", "API Err")
	}

	for i, js := range jszResponses {
		jss := js.Data.JetStreamStats
		var acc *server.AccountDetail
		var doAccountStats bool

		if c.account != "" && len(js.Data.AccountDetails) == 1 {
			acc = js.Data.AccountDetails[0]
			jss = acc.JetStreamStats
			doAccountStats = true
		}

		apiErr += jss.API.Errors
		apiTotal += jss.API.Total
		memory += jss.Memory
		store += jss.Store

		rStreams := 0
		rConsumers := 0
		rMessages := uint64(0)
		rBytes := uint64(0)

		if doAccountStats {
			rBytes = acc.Memory + acc.Store
			bytes += rBytes
			rStreams = len(acc.Streams)
			streams += rStreams

			for _, sd := range acc.Streams {
				consumers += sd.State.Consumers
				rConsumers += sd.State.Consumers
				msgs += sd.State.Msgs
				rMessages += sd.State.Msgs
			}
		} else {
			consumers += js.Data.Consumers
			rConsumers = js.Data.Consumers
			streams += js.Data.Streams
			rStreams = js.Data.Streams
			bytes += js.Data.Bytes
			rBytes = js.Data.Bytes
			msgs += js.Data.Messages
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

		table.AddRow(row...)
	}

	row := []any{"", ""}
	if renderDomain {
		row = append(row, "")
	}
	row = append(row, f(streams), f(consumers), f(msgs), humanize.IBytes(bytes), humanize.IBytes(memory), humanize.IBytes(store), f(apiTotal), f(apiErr))
	table.AddFooter(row...)

	fmt.Print(table.Render())
	fmt.Println()

	switch {
	case c.isFiltered():
	case len(jszResponses) > 0 && cluster == nil:
		fmt.Println()
		fmt.Printf("WARNING: No cluster meta leader found. The cluster expects %d nodes but only %d responded. JetStream operation require at least %d up nodes.", expectedClusterSize, len(jszResponses), expectedClusterSize/2+1)
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
			cNames = compactStrings(names)
		} else {
			cNames = names
		}

		table := newTableWriter("RAFT Meta Group Information")
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
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	connz, err := c.getConnz(0, nc)
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
			printJSON(account)
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
			accounts = accounts[len(accounts)-c.topk:]
		} else {
			accounts = accounts[0:c.topk]
		}
	}

	if c.json {
		printJSON(accounts)
		return nil
	}

	table := newTableWriter(fmt.Sprintf("%d Accounts Overview", len(accounts)))
	table.AddHeaders("Account", "Connections", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	for _, acct := range accounts {
		table.AddRow(acct.Account, f(acct.Connections), f(acct.InMsgs), f(acct.OutMsgs), humanize.IBytes(uint64(acct.InBytes)), humanize.IBytes(uint64(acct.OutBytes)), f(acct.Subs))
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
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	connz, err := c.getConnz(0, nc)
	if err != nil {
		return err
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	conns := connz.flatConnInfo()

	if c.json {
		printJSON(conns)
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

	table := newTableWriter(fmt.Sprintf("Top %d Connections out of %s by %s", limit, f(total), c.sort))
	table.AddHeaders("CID", "Name", "Server", "Cluster", "IP", "Account", "Uptime", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

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
		if len(info.Name) > 40 {
			name = info.Name[:40] + " .."
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
			table.AddRow(cid, name, srvName, cluster, fmt.Sprintf("%s:%d", info.IP, info.Port), acc, info.Uptime, f(info.InMsgs), f(info.OutMsgs), humanize.IBytes(uint64(info.InBytes)), humanize.IBytes(uint64(info.OutBytes)), f(len(info.Subs)))
		}
	}

	if len(report) > 1 {
		table.AddFooter("", fmt.Sprintf("Totals for %s connections", humanize.Comma(int64(total))), "", "", "", "", "", f(iMsgs), f(oMsgs), humanize.IBytes(uint64(iBytes)), humanize.IBytes(uint64(oBytes)), f(subs))
	}

	fmt.Print(table.Render())

	if len(serverNames) > 0 {
		fmt.Println()

		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		table := newTableWriter("Connections per server")
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

	if c.filterExpression != "" {
		program, err = expr.Compile(c.filterExpression, expr.Env(map[string]any{}), expr.AsBool(), expr.AllowUndefinedVariables())
		fisk.FatalIfError(err, "Invalid expression: %v", err)
	}

	removeFilteredConns := func(co *server.ServerAPIConnzResponse) error {
		conns := make([]*server.ConnInfo, len(co.Data.Conns))
		copy(conns, co.Data.Conns)
		co.Data.Conns = []*server.ConnInfo{}
		srv := structWithoutOmitEmpty(*co.Server)

		for _, conn := range conns {
			env["server"] = srv
			env["Server"] = co.Server
			env["conns"] = structWithoutOmitEmpty(*conn)
			env["Conns"] = conn

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

	req := &server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Subscriptions:       true,
			SubscriptionsDetail: false,
			Username:            true,
			User:                c.user,
			Account:             c.account,
			FilterSubject:       c.subject,
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

	offset := 0
	for _, conn := range result {
		if conn.Data.Offset+conn.Data.Limit < conn.Data.Total {
			offset = conn.Data.Offset + conn.Data.Limit + 1
			break
		}
	}

	if offset > 0 && !c.json {
		fmt.Print("Gathering paged connection information")
	}

	for {
		if offset <= 0 {
			break
		}

		if limit != 0 && found > limit {
			break
		}

		// Show visual progress if JSON is not requested.
		if !c.json {
			fmt.Print(".")
		}

		// get on offset
		// iterate and add to results
		req := &server.ConnzEventOptions{
			ConnzOptions: server.ConnzOptions{
				Subscriptions:       true,
				SubscriptionsDetail: false,
				Account:             c.account,
				Username:            true,
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

		offset = 0

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

			if co.Data.Offset+co.Data.Limit < co.Data.Total {
				offset = co.Data.Offset + co.Data.Limit + 1
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
		Domain:  opts.Config.JSDomain(),
		Name:    c.server,
		Cluster: c.cluster,
		Tags:    c.tags,
	}
}

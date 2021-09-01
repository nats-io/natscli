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
	"sort"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/xlab/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvReportCmd struct {
	json bool

	account string
	waitFor int
	sort    string
	topk    int
	reverse bool
	compact bool
	subject string
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

func configureServerReportCommand(srv *kingpin.CmdClause) {
	c := &SrvReportCmd{}

	report := srv.Command("report", "Report on various server metrics").Alias("rep")
	report.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	report.Flag("reverse", "Reverse sort connections").Short('R').Default("true").BoolVar(&c.reverse)

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.reportConnections)
	conns.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	conns.Flag("account", "Limit report to a specific account").StringVar(&c.account)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	conns.Flag("subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subject)

	acct := report.Command("accounts", "Report on account activity").Alias("acct").Action(c.reportAccount)
	acct.Arg("account", "Account to produce a report for").StringVar(&c.account)
	acct.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	acct.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,conns,subs,uptime,cid)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "conns", "subs", "uptime", "cid")
	acct.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)

	jsz := report.Command("jetstream", "Report on JetStream activity").Alias("jsz").Alias("js").Action(c.reportJetStream)
	jsz.Arg("limit", "Limit the responses to a certain amount of servers").IntVar(&c.waitFor)
	jsz.Flag("account", "Produce the report for a specific account").StringVar(&c.account)
	jsz.Flag("sort", "Sort by a specific property (name,cluster,streams,consumers,msgs,mbytes,mem,file,api,err").Default("cluster").EnumVar(&c.sort, "name", "cluster", "streams", "consumers", "msgs", "mbytes", "bytes", "mem", "file", "store", "api", "err")
	jsz.Flag("compact", "Compact server names").Default("true").BoolVar(&c.compact)
}

func (c *SrvReportCmd) reportJetStream(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	req := &server.JszEventOptions{JSzOptions: server.JSzOptions{Account: c.account}, EventFilterOptions: c.reqFilter()}
	res, err := doReq(req, "$SYS.REQ.SERVER.PING.JSZ", c.waitFor, nc)
	if err != nil {
		return err
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	var (
		names        []string
		jszResponses []*jszr
		apiErr       uint64
		apiTotal     uint64
		memory       uint64
		store        uint64
		consumers    int
		streams      int
		bytes        uint64
		msgs         uint64
		cluster      *server.ClusterInfo
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
			return c.boolReverse(jszResponses[i].Data.Streams < jszResponses[j].Data.Streams)
		case "consumers":
			return c.boolReverse(jszResponses[i].Data.Consumers < jszResponses[j].Data.Consumers)
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
			return c.boolReverse(jszResponses[i].Server.Cluster < jszResponses[j].Server.Cluster)
		}
	})

	if len(jszResponses) == 0 {
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}

	// here so its after the sort
	for _, js := range jszResponses {
		names = append(names, js.Server.Name)
	}
	var cNames []string
	if c.compact {
		cNames = compactStrings(names)
	} else {
		cNames = names
	}

	var table *tablewriter.Table
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
		apiErr += js.Data.JetStreamStats.API.Errors
		apiTotal += js.Data.JetStreamStats.API.Total
		memory += js.Data.JetStreamStats.Memory
		store += js.Data.JetStreamStats.Store
		consumers += js.Data.Consumers
		streams += js.Data.Streams
		bytes += js.Data.Bytes
		msgs += js.Data.Messages

		leader := ""
		if js.Data.Meta != nil && js.Data.Meta.Leader == js.Server.Name {
			leader = "*"
			cluster = js.Data.Meta
		}

		row := []interface{}{cNames[i] + leader, js.Server.Cluster}
		if renderDomain {
			row = append(row, js.Data.Config.Domain)
		}
		row = append(row,
			humanize.Comma(int64(js.Data.Streams)),
			humanize.Comma(int64(js.Data.Consumers)),
			humanize.Comma(int64(js.Data.Messages)),
			humanize.IBytes(js.Data.Bytes),
			humanize.IBytes(js.Data.JetStreamStats.Memory),
			humanize.IBytes(js.Data.JetStreamStats.Store),
			humanize.Comma(int64(js.Data.JetStreamStats.API.Total)),
			humanize.Comma(int64(js.Data.JetStreamStats.API.Errors)))

		table.AddRow(row...)
	}

	table.AddSeparator()
	row := []interface{}{"", ""}
	if renderDomain {
		row = append(row, "")
	}
	row = append(row, humanize.Comma(int64(streams)), humanize.Comma(int64(consumers)), humanize.Comma(int64(msgs)), humanize.IBytes(bytes), humanize.IBytes(memory), humanize.IBytes(store), humanize.Comma(int64(apiTotal)), humanize.Comma(int64(apiErr)))
	table.AddRow(row...)

	fmt.Print(table.Render())
	fmt.Println()

	if cluster != nil {
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
		table.AddHeaders("Name", "Leader", "Current", "Online", "Active", "Lag")
		for i, replica := range cluster.Replicas {
			leader := ""
			if replica.Name == cluster.Leader {
				leader = "yes"
			}

			online := "true"
			if replica.Offline {
				online = color.New(color.Bold).Sprint("false")
			}

			table.AddRow(cNames[i], leader, replica.Current, online, humanizeDuration(replica.Active), humanize.Comma(int64(replica.Lag)))
		}
		fmt.Print(table.Render())
	}

	return nil
}

func (c *SrvReportCmd) reportAccount(_ *kingpin.ParseContext) error {
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
			if c.topk > 0 && c.topk <= len(account.ConnInfo) {
				report = account.ConnInfo[len(account.ConnInfo)-c.topk:]
			}

			c.sortConnections(report)
			c.renderConnections(int64(len(account.ConnInfo)), report)
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

	if c.json {
		printJSON(accounts)
		return nil
	}

	table := newTableWriter(fmt.Sprintf("%d Accounts Overview", len(accounts)))
	table.AddHeaders("Account", "Connections", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	for _, acct := range accounts {
		table.AddRow(acct.Account, humanize.Comma(int64(acct.Connections)), humanize.Comma(acct.InMsgs), humanize.Comma(acct.OutMsgs), humanize.IBytes(uint64(acct.InBytes)), humanize.IBytes(uint64(acct.OutBytes)), humanize.Comma(int64(acct.Subs)))
	}

	fmt.Print(table.Render())

	return nil
}

func (c *SrvReportCmd) accountInfo(connz connzList) map[string]*srvReportAccountInfo {
	result := make(map[string]*srvReportAccountInfo)

	for _, conn := range connz {
		for _, info := range conn.Connz.Conns {
			account, ok := result[info.Account]
			if !ok {
				result[info.Account] = &srvReportAccountInfo{Account: info.Account}
				account = result[info.Account]
			}

			account.ConnInfo = append(account.ConnInfo, connInfo{info, conn.ServerInfo})
			account.Connections++
			account.InBytes += info.InBytes
			account.OutBytes += info.OutBytes
			account.InMsgs += info.InMsgs
			account.OutMsgs += info.OutMsgs
			account.Subs += len(info.Subs)

			// make sure we only store one server info per unique server
			found := false
			for _, s := range account.Server {
				if s.ID == conn.ServerInfo.ID {
					found = true
					break
				}
			}
			if !found {
				account.Server = append(account.Server, conn.ServerInfo)
			}
		}
	}

	return result
}

type connInfo struct {
	*server.ConnInfo
	Info *server.ServerInfo `json:"server"`
}

func (c *SrvReportCmd) reportConnections(_ *kingpin.ParseContext) error {
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

	c.sortConnections(conns)

	report := conns
	if c.topk > 0 && c.topk <= len(conns) {
		if c.reverse {
			report = conns[len(conns)-c.topk:]
		} else {
			report = conns[:c.topk]
		}
	}

	if c.json {
		printJSON(report)
		return nil
	}

	c.renderConnections(int64(len(conns)), report)

	return nil
}

func (c *SrvReportCmd) boolReverse(v bool) bool {
	if !c.reverse {
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

func (c *SrvReportCmd) renderConnections(total int64, report []connInfo) {
	table := newTableWriter(fmt.Sprintf("Top %d Connections out of %s by %s", len(report), humanize.Comma(total), c.sort))
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

	for _, info := range report {
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
			servers[srvName] = &srvInfo{srvName, 0}
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

		table.AddRow(cid, name, srvName, cluster, info.IP, acc, info.Uptime, humanize.Comma(info.InMsgs), humanize.Comma(info.OutMsgs), humanize.IBytes(uint64(info.InBytes)), humanize.IBytes(uint64(info.OutBytes)), len(info.Subs))
	}

	if len(report) > 1 {
		table.AddSeparator()
		table.AddRow("", "", "", "", "", "", "", humanize.Comma(iMsgs), humanize.Comma(oMsgs), humanize.IBytes(uint64(iBytes)), humanize.IBytes(uint64(oBytes)), humanize.Comma(int64(subs)))
	}

	fmt.Print(table.Render())

	if len(serverNames) > 0 {
		fmt.Println()

		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		table := newTableWriter("Connections per server")
		table.AddHeaders("Server", "Cluster", "Connections")
		for _, n := range serverNames {
			table.AddRow(n, servers[n].cluster, servers[n].conns)
		}
		fmt.Print(table.Render())
	}
}

type connz struct {
	Connz      *server.Connz
	ServerInfo *server.ServerInfo
}

type connzList []connz

func (c connzList) flatConnInfo() []connInfo {
	var conns []connInfo
	for _, conn := range c {
		for _, c := range conn.Connz.Conns {
			conns = append(conns, connInfo{c, conn.ServerInfo})
		}
	}
	return conns
}

func parseConnzResp(resp []byte) (connz, error) {
	reqresp := map[string]json.RawMessage{}

	err := json.Unmarshal(resp, &reqresp)
	if err != nil {
		return connz{}, err
	}

	errresp, ok := reqresp["error"]
	if ok {
		return connz{}, fmt.Errorf("invalid response received: %#v", errresp)
	}

	data, ok := reqresp["data"]
	if !ok {
		return connz{}, fmt.Errorf("no data received in response: %#v", reqresp)
	}

	c := connz{
		Connz:      &server.Connz{},
		ServerInfo: &server.ServerInfo{},
	}

	s, ok := reqresp["server"]
	if !ok {
		return connz{}, fmt.Errorf("no server data received in response: %#v", reqresp)
	}
	err = json.Unmarshal(s, c.ServerInfo)
	if err != nil {
		return connz{}, err
	}

	err = json.Unmarshal(data, c.Connz)
	if err != nil {
		return connz{}, err
	}
	return c, nil
}

func (c *SrvReportCmd) getConnz(limit int, nc *nats.Conn) (connzList, error) {
	result := connzList{}
	found := 0

	req := &server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Subscriptions:       true,
			SubscriptionsDetail: false,
			Username:            true,
			Account:             c.account,
			FilterSubject:       c.subject,
		},
		EventFilterOptions: c.reqFilter(),
	}
	res, err := doReq(req, "$SYS.REQ.SERVER.PING.CONNZ", c.waitFor, nc)
	if err != nil {
		return nil, err
	}

	for _, c := range res {
		co, err := parseConnzResp(c)
		if err != nil {
			return nil, err
		}
		result = append(result, co)
		found += len(co.Connz.Conns)
	}

	if limit != 0 && found > limit {
		return result[:limit], nil
	}

	incomplete := []connz{}
	for _, con := range result {
		if con.Connz.Offset+con.Connz.Limit < con.Connz.Total {
			incomplete = append(incomplete, con)
		}
	}

	if len(incomplete) > 0 && !c.json {
		fmt.Print("Gathering paged connection information")
	}

	for {
		if len(incomplete) == 0 {
			break
		}

		if limit != 0 && found > limit {
			break
		}

		fmt.Print(".")

		getList := incomplete[0:]
		incomplete = []connz{}

		for _, conn := range getList {
			req := &server.ConnzEventOptions{
				ConnzOptions: server.ConnzOptions{
					Subscriptions:       true,
					SubscriptionsDetail: false,
					Account:             c.account,
					Username:            true,
					Offset:              conn.Connz.Offset + conn.Connz.Limit + 1,
				},
				EventFilterOptions: c.reqFilter(),
			}

			res, err := doReq(req, fmt.Sprintf("$SYS.REQ.SERVER.%s.CONNZ", conn.Connz.ID), 1, nc)
			if err == nats.ErrNoResponders {
				return nil, fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
			} else if err != nil {
				return nil, err
			}

			if len(res) != 1 {
				return nil, fmt.Errorf("received %d responses from server %s expcting exactly 1", len(res), conn.Connz.ID)
			}

			for _, c := range res {
				co, err := parseConnzResp(c)
				if err != nil {
					return nil, err
				}
				result = append(result, co)
				found += len(co.Connz.Conns)

				if co.Connz.Offset+co.Connz.Limit < co.Connz.Total {
					incomplete = append(incomplete, co)
				}
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

func (c *SrvReportCmd) reqFilter() server.EventFilterOptions {
	return server.EventFilterOptions{Domain: config.JSDomain()}
}

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
	"log"
	"sort"
	"sync"

	"github.com/dustin/go-humanize"
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
}

type srvReportAccountInfo struct {
	Account     string             `json:"account"`
	Connections int                `json:"connections"`
	ConnInfo    []*server.ConnInfo `json:"connection_info"`
	InMsgs      int64              `json:"in_msgs"`
	OutMsgs     int64              `json:"out_msgs"`
	InBytes     int64              `json:"in_bytes"`
	OutBytes    int64              `json:"out_bytes"`
	Subs        int                `json:"subscriptions"`
}

func configureServerReportCommand(srv *kingpin.CmdClause) {
	c := &SrvReportCmd{}

	report := srv.Command("report", "Report on various server metrics").Alias("rep")
	report.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.reportConnections)
	conns.Arg("limit", "Limit the responses to a certain amount of servers").Default("1024").IntVar(&c.waitFor)
	conns.Flag("account", "Limit report to a specific account").StringVar(&c.account)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").IntVar(&c.topk)

	acct := report.Command("accounts", "Report on account activity").Alias("acct").Action(c.reportAccount)
	acct.Arg("account", "Account to produce a report for").StringVar(&c.account)
	acct.Arg("limit", "Limit the responses to a certain amount of servers").Default("1024").IntVar(&c.waitFor)
	acct.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,conns,subs,uptime,cid)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "conns", "subs", "uptime", "cid")
	acct.Flag("top", "Limit results to the top results").IntVar(&c.topk)
}

func (c *SrvReportCmd) reportAccount(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	connz, all, err := c.getConnz(nil, nc, 0)
	if err != nil {
		return err
	}

	if !all {
		return fmt.Errorf("expected all servers but did not fully converge")
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	if c.account != "" {
		accounts := c.accountInfo(connz)
		if len(accounts) != 1 {
			return fmt.Errorf("received results for multiple accounts")
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
			return accounts[i].InBytes < accounts[j].InBytes
		case "out-bytes":
			return accounts[i].OutBytes < accounts[j].OutBytes
		case "in-msgs":
			return accounts[i].InMsgs < accounts[j].InMsgs
		case "out-msgs":
			return accounts[i].OutMsgs < accounts[j].OutMsgs
		case "conns":
			return accounts[i].Connections < accounts[j].Connections
		default:
			return accounts[i].Subs < accounts[j].Subs
		}
	})

	if c.json {
		printJSON(accounts)
		return nil
	}

	table := tablewriter.CreateTable()
	table.AddTitle(fmt.Sprintf("%d Accounts Overview", len(accounts)))
	table.AddHeaders("Account", "Connections", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	for _, acct := range accounts {
		table.AddRow(acct.Account, humanize.Comma(int64(acct.Connections)), humanize.Comma(acct.InMsgs), humanize.Comma(acct.OutMsgs), humanize.IBytes(uint64(acct.InBytes)), humanize.IBytes(uint64(acct.OutBytes)), humanize.Comma(int64(acct.Subs)))
	}

	fmt.Print(table.Render())

	return nil
}

func (c *SrvReportCmd) accountInfo(connz []*server.Connz) map[string]*srvReportAccountInfo {
	result := make(map[string]*srvReportAccountInfo)

	for _, conn := range connz {
		for _, info := range conn.Conns {
			account, ok := result[info.Account]
			if !ok {
				result[info.Account] = &srvReportAccountInfo{Account: info.Account}
				account = result[info.Account]
			}

			account.ConnInfo = append(account.ConnInfo, info)
			account.Connections++
			account.InBytes += info.InBytes
			account.OutBytes += info.OutBytes
			account.InMsgs += info.InMsgs
			account.OutMsgs += info.OutMsgs
			account.Subs += len(info.Subs)
		}
	}

	return result
}

func (c *SrvReportCmd) reportConnections(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	connz, all, err := c.getConnz(nil, nc, 0)
	if err != nil {
		return err
	}

	if !all {
		return fmt.Errorf("expected all servers but did not fully converge")
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	var conns []*server.ConnInfo

	for _, conn := range connz {
		conns = append(conns, conn.Conns...)
	}

	c.sortConnections(conns)

	report := conns
	if c.topk > 0 && c.topk <= len(conns) {
		report = conns[len(conns)-c.topk:]
	}

	if c.json {
		printJSON(report)
		return nil
	}

	c.renderConnections(report)

	return nil
}

func (c *SrvReportCmd) sortConnections(conns []*server.ConnInfo) {
	sort.Slice(conns, func(i int, j int) bool {
		switch c.sort {
		case "in-bytes":
			return conns[i].InBytes < conns[j].InBytes
		case "out-bytes":
			return conns[i].OutBytes < conns[j].OutBytes
		case "in-msgs":
			return conns[i].InMsgs < conns[j].InMsgs
		case "out-msgs":
			return conns[i].OutMsgs < conns[j].OutMsgs
		case "uptime":
			return conns[i].Start.After(conns[j].Start)
		case "cid":
			return conns[i].Cid < conns[j].Cid
		default:
			return len(conns[i].Subs) < len(conns[j].Subs)
		}
	})
}

func (c *SrvReportCmd) renderConnections(report []*server.ConnInfo) {
	table := tablewriter.CreateTable()
	table.AddTitle(fmt.Sprintf("%d Connections Overview", len(report)))
	table.AddHeaders("CID", "Name", "IP", "Account", "Uptime", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	if c.json {
		printJSON(report)
		return
	}

	var oMsgs int64
	var iMsgs int64
	var oBytes int64
	var iBytes int64
	var subs uint32

	for _, info := range report {
		name := info.Name
		if len(info.Name) > 30 {
			name = info.Name[:30] + "..."
		}

		oMsgs += info.OutMsgs
		iMsgs += info.InMsgs
		oBytes += info.OutBytes
		iBytes += info.InBytes
		subs += info.NumSubs

		table.AddRow(info.Cid, name, info.IP, info.Account, info.Uptime, humanize.Comma(info.InMsgs), humanize.Comma(info.OutMsgs), humanize.IBytes(uint64(info.InBytes)), humanize.IBytes(uint64(info.OutBytes)), len(info.Subs))
	}

	if len(report) > 1 {
		table.AddSeparator()
		table.AddRow("", "", "", "", "", humanize.Comma(iMsgs), humanize.Comma(oMsgs), humanize.IBytes(uint64(iBytes)), humanize.IBytes(uint64(oBytes)), humanize.Comma(int64(subs)))
	}

	fmt.Print(table.Render())
}

// recursively fetches connz from the fleet till all paged results is complete
func (c *SrvReportCmd) getConnz(current []*server.Connz, nc *nats.Conn, level int) (result []*server.Connz, all bool, err error) {
	// warn every 5 levels that we're still recursing...
	if level != 0 && level%5 == 0 {
		log.Printf("Recusring into %d servers to resolve all pages of connection info", len(current))
	}

	if current == nil {
		current = []*server.Connz{}
	}

	// get the initial result from all nodes as one req and then recursively fetch all that has more pages
	if len(current) == 0 {
		res, err := c.doReq(&server.ConnzOptions{
			Subscriptions:       true,
			SubscriptionsDetail: false,
			Username:            true,
			Account:             c.account,
		}, "$SYS.REQ.SERVER.PING.CONNZ", nc)
		if err != nil {
			return nil, false, err
		}

		for _, c := range res {
			reqresp := map[string]json.RawMessage{}

			err = json.Unmarshal(c, &reqresp)
			if err != nil {
				return nil, false, err
			}

			errresp, ok := reqresp["error"]
			if ok {
				return nil, false, fmt.Errorf("invalid response received: %#v", errresp)
			}

			data, ok := reqresp["data"]
			if !ok {
				return nil, false, fmt.Errorf("no data received in response: %#v", reqresp)
			}

			connz := &server.Connz{}
			err = json.Unmarshal(data, &connz)
			if err != nil {
				return nil, false, err
			}

			result = append(result, connz)
		}

		// recursively fetch...
		recursed, all, err := c.getConnz(result, nc, level+1)
		if !all {
			return nil, false, fmt.Errorf("could not fetch all connections")
		}

		result = append(result, recursed...)
		return result, all, err
	}

	// find ones in the current list that's incomplete
	var incomplete []*server.Connz
	for _, conn := range current {
		if (conn.Offset+1)*conn.Limit < conn.Total {
			incomplete = append(incomplete, conn)
		}
	}

	// no more to fetch, we have some results already and none are incomplete
	if len(current) > 0 && len(incomplete) == 0 {
		return current, true, nil
	}

	// we have some incomplete ones, and it's not the entire set, recuse into the subset that need resolving
	if len(incomplete) > 0 && len(incomplete) != len(current) {
		new, all, err := c.getConnz(incomplete, nc, level+1)
		if err != nil {
			return nil, false, err
		}

		if !all {
			return nil, false, fmt.Errorf("could not fetch all connections")
		}

		result = append(result, new...)

		return result, true, nil
	}

	// We are here because we have only incomplete ones in the current set, get their next page and recurse
	if len(incomplete) == len(current) {
		for _, conn := range current {
			res, err := c.doReq(&server.ConnzOptions{
				Subscriptions:       true,
				SubscriptionsDetail: false,
				Account:             c.account,
				Username:            true,
				Offset:              conn.Offset + 1,
			}, fmt.Sprintf("$SYS.REQ.SERVER.%s.CONNZ", conn.ID), nc)
			if err != nil {
				return nil, false, err
			}

			for _, c := range res {
				connz := &server.Connz{}
				err = json.Unmarshal(c, connz)
				if err != nil {
					return nil, false, err
				}

				result = append(result, connz)
			}
		}

		return c.getConnz(result, nc, level+1)
	}

	// we shouldnt get here
	return nil, false, fmt.Errorf("unexpected error resolving connections")
}

func (c *SrvReportCmd) doReq(req interface{}, subj string, nc *nats.Conn) ([][]byte, error) {
	jreq, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return nil, err
	}

	if trace {
		log.Printf(">>> %s: %s\n", subj, string(jreq))
	}

	var resp [][]byte
	var mu sync.Mutex
	ctr := 0

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sub, err := nc.Subscribe(nats.NewInbox(), func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		resp = append(resp, m.Data)
		ctr++

		if ctr == c.waitFor {
			cancel()
		}
	})
	if err != nil {
		return nil, err
	}

	sub.AutoUnsubscribe(c.waitFor)

	err = nc.PublishRequest(subj, sub.Subject, jreq)
	if err != nil {
		return nil, err
	}

	<-ctx.Done()

	return resp, nil
}

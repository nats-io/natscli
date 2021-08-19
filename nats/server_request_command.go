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
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvRequestCmd struct {
	name    string
	host    string
	cluster string
	account string
	tags    []string

	limit   int
	offset  int
	waitFor uint32

	includeAccounts  bool
	includeStreams   bool
	includeConsumers bool
	includeConfig    bool
	leaderOnly       bool
	includeAll       bool

	detail        bool
	sortOpt       string
	cidFilter     uint64
	stateFilter   string
	userFilter    string
	accountFilter string
	subjectFilter string
	nameFilter    string
}

func configureServerRequestCommand(srv *kingpin.CmdClause) {
	c := &SrvRequestCmd{}

	req := srv.Command("request", "Request monitoring data from a specific server").Alias("req")
	req.Flag("limit", "Limit the responses to a certain amount of records").Default("2048").IntVar(&c.limit)
	req.Flag("offset", "Start at a certain record").Default("0").IntVar(&c.offset)
	req.Flag("name", "Limit to servers matching a server name").StringVar(&c.name)
	req.Flag("host", "Limit to servers matching a server host name").StringVar(&c.host)
	req.Flag("cluster", "Limit to servers matching a cluster name").StringVar(&c.cluster)
	req.Flag("tags", "Limit to servers with these configured tags").StringsVar(&c.tags)

	subz := req.Command("subscriptions", "Show subscription information").Alias("sub").Alias("subsz").Action(c.subs)
	subz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	subz.Flag("detail", "Include detail about all subscriptions").Default("false").BoolVar(&c.detail)
	subz.Flag("filter-account", "Filter on a specific account").StringVar(&c.accountFilter)

	varz := req.Command("variables", "Show runtime variables").Alias("var").Alias("varz").Action(c.varz)
	varz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)

	connz := req.Command("connections", "Show connection details").Alias("conn").Alias("connz").Action(c.conns)
	connz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	connz.Flag("sort", "Sort by a specific property").Default("cid").EnumVar(&c.sortOpt, "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason")
	connz.Flag("subscriptions", "Show subscriptions").Default("false").BoolVar(&c.detail)
	connz.Flag("filter-cid", "Filter on a specific CID").Uint64Var(&c.cidFilter)
	connz.Flag("filter-state", "Filter on a specific account state (open, closed, all)").Default("open").EnumVar(&c.stateFilter, "open", "closed", "all")
	connz.Flag("filter-user", "Filter on a specific username").StringVar(&c.userFilter)
	connz.Flag("filter-account", "Filter on a specific account").StringVar(&c.accountFilter)
	connz.Flag("filter-subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subjectFilter)

	routez := req.Command("routes", "Show route details").Alias("route").Alias("routez").Action(c.routez)
	routez.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	routez.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)

	gwyz := req.Command("gateways", "Show gateway details").Alias("gateway").Alias("gwy").Alias("gatewayz").Action(c.gwyz)
	gwyz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	gwyz.Arg("filter-name", "Filter results on gateway name").StringVar(&c.nameFilter)
	gwyz.Flag("filter-account", "Show only a certain account in account detail").StringVar(&c.accountFilter)
	gwyz.Flag("accounts", "Show account detail").Default("false").BoolVar(&c.detail)

	leafz := req.Command("leafnodes", "Show leafnode details").Alias("leaf").Alias("leafz").Action(c.leafz)
	leafz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	leafz.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)

	accountz := req.Command("accounts", "Show account details").Alias("accountz").Alias("acct").Action(c.accountz)
	accountz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	accountz.Flag("account", "Retrieve information for a specific account").StringVar(&c.account)

	jsz := req.Command("jetstream", "Show JetStream details").Alias("jsz").Alias("js").Action(c.jsz)
	jsz.Arg("wait", "Wait for a certain number of responses").Uint32Var(&c.waitFor)
	jsz.Flag("account", "Show statistics scoped to a specific account").StringVar(&c.account)
	jsz.Flag("accounts", "Include details about accounts").BoolVar(&c.includeAccounts)
	jsz.Flag("streams", "Include details about Streams").BoolVar(&c.includeStreams)
	jsz.Flag("consumer", "Include details about Consumers").BoolVar(&c.includeConsumers)
	jsz.Flag("config", "Include details about configuration").BoolVar(&c.includeConfig)
	jsz.Flag("leader", "Request a response from the Meta-group leader only").BoolVar(&c.leaderOnly)
	jsz.Flag("all", "Include accounts, streams, consumers and configuration").BoolVar(&c.includeAll)
}

func (c *SrvRequestCmd) jsz(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.JszEventOptions{
		JSzOptions:         server.JSzOptions{Account: c.account, LeaderOnly: c.leaderOnly},
		EventFilterOptions: c.reqFilter(),
	}

	if c.includeAccounts || c.includeAll {
		opts.JSzOptions.Accounts = true
	}
	if c.includeStreams || c.includeAll {
		opts.JSzOptions.Streams = true
	}
	if c.includeConsumers || c.includeAll {
		opts.JSzOptions.Consumer = true
	}
	if c.includeConfig || c.includeAll {
		opts.JSzOptions.Config = true
	}

	res, err := c.doReq("JSZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil
}

func (c *SrvRequestCmd) reqFilter() server.EventFilterOptions {
	return server.EventFilterOptions{
		Name:    c.name,
		Host:    c.host,
		Cluster: c.cluster,
		Tags:    c.tags,
		Domain:  config.JSDomain(),
	}
}

func (c *SrvRequestCmd) accountz(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.AccountzEventOptions{
		AccountzOptions:    server.AccountzOptions{Account: c.account},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("ACCOUNTZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil
}

func (c *SrvRequestCmd) leafz(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.LeafzEventOptions{
		LeafzOptions:       server.LeafzOptions{Subscriptions: c.detail},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("LEAFZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil
}

func (c *SrvRequestCmd) gwyz(_ *kingpin.ParseContext) error {
	if c.accountFilter != "" {
		c.detail = true
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.GatewayzEventOptions{
		GatewayzOptions: server.GatewayzOptions{
			Name:        c.nameFilter,
			Accounts:    c.detail,
			AccountName: c.accountFilter,
		},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("GATEWAYZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil
}

func (c *SrvRequestCmd) routez(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.RoutezEventOptions{
		RoutezOptions: server.RoutezOptions{
			Subscriptions:       c.detail,
			SubscriptionsDetail: c.detail,
		},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("ROUTEZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil

}
func (c *SrvRequestCmd) conns(_ *kingpin.ParseContext) error {
	opts := &server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Sort:                server.SortOpt(c.sortOpt),
			Username:            true,
			Subscriptions:       c.detail,
			SubscriptionsDetail: c.detail,
			Offset:              c.offset,
			Limit:               c.limit,
			CID:                 c.cidFilter,
			User:                c.userFilter,
			Account:             c.accountFilter,
			FilterSubject:       c.subjectFilter,
		},
		EventFilterOptions: c.reqFilter(),
	}

	switch c.stateFilter {
	case "open":
		opts.State = 0
	case "closed":
		opts.State = 1
	default:
		opts.State = 2
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq("CONNZ", opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil

}

func (c *SrvRequestCmd) varz(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.VarzEventOptions{
		VarzOptions:        server.VarzOptions{},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("VARZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil

}

func (c *SrvRequestCmd) subs(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.SubszEventOptions{
		SubszOptions: server.SubszOptions{
			Offset:        c.offset,
			Limit:         c.limit,
			Subscriptions: c.detail,
			Account:       c.accountFilter,
			Test:          "",
		},
		EventFilterOptions: c.reqFilter(),
	}

	res, err := c.doReq("SUBSZ", &opts, nc)
	if err != nil {
		return err
	}

	for _, m := range res {
		fmt.Println(string(m))
	}

	return nil
}

func (c *SrvRequestCmd) doReq(kind string, req interface{}, nc *nats.Conn) ([][]byte, error) {
	return doReq(req, fmt.Sprintf("$SYS.REQ.SERVER.PING.%s", kind), int(c.waitFor), nc)
}

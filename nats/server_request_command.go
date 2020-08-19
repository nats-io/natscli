package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvRequestCmd struct {
	name    string
	host    string
	cluster string

	limit   int
	offset  int
	waitFor int

	detail        bool
	sortOpt       string
	cidFilter     uint64
	stateFilter   string
	userFilter    string
	accountFilter string
	nameFilter    string
}

func configureServerRequestCommand(srv *kingpin.CmdClause) {
	c := &SrvRequestCmd{}

	req := srv.Command("request", "Request monitoring data from a specific server").Alias("req")
	req.Flag("limit", "Limit the responses to a certain amount of records").Default("1024").IntVar(&c.limit)
	req.Flag("offset", "Start at a certain record").Default("0").IntVar(&c.offset)
	req.Flag("name", "Limit to servers matching a server name").StringVar(&c.name)
	req.Flag("host", "Limit to servers matching a server host name").StringVar(&c.host)
	req.Flag("cluster", "Limit to servers matching a cluster name").StringVar(&c.cluster)

	subz := req.Command("subscriptions", "Show subscription information").Alias("sub").Alias("subsz").Action(c.subs)
	subz.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)
	subz.Flag("detail", "Include detail about all subscriptions").Default("false").BoolVar(&c.detail)
	subz.Flag("filter-account", "Filter on a specific account").StringVar(&c.accountFilter)

	varz := req.Command("variables", "Show runtime variables").Alias("var").Alias("varz").Action(c.varz)
	varz.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)

	connz := req.Command("connections", "Show connection details").Alias("conn").Alias("connz").Action(c.conns)
	connz.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)
	connz.Flag("sort", "Sort by a specific property").Default("cid").EnumVar(&c.sortOpt, "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason")
	connz.Flag("subscriptions", "Show subscriptions").Default("false").BoolVar(&c.detail)
	connz.Flag("filter-cid", "Filter on a specific CID").Uint64Var(&c.cidFilter)
	connz.Flag("filter-state", "Filter on a specific account state (open, closed, all)").Default("open").EnumVar(&c.stateFilter, "open", "closed", "all")
	connz.Flag("filter-user", "Filter on a specific username").StringVar(&c.userFilter)
	connz.Flag("filter-account", "Filter on a specific account").StringVar(&c.accountFilter)

	routez := req.Command("routes", "Show route details").Alias("route").Alias("routez").Action(c.routez)
	routez.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)
	routez.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)

	gwyz := req.Command("gateways", "Show gateway details").Alias("gateway").Alias("gwy").Alias("gatewayz").Action(c.gwyz)
	gwyz.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)
	gwyz.Arg("filter-name", "Filter results on gateway name").StringVar(&c.nameFilter)
	gwyz.Flag("filter-account", "Show only a certain account in account detail").StringVar(&c.accountFilter)
	gwyz.Flag("accounts", "Show account detail").Default("false").BoolVar(&c.detail)

	leafz := req.Command("leafnodes", "Show leafnode details").Alias("leaf").Alias("leafz").Action(c.leafz)
	leafz.Arg("wait", "Wait for a certain number of responses").Default("1").IntVar(&c.waitFor)
	leafz.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)
}

func (c *SrvRequestCmd) reqFilter() server.EventFilterOptions {
	return server.EventFilterOptions{
		Name:    c.name,
		Host:    c.host,
		Cluster: c.cluster,
	}
}

func (c *SrvRequestCmd) leafz(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper("", natsOpts()...)
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

	nc, err := prepareHelper("", natsOpts()...)
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
	nc, err := prepareHelper("", natsOpts()...)
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

	nc, err := prepareHelper("", natsOpts()...)
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
	nc, err := prepareHelper("", natsOpts()...)
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
	nc, err := prepareHelper("", natsOpts()...)
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
	jreq, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return nil, err
	}

	subj := fmt.Sprintf("$SYS.REQ.SERVER.PING.%s", kind)

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

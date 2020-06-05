package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvRequestCmd struct {
	sid    string
	limit  int
	offset int

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

	subz := req.Command("subs", "Show subscription information").Alias("subsz").Action(c.subs)
	subz.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)
	subz.Flag("detail", "Include detail about all subscriptions").Default("false").BoolVar(&c.detail)

	varz := req.Command("variables", "Show runtime variables").Alias("varz").Action(c.varz)
	varz.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)

	connz := req.Command("connections", "Show connection details").Alias("connz").Action(c.conns)
	connz.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)
	connz.Flag("sort", "Sort by a specific property").Default("cid").EnumVar(&c.sortOpt, "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason")
	connz.Flag("subscriptions", "Show subscriptions").Default("false").BoolVar(&c.detail)
	connz.Flag("filter-cid", "Filter on a specific CID").Uint64Var(&c.cidFilter)
	connz.Flag("filter-state", "Filter on a specific account state (open, closed, all)").Default("open").EnumVar(&c.stateFilter, "open", "closed", "all")
	connz.Flag("filter-user", "Filter on a specific username").StringVar(&c.userFilter)
	connz.Flag("filter-account", "Filter on a specific account").StringVar(&c.accountFilter)

	routez := req.Command("routes", "Show route details").Alias("routez").Action(c.routez)
	routez.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)
	routez.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)

	gwyz := req.Command("gateways", "Show gateway details").Alias("gatewayz").Action(c.gwyz)
	gwyz.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)
	gwyz.Arg("filter-name", "Filter results on gateway name").StringVar(&c.nameFilter)
	gwyz.Flag("filter-account", "Show only a certain account in account detail").StringVar(&c.accountFilter)
	gwyz.Flag("accounts", "Show account detail").Default("false").BoolVar(&c.detail)

	leafz := req.Command("leafnodes", "Show leafnode details").Alias("leafz").Action(c.leafz)
	leafz.Arg("server", "The ID of the server to request from").Required().StringVar(&c.sid)
	leafz.Flag("subscriptions", "Show subscription detail").Default("false").BoolVar(&c.detail)
}

func (c *SrvRequestCmd) leafz(_ *kingpin.ParseContext) error {
	type leafzOptions struct {
		// Subscriptions indicates that Leafz will return a leafnode's subscriptions
		Subscriptions bool `json:"subscriptions"`
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "LEAFZ", &leafzOptions{c.detail}, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil

}

func (c *SrvRequestCmd) gwyz(_ *kingpin.ParseContext) error {
	type gatewayzOptions struct {
		// Name will output only remote gateways with this name
		Name string `json:"name"`

		// Accounts indicates if accounts with its interest should be included in the results.
		Accounts bool `json:"accounts"`

		// AccountName will limit the list of accounts to that account name (makes Accounts implicit)
		AccountName string `json:"account_name"`
	}

	if c.accountFilter != "" {
		c.detail = true
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "GATEWAYZ", &gatewayzOptions{c.nameFilter, c.detail, c.accountFilter}, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil
}

func (c *SrvRequestCmd) routez(_ *kingpin.ParseContext) error {
	type routezOptions struct {
		// Subscriptions indicates that Routez will return a route's subscriptions
		Subscriptions bool `json:"subscriptions"`
		// SubscriptionsDetail indicates if subscription details should be included in the results
		SubscriptionsDetail bool `json:"subscriptions_detail"`
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "ROUTEZ", &routezOptions{c.detail, c.detail}, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil

}
func (c *SrvRequestCmd) conns(_ *kingpin.ParseContext) error {
	type connzOptions struct {
		// Sort indicates how the results will be sorted. Check SortOpt for possible values.
		// Only the sort by connection ID (ByCid) is ascending, all others are descending.
		Sort string `json:"sort"`

		// Username indicates if user names should be included in the results.
		Username bool `json:"auth"`

		// Subscriptions indicates if subscriptions should be included in the results.
		Subscriptions bool `json:"subscriptions"`

		// SubscriptionsDetail indicates if subscription details should be included in the results
		SubscriptionsDetail bool `json:"subscriptions_detail"`

		// Offset is used for pagination. Connz() only returns connections starting at this
		// offset from the global results.
		Offset int `json:"offset"`

		// Limit is the maximum number of connections that should be returned by Connz().
		Limit int `json:"limit"`

		// Filter for this explicit client connection.
		CID uint64 `json:"cid"`

		// Filter by connection state.
		State int `json:"state"`

		// The below options only apply if auth is true.

		// Filter by username.
		User string `json:"user"`

		// Filter by account.
		Account string `json:"acc"`
	}

	opts := &connzOptions{
		Sort:                c.sortOpt,
		Username:            true,
		Subscriptions:       c.detail,
		SubscriptionsDetail: c.detail,
		Offset:              c.offset,
		Limit:               c.limit,
		CID:                 c.cidFilter,
		User:                c.userFilter,
		Account:             c.accountFilter,
	}

	switch c.stateFilter {
	case "open":
		opts.State = 0
	case "closed":
		opts.State = 1
	default:
		opts.State = 2
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "CONNZ", opts, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil

}

func (c *SrvRequestCmd) varz(_ *kingpin.ParseContext) error {
	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "VARZ", struct{}{}, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil

}

func (c *SrvRequestCmd) subs(_ *kingpin.ParseContext) error {
	type subszOptions struct {
		// Offset is used for pagination. Subsz() only returns connections starting at this
		// offset from the global results.
		Offset int `json:"offset"`

		// Limit is the maximum number of subscriptions that should be returned by Subsz().
		Limit int `json:"limit"`

		// Subscriptions indicates if subscriptions should be included in the results.
		Subscriptions bool `json:"subscriptions"`

		// Test the list against this subject. Needs to be literal since it signifies a publish subject.
		// We will only return subscriptions that would match if a message was sent to this subject.
		Test string `json:"test,omitempty"`
	}

	nc, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	res, err := c.doReq(c.sid, "SUBSZ", &subszOptions{c.offset, c.limit, c.detail, ""}, nc)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	return nil
}

func (c *SrvRequestCmd) doReq(server string, kind string, req interface{}, nc *nats.Conn) ([]byte, error) {
	jreq, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return nil, err
	}

	subj := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", server, kind)

	if trace {
		log.Printf(">>> %s: %s\n", subj, string(jreq))
	}

	res, err := nc.Request(subj, jreq, timeout)
	if err != nil {
		return nil, err
	}

	return res.Data, nil
}

// Copyright 2023-2024 The NATS Authors
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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	terminal "golang.org/x/term"
)

type SrvWatchCmd struct {
	topCount  int
	sort      string
	accounts  map[string]map[string]server.AccountStat
	sortNames map[string]string
	mu        sync.Mutex
}

func configureServerWatchCommand(srv *fisk.CmdClause) {
	c := &SrvWatchCmd{
		accounts: map[string]map[string]server.AccountStat{},
		sortNames: map[string]string{
			"conns": "Connections",
			"subs":  "Subscriptions",
			"sentb": "Sent Bytes",
			"sentm": "Sent Messages",
			"recvb": "Received Bytes",
			"recvm": "Received Messages",
			"slow":  "Slow Consumers",
		},
	}

	watch := srv.Command("watch", "Live views of server conditions").Hidden()

	accounts := watch.Command("accounts", "Watch account usage").Action(c.accountsAction)
	accounts.Flag("sort", "Sorts by a specific property (conns*, subs, sentb, sentm, recvb, recvm, slow)").Default("conns").EnumVar(&c.sort, "conns", "subs", "sentb", "sentm", "recvb", "recvm", "slow")
	accounts.Flag("number", "Amount of Accounts to show by the selected dimension").Default("10").Short('n').IntVar(&c.topCount)
}

func (c *SrvWatchCmd) accountsAction(_ *fisk.ParseContext) error {
	_, h, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil && c.topCount == 0 {
		return fmt.Errorf("could not determine screen dimensions: %v", err)
	}

	if c.topCount == 0 {
		c.topCount = h - 6
	}

	if c.topCount < 1 {
		return fmt.Errorf("requested render limits exceed screen size")
	}

	if c.topCount > h-6 {
		c.topCount = h - 6
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	_, err = nc.Subscribe("$SYS.ACCOUNT.*.SERVER.CONNS", func(msg *nats.Msg) {
		var conns server.AccountNumConns
		err := json.Unmarshal(msg.Data, &conns)
		if err != nil {
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		_, ok := c.accounts[conns.Account]
		if !ok {
			c.accounts[conns.Account] = map[string]server.AccountStat{}
		}
		c.accounts[conns.Account][conns.Server.ID] = conns.AccountStat
	})
	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	for {
		select {
		case <-tick.C:
			c.redraw()
		case sig := <-ctx.Done():
			fmt.Printf("Exiting on %v\n", sig)
			return nil
		}
	}
}

func (c *SrvWatchCmd) redraw() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var accounts []*server.AccountStat

	for acct := range c.accounts {
		accounts = append(accounts, c.accountTotal(acct))
	}

	sort.Slice(accounts, func(i, j int) bool {
		ai := accounts[i]
		aj := accounts[j]

		switch c.sort {
		case "subs":
			return ai.NumSubs > aj.NumSubs
		case "slow":
			return ai.SlowConsumers > aj.SlowConsumers
		case "sentb":
			return ai.Sent.Bytes > aj.Sent.Bytes
		case "sentm":
			return ai.Sent.Msgs > aj.Sent.Msgs
		case "recvb":
			return ai.Received.Bytes > aj.Received.Bytes
		case "recvm":
			return ai.Received.Msgs > aj.Received.Msgs
		default:
			return ai.Conns > aj.Conns
		}
	})

	table := newTableWriter(fmt.Sprintf("Top %d Account activity by %s", c.topCount, c.sortNames[c.sort]))
	table.AddHeaders("Account", "Connections", "Leafnodes", "Subscriptions", "Slow", "Sent", "Received")

	var matched []*server.AccountStat
	if len(accounts) < c.topCount {
		matched = accounts
	} else {
		matched = accounts[:c.topCount]
	}

	for _, account := range matched {
		table.AddRow(
			account.Account,
			f(account.Conns),
			f(account.LeafNodes),
			f(account.NumSubs),
			f(account.SlowConsumers),
			fmt.Sprintf("%s / %s", f(account.Sent.Msgs), fiBytes(uint64(account.Sent.Bytes))),
			fmt.Sprintf("%s / %s", f(account.Received.Msgs), fiBytes(uint64(account.Received.Bytes))),
		)
	}
	table.AddRow()

	clearScreen()
	fmt.Println(table.Render())
}

func (c *SrvWatchCmd) accountTotal(acct string) *server.AccountStat {
	stats, ok := c.accounts[acct]
	if !ok {
		return nil
	}

	total := &server.AccountStat{}

	for _, stat := range stats {
		total.Account = acct
		total.TotalConns += stat.TotalConns
		total.Conns += stat.Conns
		total.LeafNodes += stat.LeafNodes
		total.NumSubs += stat.NumSubs
		total.SlowConsumers += stat.SlowConsumers
		total.Sent.Bytes += stat.Sent.Bytes
		total.Sent.Msgs += stat.Sent.Msgs
		total.Received.Msgs += stat.Received.Msgs
		total.Received.Bytes += stat.Received.Bytes
	}

	return total
}

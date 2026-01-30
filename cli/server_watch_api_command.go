// Copyright 2026 The NATS Authors
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
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
)

type srvWatchApiCmd struct {
	top          int
	reverse      bool
	consumers    bool
	compact      bool
	account      *regexp.Regexp
	stats        map[string]*srvWatchApiStats
	rateInterval time.Duration
	mu           sync.Mutex
}

type srvWatchApiStats struct {
	apiType   string
	account   string
	stream    string
	consumer  string
	count     uint64
	rateCount []time.Time
}

func (s *srvWatchApiStats) asset(compact bool) string {
	asset := ""
	if s.stream != "" {
		asset = s.stream
	}
	if s.consumer != "" {
		consumer := s.consumer
		if compact && len(consumer) > 20 {
			consumer = consumer[0:12] + "..."
		}

		asset = asset + " > " + consumer
	}

	return asset
}

func configureServerWatchApiCommand(watch *fisk.CmdClause) {
	c := &srvWatchApiCmd{
		stats: make(map[string]*srvWatchApiStats),
	}

	api := watch.Command("api", "Watch JetStream API")

	requests := api.Command("requests", "Watch JS API Requests").Alias("req").Action(c.reqAction)
	requests.Flag("account", "Limit to JetStream account (regex)").RegexpVar(&c.account)
	requests.Flag("top", "Shows top n requests").Default("10").Short('n').IntVar(&c.top)
	requests.Flag("rate", "Rate interval to calculate").Default("1m").DurationVar(&c.rateInterval)
	requests.Flag("record-consumer", "Record consumer names").Default("true").BoolVar(&c.consumers)
	requests.Flag("compact", "Compact output of long asset and account names").Default("true").BoolVar(&c.compact)
	requests.Flag("reverse", "Reverse sorting order").Short('R').UnNegatableBoolVar(&c.reverse)
}

func (c *srvWatchApiCmd) reqAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	if c.rateInterval == 0 {
		c.rateInterval = time.Minute
	}

	_, err = nc.Subscribe("$JS.API.>", c.handle)
	if err != nil {
		return err
	}

	tick := time.NewTicker(time.Second)
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	for {
		select {
		case <-tick.C:
			c.redraw()
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *srvWatchApiCmd) cleanRateCount(rateCount []time.Time) []time.Time {
	deleteUpTo := 0
	for _, stat := range rateCount {
		if time.Since(stat) > c.rateInterval {
			deleteUpTo++
		} else {
			break
		}
	}
	if deleteUpTo > 0 {
		rateCount = rateCount[deleteUpTo:]
	}

	return rateCount
}

func (c *srvWatchApiCmd) clearRatesUnlocked() {
	for key := range c.stats {
		c.stats[key].rateCount = c.cleanRateCount(c.stats[key].rateCount)
	}
}

func (c *srvWatchApiCmd) redraw() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.stats) == 0 {
		fmt.Println("Waiting for requests...")
		return
	}

	c.clearRatesUnlocked()

	var reqs []*srvWatchApiStats
	for _, stats := range c.stats {
		reqs = append(reqs, stats)
	}

	sort.Slice(reqs, func(i int, j int) bool {
		li := len(reqs[i].rateCount)
		lj := len(reqs[j].rateCount)

		if li == lj {
			if reqs[i].count == reqs[j].count {
				return c.boolReverse(reqs[i].asset(c.compact) < reqs[j].asset(c.compact))
			}
			return c.boolReverse(reqs[i].count < reqs[j].count)
		}
		return c.boolReverse(li < lj)
	})

	table := iu.NewTableWriterf(opts(), "Top %s JS API activity", f(c.top))
	table.AddHeaders("Type", "Account", "Count", fmt.Sprintf("Event / %s", c.rateInterval), "Asset")
	for i, stat := range reqs {
		if i == c.top {
			break
		}

		account := stat.account
		if c.compact && len(account) > 20 {
			account = fmt.Sprintf("%s...%s", account[0:10], account[len(account)-5:])
		}

		table.AddRow(stat.apiType, account, stat.count, f(len(stat.rateCount)), stat.asset(c.compact))
	}

	iu.ClearScreen()
	fmt.Println(table.Render())
}

func (c *srvWatchApiCmd) handle(msg *nats.Msg) {
	t, err := api.TypeForRequestSubject(msg.Subject)
	if err != nil {
		return
	}

	apiType, ok := t.(api.SchemaManagedType)
	if !ok {
		return
	}

	schemaType := apiType.SchemaType()
	key := schemaType
	account := ""
	stream := ""
	consumer := ""
	ts := time.Now()

	jnfo := msg.Header.Get("Nats-Request-Info")
	if jnfo != "" {
		var nfo server.ClientInfo
		err := json.Unmarshal([]byte(jnfo), &nfo)
		if err == nil && nfo.Account != "" {
			if c.account != nil && !c.account.MatchString(nfo.Account) {
				return
			}

			key = nfo.Account + ":" + key
			account = nfo.Account
		}
	}

	switch schemaType {
	case "io.nats.jetstream.api.v1.stream_info_request":
		parts := strings.Split(msg.Subject, ".")
		stream = parts[len(parts)-1]
		key = key + ":" + stream
	case "io.nats.jetstream.api.v1.consumer_info_request":
		parts := strings.Split(msg.Subject, ".")
		stream = parts[len(parts)-2]
		consumer = parts[len(parts)-1]
		key = key + ":" + stream + ":" + consumer
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok = c.stats[key]
	if !ok {
		c.stats[key] = &srvWatchApiStats{
			apiType: strings.TrimPrefix(schemaType, "io.nats.jetstream.api.v1."),
			account: account,
			stream:  stream,
		}
		if c.consumers {
			c.stats[key].consumer = consumer
		}
	}

	c.stats[key].rateCount = append(c.stats[key].rateCount, ts)
	c.stats[key].count++

}

func (c *srvWatchApiCmd) boolReverse(v bool) bool {
	if c.reverse {
		return v
	}

	return !v
}

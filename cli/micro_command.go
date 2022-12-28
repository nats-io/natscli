// Copyright 2022 The NATS Authors
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/xlab/tablewriter"
)

type microCmd struct {
	name     string
	id       string
	showJSON bool
}

func configureMicroCommand(app commandHost) {
	c := &microCmd{}
	mc := app.Command("micro", "Micro Services discovery and management").Alias("a")
	mc.HelpLong("WARNING: This command is experimental")

	ls := mc.Command("list", "List known Micro services").Alias("ls").Alias("l").Action(c.listAction)
	ls.Arg("service", "List instances of a specific service").PlaceHolder("NAME").StringVar(&c.name)
	ls.Flag("json", "Show JSON output").Short('j').UnNegatableBoolVar(&c.showJSON)

	info := mc.Command("info", "Show Micro service information").Alias("i").Action(c.infoAction)
	info.Arg("service", "Service to show").Required().StringVar(&c.name)
	info.Arg("id", "Show infor for a specific ID").StringVar(&c.id)
	info.Flag("json", "Show JSON output").Short('j').UnNegatableBoolVar(&c.showJSON)

	stats := mc.Command("stats", "Report Micro service statistics").Action(c.statsAction)
	stats.Arg("service", "Service to show").Required().StringVar(&c.name)
	stats.Flag("json", "Show JSON output").Short('j').UnNegatableBoolVar(&c.showJSON)

	ping := mc.Command("ping", "Sends a ping to all services").Action(c.pingAction)
	ping.Arg("service", "Service to show").StringVar(&c.name)
}

func init() {
	registerCommand("micro", 0, configureMicroCommand)
}

func (c *microCmd) makeSubj(v micro.Verb, s string, i string) string {
	if s == "" {
		return fmt.Sprintf("%s.%s", micro.APIPrefix, v.String())
	}

	if i == "" {
		return fmt.Sprintf("%s.%s.%s", micro.APIPrefix, v.String(), s)
	}

	return fmt.Sprintf("%s.%s.%s.%s", micro.APIPrefix, v.String(), s, i)
}

func (c *microCmd) getInstanceStats(nc *nats.Conn, name string, id string) (*micro.Stats, error) {
	resp, err := doReq(nil, c.makeSubj(micro.StatsVerb, name, id), 1, nc)
	if err != nil {
		return nil, err
	}

	if len(resp) == 0 {
		return nil, fmt.Errorf("no statistics received for %s > %s", name, id)
	}

	var s micro.Stats
	err = json.Unmarshal(resp[0], &s)

	return &s, err
}

func (c *microCmd) getInfo(nc *nats.Conn, name string, id string, wait int) ([]micro.Info, error) {
	resp, err := doReq(nil, c.makeSubj(micro.InfoVerb, name, id), wait, nc)
	if err != nil {
		return nil, err
	}

	var nfos []micro.Info
	for _, r := range resp {
		res := micro.Info{}
		err = json.Unmarshal(r, &res)
		if err != nil {
			return nil, fmt.Errorf("invalid response: %v", err)
		}
		nfos = append(nfos, res)
	}

	sort.Slice(nfos, func(i, j int) bool {
		if nfos[i].Name < nfos[j].Name {
			return true
		}

		return nfos[i].ID < nfos[j].ID
	})

	return nfos, nil
}

func (c *microCmd) pingAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	start := time.Now()

	sub, err := nc.Subscribe(nc.NewRespInbox(), func(m *nats.Msg) {
		var r micro.Ping
		err = json.Unmarshal(m.Data, &r)
		if err != nil {
			return
		}

		fmt.Printf("%-50s rtt=%s\n", fmt.Sprintf("%s %s", r.Name, r.ID), humanizeDuration(time.Since(start)))
	})
	if err != nil {
		return err
	}

	msg := nats.NewMsg(c.makeSubj(micro.PingVerb, c.name, ""))
	msg.Reply = sub.Subject
	nc.PublishMsg(msg)

	<-ctx.Done()

	return nil
}

func (c *microCmd) statsAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	resp, err := doReq(nil, c.makeSubj(micro.StatsVerb, c.name, ""), 0, nc)
	if err != nil {
		return err
	}

	if len(resp) == 0 {
		fmt.Println("No responses received")
		return nil
	}

	var stats []*micro.Stats
	for _, r := range resp {
		s := micro.Stats{}
		err = json.Unmarshal(r, &s)
		if err != nil {
			return err
		}
		stats = append(stats, &s)
	}

	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Name < stats[j].Name {
			return true
		}

		return stats[i].ID < stats[j].ID
	})

	if c.showJSON {
		printJSON(stats)
		return nil
	}

	table := newTableWriter(fmt.Sprintf("%s Service Statistics", c.name))
	table.AddHeaders("ID", "Requests", "Errors", "Processing Time", "Average Time")

	var requests, errors int
	var runTime time.Duration
	for _, s := range stats {
		table.AddRow(s.ID, humanize.Comma(int64(s.NumRequests)), humanize.Comma(int64(s.NumErrors)), humanizeDuration(s.ProcessingTime), humanizeDuration(s.AverageProcessingTime))
		requests += s.NumRequests
		errors += s.NumErrors
		runTime += s.ProcessingTime
	}

	table.AddSeparator()
	var avg time.Duration
	if runTime > 0 {
		avg = runTime / time.Duration(requests+errors)
	}

	table.AddRow("", humanize.Comma(int64(requests)), humanize.Comma(int64(errors)), humanizeDuration(runTime), humanizeDuration(avg))

	fmt.Println(table.Render())

	return nil
}

func (c *microCmd) infoAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	nfos, err := c.getInfo(nc, c.name, c.id, 1)
	if err != nil {
		return err
	}

	if len(nfos) == 0 {
		if c.showJSON {
			fmt.Println("{}")
		} else {
			fmt.Println("No results received")
		}
		return nil
	}

	nfo := nfos[0]

	stats, err := c.getInstanceStats(nc, nfo.Name, nfo.ID)
	if err != nil {
		return err
	}

	if c.showJSON {
		printJSON(map[string]any{
			"info":  nfo,
			"stats": stats,
		})
		return nil
	}

	fmt.Println("Service Information:")
	fmt.Println()
	fmt.Printf("      Service: %v (%v)\n", nfo.Name, nfo.ID)
	fmt.Printf("  Description: %v\n", nfo.Description)
	fmt.Printf("      Version: %v\n", nfo.Version)
	fmt.Printf("      Subject: %v\n", nfo.Subject)
	fmt.Println()
	fmt.Println("Statistics:")
	fmt.Println()
	fmt.Printf("         Requests: %s\n", humanize.Comma(int64(stats.NumRequests)))
	fmt.Printf("           Errors: %s\n", humanize.Comma(int64(stats.NumErrors)))
	fmt.Printf("  Processing Time: %s (average %s)\n", humanizeDuration(stats.ProcessingTime), humanizeDuration(stats.AverageProcessingTime))
	fmt.Printf("          Started: %v\n", stats.Started)
	if stats.LastError != "" {
		fmt.Printf("   Last Error: %v\n", stats.LastError)
	}
	if stats.Data != nil {
		fmt.Println()
		fmt.Println("Service Specific Statistics:")
		fmt.Println()
		out := bytes.NewBuffer([]byte{})
		json.Indent(out, stats.Data, "  ", "  ")
		fmt.Printf("  %s\n", out.String())
	}
	fmt.Println()

	return nil
}

func (c *microCmd) listAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	nfos, err := c.getInfo(nc, c.name, "", 0)
	if err != nil {
		return err
	}

	if c.showJSON {
		printJSON(nfos)
		return nil
	}

	if len(nfos) == 0 {
		fmt.Println("No results received")
		return nil
	}

	var table *tablewriter.Table
	if c.name == "" {
		table = newTableWriter("All Micro Services")
	} else {
		table = newTableWriter(fmt.Sprintf("%s Micro Service", c.name))
	}
	table.AddHeaders("Name", "Version", "ID", "Description")
	var pd, pv, pn string
	for _, s := range nfos {
		v := s.Version
		if v == pv {
			v = ""
		}
		d := s.Description
		if d == pd {
			d = ""
		}
		n := s.Name
		if n == pn {
			n = ""
		}
		table.AddRow(n, v, s.ID, d)
		pd = s.Description
		pv = s.Version
		pn = s.Name
	}
	fmt.Println(table.Render())

	return nil
}

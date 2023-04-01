// Copyright 2022-2023 The NATS Authors
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
	"os"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
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
	info.Arg("id", "Show info for a specific ID").StringVar(&c.id)
	info.Flag("json", "Show JSON output").Short('j').UnNegatableBoolVar(&c.showJSON)

	stats := mc.Command("stats", "Report Micro service statistics").Action(c.statsAction)
	stats.Arg("service", "Service to show").Required().StringVar(&c.name)
	stats.Arg("id", "Show info for a specific ID").StringVar(&c.id)
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

func (c *microCmd) parseMessage(m []byte, expectedType string) (any, error) {
	var (
		t      string
		parsed any
		err    error
	)

	if os.Getenv("NOVALIDATE") == "" {
		t, parsed, err = api.ParseAndValidateMessage(m, validator())
	} else {
		t, parsed, err = api.ParseMessage(m)
	}
	if err != nil {
		return nil, err
	}

	if t != expectedType {
		return nil, fmt.Errorf("invalid response type %s", t)
	}

	return parsed, nil
}

func (c *microCmd) getInstanceStats(nc *nats.Conn, name string, id string) (*micro.Stats, error) {
	resp, err := doReq(nil, c.makeSubj(micro.StatsVerb, name, id), 1, nc)
	if err != nil {
		if err == nats.ErrNoResponders {
			return nil, fmt.Errorf("no micro instances found")
		}
		return nil, err
	}

	if len(resp) == 0 {
		return nil, fmt.Errorf("no statistics received for %s > %s", name, id)
	}

	stats, err := c.parseMessage(resp[0], micro.StatsResponseType)
	if err != nil {
		return nil, err
	}

	return stats.(*micro.Stats), err
}

func (c *microCmd) getInfo(nc *nats.Conn, name string, id string, wait int) ([]*micro.Info, error) {
	resp, err := doReq(nil, c.makeSubj(micro.InfoVerb, name, id), wait, nc)
	if err != nil {
		if err == nats.ErrNoResponders {
			return nil, fmt.Errorf("no micro instances found")
		}
		return nil, err
	}

	var nfos []*micro.Info
	for _, r := range resp {
		nfo, err := c.parseMessage(r, micro.InfoResponseType)
		if err != nil {
			return nil, err
		}
		nfos = append(nfos, nfo.(*micro.Info))
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
		if opts.Trace {
			log.Printf("<<< %s", string(m.Data))
		}
		resp, err := c.parseMessage(m.Data, micro.PingResponseType)
		if err != nil {
			return
		}
		r := resp.(*micro.Ping)
		fmt.Printf("%-50s rtt=%s\n", fmt.Sprintf("%s %s", r.Name, r.ID), humanizeDuration(time.Since(start)))
	})
	if err != nil {
		return err
	}

	msg := nats.NewMsg(c.makeSubj(micro.PingVerb, c.name, ""))
	msg.Reply = sub.Subject
	nc.PublishMsg(msg)
	if opts.Trace {
		log.Printf(">>> %s", msg.Subject)
	}
	<-ctx.Done()

	return nil
}

func (c *microCmd) statsAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	resp, err := doReq(nil, c.makeSubj(micro.StatsVerb, c.name, c.id), 0, nc)
	if err != nil {
		if err == nats.ErrNoResponders {
			return fmt.Errorf("no micro instances found")
		}

		return err
	}

	if len(resp) == 0 {
		fmt.Println("No responses received")
		return nil
	}

	var stats []*micro.Stats
	for _, r := range resp {
		s, err := c.parseMessage(r, micro.StatsResponseType)
		if err != nil {
			return err
		}
		stats = append(stats, s.(*micro.Stats))
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
	table.AddHeaders("ID", "Endpoint", "Requests", "Errors", "Processing Time", "Average Time")

	var requests, errors int
	var runTime time.Duration
	for _, s := range stats {
		for c, e := range s.Endpoints {
			id := s.ID
			if c > 0 {
				id = ""
			}

			table.AddRow(id, e.Name, humanize.Comma(int64(e.NumRequests)), humanize.Comma(int64(e.NumErrors)), humanizeDuration(e.ProcessingTime), humanizeDuration(e.AverageProcessingTime))
			requests += e.NumRequests
			errors += e.NumErrors
			runTime += e.ProcessingTime
		}
	}

	var avg time.Duration
	if runTime > 0 {
		avg = runTime / time.Duration(requests+errors)
	}

	table.AddFooter("", "", humanize.Comma(int64(requests)), humanize.Comma(int64(errors)), humanizeDuration(runTime), humanizeDuration(avg))

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
	fmt.Printf("     Subjects: %v\n", strings.Join(nfo.Subjects, ", "))
	if len(nfo.Metadata) > 0 {
		fmt.Println()
		fmt.Printf("Metadata:")
		dumpMapStrings(nfo.Metadata, 3)
	}
	fmt.Println()

	fmt.Printf("Statistics for %d Endpoint(s):\n\n", len(stats.Endpoints))
	for _, e := range stats.Endpoints {
		if e.Name == "" {
			e.Name = "default"
		}

		fmt.Printf("  %s Endpoint Statistics:\n", e.Name)
		fmt.Println()
		fmt.Printf("           Requests: %s\n", humanize.Comma(int64(e.NumRequests)))
		fmt.Printf("             Errors: %s\n", humanize.Comma(int64(e.NumErrors)))
		fmt.Printf("    Processing Time: %s (average %s)\n", humanizeDuration(e.ProcessingTime), humanizeDuration(e.AverageProcessingTime))
		fmt.Printf("            Started: %v\n", stats.Started)
		if e.LastError != "" {
			fmt.Printf("     Last Error: %v\n", e.LastError)
		}
		if e.Data != nil {
			fmt.Println()
			fmt.Println("  Endpoint Specific Statistics:")
			fmt.Println()
			out := bytes.NewBuffer([]byte{})
			json.Indent(out, e.Data, "    ", "    ")
			fmt.Printf("    %s\n", out.String())
		}
		fmt.Println()
	}

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

	var table *tbl
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

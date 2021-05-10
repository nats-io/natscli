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
	"log"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvInfoCmd struct {
	id string
}

func configureServerInfoCommand(srv *kingpin.CmdClause) {
	c := &SrvInfoCmd{}

	info := srv.Command("info", "Show information about a single server").Alias("i").Action(c.info)
	info.Arg("server", "Server ID or Name to inspect").StringVar(&c.id)
}

func (c *SrvInfoCmd) info(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	subj := fmt.Sprintf("$SYS.REQ.SERVER.%s.VARZ", c.id)
	body := []byte("{}")

	if len(c.id) != 56 || strings.ToUpper(c.id) != c.id {
		subj = "$SYS.REQ.SERVER.PING.VARZ"
		opts := server.VarzEventOptions{EventFilterOptions: server.EventFilterOptions{Name: c.id}}
		body, err = json.Marshal(opts)
		if err != nil {
			return err
		}
	}

	if trace {
		log.Printf(">>> %s: %s", subj, string(body))
	}

	resp, err := nc.Request(subj, body, timeout)
	if err != nil {
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}
	if trace {
		log.Printf("<<< %q", resp.Data)
	}

	reqresp := map[string]json.RawMessage{}
	err = json.Unmarshal(resp.Data, &reqresp)
	if err != nil {
		return err
	}

	errresp, ok := reqresp["error"]
	if ok {
		return fmt.Errorf("invalid response received: %#v", errresp)
	}

	data, ok := reqresp["data"]
	if !ok {
		return fmt.Errorf("no data received in response: %#v", reqresp)
	}

	varz := &server.Varz{}
	err = json.Unmarshal(data, varz)
	if err != nil {
		return err
	}

	bold := color.New(color.Bold).SprintFunc()

	if varz.ID == varz.Name {
		fmt.Printf("Server information for %s\n\n", bold(varz.ID))
	} else {
		fmt.Printf("Server information for %s (%s)\n\n", bold(varz.Name), bold(varz.ID))
	}

	fmt.Printf("%s\n\n", bold("Process Details:"))
	fmt.Printf("         Version: %s\n", varz.Version)
	fmt.Printf("      Git Commit: %s\n", varz.GitCommit)
	fmt.Printf("      Go Version: %s\n", varz.GoVersion)
	fmt.Printf("      Start Time: %v\n", varz.Start)
	fmt.Printf("          Uptime: %s\n", varz.Uptime)

	fmt.Println()
	fmt.Printf("%s\n\n", bold("Connection Details:"))
	fmt.Printf("   Auth Required: %v\n", varz.AuthRequired)
	fmt.Printf("    TLS Required: %v\n", varz.TLSRequired)
	if varz.IP != "" {
		fmt.Printf("            Host: %s:%d (%s)\n", varz.Host, varz.Port, varz.IP)
	} else {
		fmt.Printf("            Host: %s:%d\n", varz.Host, varz.Port)
	}
	fmt.Printf("     Client URLs: %s\n", strings.Join(varz.ClientConnectURLs, "\n                  "))
	if len(varz.WSConnectURLs) > 0 {
		fmt.Printf("  WebSocket URLs: %s\n", strings.Join(varz.WSConnectURLs, "\n                  "))
	}

	fmt.Println()
	if varz.JetStream.Config != nil && varz.JetStream.Config.StoreDir != "" {
		js := varz.JetStream
		fmt.Printf("%s\n\n", bold("JetStream:"))
		if len(varz.Tags) > 0 {
			fmt.Printf("         Server Tags: %s\n", strings.Join(varz.Tags, ", "))
		}
		fmt.Printf("              Domain: %s\n", js.Config.Domain)
		fmt.Printf("   Storage Directory: %s\n", js.Config.StoreDir)
		fmt.Printf("          Max Memory: %s\n", humanize.IBytes(uint64(js.Config.MaxMemory)))
		fmt.Printf("            Max File: %s\n", humanize.IBytes(uint64(js.Config.MaxStore)))
		fmt.Printf("      Active Acconts: %s\n", humanize.Comma(int64(js.Stats.Accounts)))
		fmt.Printf("       Memory In Use: %s\n", humanize.IBytes(js.Stats.Memory))
		fmt.Printf("         File In Use: %s\n", humanize.IBytes(js.Stats.Store))
		fmt.Printf("        API Requests: %s\n", humanize.Comma(int64(js.Stats.API.Total)))
		fmt.Printf("          API Errors: %s\n", humanize.Comma(int64(js.Stats.API.Errors)))
		fmt.Println()
	}

	fmt.Printf("%s\n\n", bold("Limits:"))
	fmt.Printf("        Max Conn: %d\n", varz.MaxConn)
	fmt.Printf("        Max Subs: %d\n", varz.MaxSubs)
	fmt.Printf("     Max Payload: %s\n", humanize.IBytes(uint64(varz.MaxPayload)))
	fmt.Printf("     TLS Timeout: %v\n", time.Duration(varz.TLSTimeout)*time.Second)
	fmt.Printf("  Write Deadline: %v\n", varz.WriteDeadline.Round(time.Millisecond))

	fmt.Println()
	fmt.Printf("%s\n\n", bold("Statistics:"))
	fmt.Printf("       CPU Cores: %d %.2f%%\n", varz.Cores, varz.CPU)
	fmt.Printf("          Memory: %s\n", humanize.IBytes(uint64(varz.Mem)))
	fmt.Printf("     Connections: %s\n", humanize.Comma(int64(varz.Connections)))
	fmt.Printf("   Subscriptions: %s\n", humanize.Comma(int64(varz.Subscriptions)))
	fmt.Printf("            Msgs: %s in %s out\n", humanize.Comma(varz.InMsgs), humanize.Comma(varz.OutMsgs))
	fmt.Printf("           Bytes: %s in %s out\n", humanize.IBytes(uint64(varz.InBytes)), humanize.IBytes(uint64(varz.OutBytes)))
	fmt.Printf("  Slow Consumers: %s\n", humanize.Comma(varz.SlowConsumers))

	if len(varz.Cluster.URLs) > 0 {
		fmt.Println()
		fmt.Printf("%s\n\n", bold("Cluster:"))
		fmt.Printf("            Name: %s\n", varz.Cluster.Name)
		fmt.Printf("            Host: %s:%d\n", varz.Cluster.Host, varz.Cluster.Port)
		fmt.Printf("            URLs: %s\n", strings.Join(varz.Cluster.URLs, "\n                  "))
	}

	if len(varz.Gateway.Gateways) > 0 {
		fmt.Println()
		fmt.Printf("%s\n\n", bold("Super Cluster:"))
		fmt.Printf("            Name: %s\n", varz.Gateway.Name)
		fmt.Printf("            Host: %s:%d\n", varz.Gateway.Host, varz.Gateway.Port)
		var list []string
		for _, gwy := range varz.Gateway.Gateways {
			list = append(list, gwy.Name)
		}
		sort.Strings(list)

		fmt.Printf("        Clusters: %s\n", strings.Join(list, "\n                  "))
	}
	fmt.Println()
	return nil
}

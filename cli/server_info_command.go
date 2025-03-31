// Copyright 2020-2025 The NATS Authors
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
	"os"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
)

type SrvInfoCmd struct {
	id string
}

func configureServerInfoCommand(srv *fisk.CmdClause) {
	c := &SrvInfoCmd{}

	info := srv.Command("info", "Show information about a single server").Alias("i").Action(c.info)
	info.Arg("server", "Server ID or Name to inspect").StringVar(&c.id)
}

func (c *SrvInfoCmd) info(_ *fisk.ParseContext) error {
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

	if opts().Trace {
		log.Printf(">>> %s: %s", subj, string(body))
	}

	resp, err := nc.Request(subj, body, opts().Timeout)
	if err != nil {
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}
	if opts().Trace {
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

	cols := newColumns("")
	defer cols.Frender(os.Stdout)

	if varz.ID == varz.Name {
		cols.SetHeading("Server information for %s", varz.ID)
	} else {
		cols.SetHeading("Server information for %s (%s)", varz.Name, varz.ID)
	}

	cols.AddSectionTitle("Process Details")
	cols.AddRow("Version", varz.Version)
	cols.AddRow("Git Commit", varz.GitCommit)
	cols.AddRow("Go Version", varz.GoVersion)
	cols.AddRow("Start Time", varz.Start)
	cols.AddRow("Configuration Load Time", varz.ConfigLoadTime)
	if varz.ConfigDigest != "" {
		cols.AddRow("Configuration Digest", varz.ConfigDigest)
	}
	cols.AddRow("Uptime", varz.Uptime)

	cols.AddSectionTitle("Connection Details")
	cols.AddRow("Auth Required", varz.AuthRequired)
	cols.AddRow("TLS Required", varz.TLSRequired)
	if varz.IP != "" {
		cols.AddRowf("Host", "%s:%d (%s)", varz.Host, varz.Port, varz.IP)
	} else {
		cols.AddRowf("Host", "%s:%d", varz.Host, varz.Port)
	}
	for i, u := range varz.ClientConnectURLs {
		if i == 0 {
			cols.AddRow("Client URLs", u)
		} else {
			cols.AddRow("", u)
		}
	}

	if len(varz.WSConnectURLs) > 0 {
		for i, u := range varz.WSConnectURLs {
			if i == 0 {
				cols.AddRow("WebSocket URLs", u)
			} else {
				cols.AddRow("", u)
			}
		}
	}

	if varz.JetStream.Config != nil && varz.JetStream.Config.StoreDir != "" {
		js := varz.JetStream
		cols.AddSectionTitle("JetStream")
		cols.AddRow("Domain", js.Config.Domain)
		cols.AddRow("API Support Level", js.Stats.API.Level)
		cols.AddRow("Storage Directory", js.Config.StoreDir)
		cols.AddRow("Active Accounts", js.Stats.Accounts)
		cols.AddRow("Memory In Use", humanize.IBytes(js.Stats.Memory))
		cols.AddRow("File In Use", humanize.IBytes(js.Stats.Store))
		cols.AddRow("API Requests", js.Stats.API.Total)
		cols.AddRow("API Errors", js.Stats.API.Errors)
		// would be zero on machines that dont support this setting
		if js.Config.SyncInterval > 0 {
			cols.AddRow("Always sync writes to disk", js.Config.SyncAlways)
			cols.AddRow("Write sync Frequency", js.Config.SyncInterval)
		}
		cols.AddRow("Maximum Memory Storage", humanize.IBytes(uint64(js.Config.MaxMemory)))
		cols.AddRow("Maximum File Storage", humanize.IBytes(uint64(js.Config.MaxStore)))
		cols.AddRowIfNotEmpty("Unique Tag", js.Config.UniqueTag)
		cols.AddRow("Cluster Message Compression", js.Config.CompressOK)
		if js.Limits != nil {
			cols.AddRowUnlimited("Maximum HA Assets", int64(js.Limits.MaxHAAssets), 0)
			cols.AddRowUnlimited("Maximum Ack Pending", int64(js.Limits.MaxAckPending), 0)
			cols.AddRowUnlimited("Maximum Request Batch", int64(js.Limits.MaxRequestBatch), 0)
			if js.Limits.Duplicates == 0 {
				cols.AddRow("Maximum Duplicate Window", "unlimited")
			} else {
				cols.AddRow("Maximum Duplicate Window", js.Limits.Duplicates)
			}
		}
		cols.AddRow("Strict API Parsing", js.Config.Strict)
	}

	cols.AddSectionTitle("Limits")
	cols.AddRow("Maximum Connections", varz.MaxConn)
	cols.AddRow("Maximum Subscriptions", varz.MaxSubs)
	cols.AddRow("Maximum Payload", humanize.IBytes(uint64(varz.MaxPayload)))
	cols.AddRow("TLS Timeout", time.Duration(varz.TLSTimeout)*time.Second)
	cols.AddRow("Write Deadline", varz.WriteDeadline.Round(time.Millisecond))

	cols.AddSectionTitle("Statistics")
	cols.AddRowf("CPU Cores", "%d %.2f%%", varz.Cores, varz.CPU)
	if varz.MaxProcs > 0 {
		cols.AddRowf("GOMAXPROCS", "%d", varz.MaxProcs)
	}
	cols.AddRow("Memory", humanize.IBytes(uint64(varz.Mem)))
	cols.AddRow("Connections", varz.Connections)
	cols.AddRow("Subscriptions", varz.Subscriptions)
	cols.AddRowf("Messages", "%s in %s out", f(varz.InMsgs), f(varz.OutMsgs))
	cols.AddRowf("Bytes", "%s in %s out", humanize.IBytes(uint64(varz.InBytes)), humanize.IBytes(uint64(varz.OutBytes)))
	cols.AddRow("Slow Consumers", varz.SlowConsumers)

	if len(varz.Cluster.URLs) > 0 {
		cols.AddSectionTitle("Cluster")
		cols.AddRow("Name", varz.Cluster.Name)
		cols.AddRowIf("Tags", strings.Join(varz.Tags, ", "), len(varz.Tags) > 0)
		cols.AddRowf("Host", "%s:%d", varz.Cluster.Host, varz.Cluster.Port)
		for i, u := range varz.Cluster.URLs {
			if i == 0 {
				cols.AddRow("URLs", u)
			} else {
				cols.AddRow("", u)
			}
		}
	}

	if len(varz.Gateway.Gateways) > 0 {
		cols.AddSectionTitle("Super Cluster")
		cols.AddRow("Name", varz.Gateway.Name)
		cols.AddRowf("Host", "%s:%d", varz.Gateway.Host, varz.Gateway.Port)
		var list []string
		for _, gwy := range varz.Gateway.Gateways {
			list = append(list, gwy.Name)
		}
		sort.Strings(list)
		for i, n := range list {
			if i == 0 {
				cols.AddRow("Clusters", n)
			} else {
				cols.AddRow("", n)
			}
		}
	}
	return nil
}

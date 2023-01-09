// Copyright 2019-2020 The NATS Authors
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type actCmd struct {
	sort    string
	subject string
	topk    int

	backupDirectory   string
	healthCheck       bool
	snapShotConsumers bool
	force             bool
	failOnWarn        bool

	placementCluster string
	placementTags    []string
}

func configureActCommand(app commandHost) {
	c := &actCmd{}
	act := app.Command("account", "Account information and status").Alias("a")
	addCheat("account", act)
	act.Command("info", "Account information").Alias("nfo").Action(c.infoAction)

	report := act.Command("report", "Report on account metrics").Alias("rep")

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.reportConnectionsAction)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	conns.Flag("subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subject)

	report.Command("statistics", "Report on server statistics").Alias("stats").Alias("statsz").Action(c.reportServerStats)

	backup := act.Command("backup", "Creates a backup of all  JetStream Streams over the NATS network").Alias("snapshot").Action(c.backupAction)
	backup.Arg("target", "Directory to create the backup in").Required().StringVar(&c.backupDirectory)
	backup.Flag("check", "Checks the Stream for health prior to backup").UnNegatableBoolVar(&c.healthCheck)
	backup.Flag("consumers", "Enable or disable consumer backups").Default("true").BoolVar(&c.snapShotConsumers)
	backup.Flag("force", "Perform backup without prompting").Short('f').UnNegatableBoolVar(&c.force)
	backup.Flag("critical-warnings", "Treat warnings as failures").Short('w').UnNegatableBoolVar(&c.failOnWarn)

	restore := act.Command("restore", "Restore an account backup over the NATS network").Action(c.restoreAction)
	restore.Arg("directory", "The directory holding the account backup to restore").Required().ExistingDirVar(&c.backupDirectory)
	restore.Flag("cluster", "Place the stream in a specific cluster").StringVar(&c.placementCluster)
	restore.Flag("tag", "Place the stream on servers that has specific tags (pass multiple times)").StringsVar(&c.placementTags)
}

func init() {
	registerCommand("account", 0, configureActCommand)
}

func (c *actCmd) backupAction(_ *fisk.ParseContext) error {
	var err error

	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	streams, missing, err := mgr.Streams(nil)
	if err != nil {
		return err
	}

	if len(missing) > 0 {
		return fmt.Errorf("could not obtain stream information for %d streams", len(missing))
	}

	if len(streams) == 0 {
		return fmt.Errorf("no streams found")
	}

	totalSize := uint64(0)
	totalConsumers := 0

	for _, s := range streams {
		state, _ := s.LatestState()
		totalConsumers += state.Consumers
		totalSize += state.Bytes
	}

	fmt.Printf("Performing backup of all streams to %s\n\n", c.backupDirectory)
	fmt.Printf("    Streams: %s\n", humanize.Comma(int64(len(streams))))
	fmt.Printf("       Size: %s\n", humanize.IBytes(totalSize))
	fmt.Printf("  Consumers: %s\n", humanize.Comma(int64(totalConsumers)))
	fmt.Println()

	if !c.force {
		ok, err := askConfirmation("Perform backup", false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	err = os.MkdirAll(c.backupDirectory, 0700)
	if err != nil {
		return err
	}

	var errs []error
	var warns []error

	for _, s := range streams {
		err = backupStream(s, false, c.snapShotConsumers, c.healthCheck, filepath.Join(c.backupDirectory, s.Name()))
		if errors.Is(err, jsm.ErrMemoryStreamNotSupported) {
			fmt.Printf("Backup of %s failed: %v\n", s.Name(), err)
			warns = append(warns, fmt.Errorf("%s: %w", s.Name(), err))
		} else if err != nil {
			fmt.Printf("Backup of %s failed: %s\n", s.Name(), err)
			errs = append(errs, fmt.Errorf("%s: %s", s.Name(), err))
		}
		fmt.Println()
	}

	if len(warns) > 0 {
		fmt.Printf("Backup Warnings: \n")
		for _, err := range warns {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 {
		fmt.Printf("Backup failures: \n")
		for _, err := range errs {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 || len(warns) > 0 && c.failOnWarn {
		return fmt.Errorf("backup failed")
	}

	return nil
}

func (c *actCmd) restoreAction(kp *fisk.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")
	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return err
	}
	existingStreams := map[string]struct{}{}
	for _, n := range streams {
		existingStreams[n] = struct{}{}
	}
	de, err := os.ReadDir(c.backupDirectory)
	fisk.FatalIfError(err, "setup failed")
	for _, d := range de {
		if !d.IsDir() {
			fisk.FatalIfError(err, "expected a directory")
		}
		if _, ok := existingStreams[d.Name()]; ok {
			fisk.Fatalf("stream %q exists already", d.Name())
		}
		_, err := os.Stat(filepath.Join(c.backupDirectory, d.Name(), "backup.json"))
		fisk.FatalIfError(err, "expected backup.json")
	}
	fmt.Printf("Restoring backup of all %d streams in directory %q\n\n", len(de), c.backupDirectory)
	s := &streamCmd{msgID: -1, showProgress: false, placementCluster: c.placementCluster, placementTags: c.placementTags}
	for _, d := range de {
		s.backupDirectory = filepath.Join(c.backupDirectory, d.Name())
		err := s.restoreAction(kp)
		fisk.FatalIfError(err, "restore for %s failed", d.Name())
	}
	return nil
}

func (c *actCmd) reportConnectionsAction(pc *fisk.ParseContext) error {
	cmd := SrvReportCmd{
		topk:    c.topk,
		sort:    c.sort,
		subject: c.subject,
	}

	return cmd.reportConnections(pc)
}

func (c *actCmd) reportServerStats(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := doReq(nil, "$SYS.REQ.ACCOUNT.PING.STATZ", 0, nc)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	table := newTableWriter("Server Statistics")
	table.AddHeaders("Server", "Cluster", "Version", "Tags", "Connections", "Leafnodes", "Sent Bytes", "Sent Messages", "Received Bytes", "Received Messages", "Slow Consumers")

	var (
		conn, ln           int
		sb, sm, rb, rm, sc int64
	)
	for _, r := range res {
		sz, err := c.parseAccountStatResp(r)
		if err != nil {
			return err
		}

		if len(sz.Stats.Accounts) == 0 {
			continue
		}

		stats := sz.Stats.Accounts[0]

		conn += stats.Conns
		ln += stats.LeafNodes
		sb += stats.Sent.Bytes
		sm += stats.Sent.Msgs
		rb += stats.Received.Bytes
		rm += stats.Received.Msgs
		sc += stats.SlowConsumers

		table.AddRow(
			sz.ServerInfo.Host,
			sz.ServerInfo.Cluster,
			sz.ServerInfo.Version,
			strings.Join(sz.ServerInfo.Tags, ", "),
			humanize.Comma(int64(stats.Conns)),
			humanize.Comma(int64(stats.LeafNodes)),
			humanize.IBytes(uint64(stats.Sent.Bytes)),
			humanize.Comma(stats.Sent.Msgs),
			humanize.IBytes(uint64(stats.Received.Bytes)),
			humanize.Comma(stats.Received.Msgs),
			humanize.Comma(stats.SlowConsumers),
		)
	}
	table.AddSeparator()
	table.AddRow(len(res), "", "", "", humanize.Comma(int64(conn)), humanize.Comma(int64(ln)), humanize.IBytes(uint64(sb)), humanize.Comma(sm), humanize.IBytes(uint64(rb)), humanize.Comma(rm), humanize.Comma(sc))
	fmt.Print(table.Render())
	fmt.Println()

	return nil
}

func (c *actCmd) parseAccountStatResp(resp []byte) (*accountStats, error) {
	reqresp := map[string]json.RawMessage{}
	err := json.Unmarshal(resp, &reqresp)
	if err != nil {
		return nil, err
	}

	errresp, ok := reqresp["error"]
	if ok {
		res := map[string]any{}
		err := json.Unmarshal(errresp, &res)
		if err != nil {
			return nil, fmt.Errorf("invalid response received: %q", errresp)
		}

		msg, ok := res["description"]
		if !ok {
			return nil, fmt.Errorf("invalid response received: %q", errresp)
		}

		return nil, fmt.Errorf("invalid response received: %v", msg)
	}

	data, ok := reqresp["data"]
	if !ok {
		return nil, fmt.Errorf("no data received in response: %#v", reqresp)
	}

	sz := &accountStats{Stats: &server.AccountStatz{}, ServerInfo: &server.ServerInfo{}}
	s, ok := reqresp["server"]
	if !ok {
		return nil, fmt.Errorf("no server data received in response: %#v", reqresp)
	}
	err = json.Unmarshal(s, sz.ServerInfo)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, sz.Stats)
	if err != nil {
		return nil, err
	}

	return sz, nil
}

type accountStats struct {
	Stats      *server.AccountStatz
	ServerInfo *server.ServerInfo
}

func (c *actCmd) renderTier(name string, tier api.JetStreamTier) {
	fmt.Printf("   Tier: %s\n\n", name)

	fmt.Printf("      Configuration Requirements:\n\n")
	fmt.Printf("         Stream Requires Max Bytes Set: %t\n", tier.Limits.MaxBytesRequired)
	if tier.Limits.MaxAckPending <= 0 {
		fmt.Printf("          Consumer Maximum Ack Pending: Unlimited\n")
	} else {
		fmt.Printf("          Consumer Maximum Ack Pending: %s\n", humanize.Comma(int64(tier.Limits.MaxAckPending)))
	}
	fmt.Println()

	fmt.Printf("      Stream Resource Usage Limits:\n\n")

	if tier.Limits.MaxMemory == -1 {
		fmt.Printf("                    Memory: %s of Unlimited\n", humanize.IBytes(tier.Memory))
	} else {
		fmt.Printf("                    Memory: %s of %s\n", humanize.IBytes(tier.Memory), humanize.IBytes(uint64(tier.Limits.MaxMemory)))
	}

	if tier.Limits.MemoryMaxStreamBytes <= 0 {
		fmt.Printf("         Memory Per Stream: Unlimited\n")
	} else {
		fmt.Printf("         Memory Per Stream: %s\n", humanize.IBytes(uint64(tier.Limits.MemoryMaxStreamBytes)))
	}

	if tier.Limits.MaxStore == -1 {
		fmt.Printf("                   Storage: %s of Unlimited\n", humanize.IBytes(tier.Store))
	} else {
		fmt.Printf("                   Storage: %s of %s\n", humanize.IBytes(tier.Store), humanize.IBytes(uint64(tier.Limits.MaxStore)))
	}

	if tier.Limits.StoreMaxStreamBytes <= 0 {
		fmt.Printf("        Storage Per Stream: Unlimited\n")
	} else {
		fmt.Printf("        Storage Per Stream: %s\n", humanize.IBytes(uint64(tier.Limits.StoreMaxStreamBytes)))
	}

	if tier.Limits.MaxStreams == -1 {
		fmt.Printf("                   Streams: %s of Unlimited\n", humanize.Comma(int64(tier.Streams)))
	} else {
		fmt.Printf("                   Streams: %s of %s\n", humanize.Comma(int64(tier.Streams)), humanize.Comma(int64(tier.Limits.MaxStreams)))
	}

	if tier.Limits.MaxConsumers == -1 {
		fmt.Printf("                 Consumers: %s of Unlimited\n", humanize.Comma(int64(tier.Consumers)))
	} else {
		fmt.Printf("                 Consumers: %s of %s\n", humanize.Comma(int64(tier.Consumers)), humanize.Comma(int64(tier.Limits.MaxConsumers)))
	}

	fmt.Println()
}

func (c *actCmd) infoAction(_ *fisk.ParseContext) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	id, _ := nc.GetClientID()
	ip, _ := nc.GetClientIP()
	rtt, _ := nc.RTT()
	tlsc, _ := nc.TLSConnectionState()

	var ui *server.UserInfo
	if serverMinVersion(nc.ConnectedServerVersion(), 2, 10, 0) {
		subj := "$SYS.REQ.USER.INFO"
		if opts.Trace {
			log.Printf(">>> %s: {}\n", subj)
		}
		resp, err := nc.Request("$SYS.REQ.USER.INFO", nil, time.Second)
		if err == nil {
			if opts.Trace {
				log.Printf("<<< %s", string(resp.Data))
			}
			var res = struct {
				Data   *server.UserInfo  `json:"data"`
				Server server.ServerInfo `json:"server"`
				Error  *server.ApiError  `json:"error"`
			}{}

			err = json.Unmarshal(resp.Data, &res)
			if err == nil && res.Error == nil {
				ui = res.Data
			}
		}
	}

	fmt.Println("Connection Information:")
	fmt.Println()
	if ui != nil {
		fmt.Printf("                    User: %v\n", ui.UserID)
		fmt.Printf("                 Account: %v\n", ui.Account)
		if ui.Expires == 0 {
			fmt.Printf("                 Expires: never\n")
		} else {
			fmt.Printf("                 Expires: %s\n", humanizeDuration(ui.Expires))
		}
	}
	fmt.Printf("               Client ID: %v\n", id)
	fmt.Printf("               Client IP: %v\n", ip)
	fmt.Printf("                     RTT: %v\n", rtt)
	fmt.Printf("       Headers Supported: %v\n", nc.HeadersSupported())
	fmt.Printf("         Maximum Payload: %v\n", humanize.IBytes(uint64(nc.MaxPayload())))
	if nc.ConnectedClusterName() != "" {
		fmt.Printf("       Connected Cluster: %s\n", nc.ConnectedClusterName())
	}
	fmt.Printf("           Connected URL: %v\n", nc.ConnectedUrl())
	fmt.Printf("       Connected Address: %v\n", nc.ConnectedAddr())
	fmt.Printf("     Connected Server ID: %v\n", nc.ConnectedServerId())
	if nc.ConnectedServerId() != nc.ConnectedServerName() {
		fmt.Printf("   Connected Server Name: %v\n", nc.ConnectedServerName())
	}

	if tlsc.HandshakeComplete {
		version := ""
		switch tlsc.Version {
		case tls.VersionTLS10:
			version = "1.0"
		case tls.VersionTLS11:
			version = "1.1"
		case tls.VersionTLS12:
			version = "1.2"
		case tls.VersionTLS13:
			version = "1.3"
		default:
			version = fmt.Sprintf("unknown (%x)", tlsc.Version)
		}

		fmt.Printf("             TLS Version: %s using %s\n", version, tls.CipherSuiteName(tlsc.CipherSuite))
		fmt.Printf("              TLS Server: %v\n", tlsc.ServerName)
		if len(tlsc.VerifiedChains) > 0 {
			fmt.Printf("            TLS Verified: issuer %s\n", tlsc.PeerCertificates[0].Issuer.String())
		} else {
			fmt.Printf("            TLS Verified: no\n")
		}
	} else {
		fmt.Printf("          TLS Connection: no\n")
	}
	fmt.Println()

	renderPerm := func(title string, p *server.SubjectPermission) {
		if p == nil {
			return
		}
		if len(p.Deny) == 0 && len(p.Allow) == 0 {
			return
		}

		fmt.Printf("  %s\n", title)
		if len(p.Allow) > 0 {
			fmt.Println("    Allow:")
			sort.Strings(p.Allow)
			for _, perm := range p.Allow {
				fmt.Printf("      %s\n", perm)
			}
		}
		if len(p.Deny) > 0 {
			fmt.Println()
			fmt.Println("    Deny:")
			sort.Strings(p.Deny)
			for _, perm := range p.Deny {
				fmt.Printf("      %s\n", perm)
			}
		}
	}

	if ui != nil && ui.Permissions != nil {
		fmt.Println("Connection Permissions:")
		fmt.Println()
		renderPerm("Publish", ui.Permissions.Publish)
		fmt.Println()
		renderPerm("Subscribe", ui.Permissions.Subscribe)
		fmt.Println()
	}

	info, err := mgr.JetStreamAccountInfo()

	if err == nil {
		if info.Domain == "" {
			fmt.Println("JetStream Account Information:")
		} else {
			fmt.Printf("JetStream Account Information for domain %s:\n", info.Domain)
		}
	}

	fmt.Println()
	switch err {
	case nil:
		fmt.Printf("Account Usage:\n\n")
		if info.Domain != "" {
			fmt.Printf("     Domain: %s\n", info.Domain)
		}
		fmt.Printf("    Storage: %s\n", humanize.IBytes(info.Store))
		fmt.Printf("     Memory: %s\n", humanize.IBytes(info.Memory))
		fmt.Printf("    Streams: %s\n", humanize.Comma(int64(info.Streams)))
		fmt.Printf("  Consumers: %s\n", humanize.Comma(int64(info.Consumers)))
		fmt.Println()

		fmt.Printf("Account Limits:\n\n")

		fmt.Printf("   Max Message Payload: %s \n\n", humanize.IBytes(uint64(nc.MaxPayload())))

		if len(info.Tiers) > 0 {
			var tiers []string
			for n := range info.Tiers {
				tiers = append(tiers, n)
			}
			sort.Strings(tiers)

			for _, n := range tiers {
				c.renderTier(n, info.Tiers[n])
			}
		} else {
			c.renderTier("Default", info.JetStreamTier)
		}

	case context.DeadlineExceeded:
		fmt.Printf("   No response from JetStream server")
	case nats.ErrNoResponders:
		fmt.Printf("   JetStream is not supported in this account")
	default:
		fmt.Printf("   Could not obtain account information: %s", err)
	}

	fmt.Println()

	return nil
}

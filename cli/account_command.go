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
	"fmt"
	"os"
	"path/filepath"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type actCmd struct {
	sort    string
	subject string
	topk    int

	backupDirectory   string
	healthCheck       bool
	snapShotConsumers bool
	force             bool

	placementCluster string
	placementTags    []string
}

func configureActCommand(app commandHost) {
	c := &actCmd{}
	act := app.Command("account", "Account information and status").Alias("a")
	act.Command("info", "Account information").Alias("nfo").Action(c.infoAction)

	report := act.Command("report", "Report on account metrics").Alias("rep")

	conns := report.Command("connections", "Report on connections").Alias("conn").Alias("connz").Alias("conns").Action(c.reportConnectionsAction)
	conns.Flag("sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)").Default("subs").EnumVar(&c.sort, "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs")
	conns.Flag("top", "Limit results to the top results").Default("1000").IntVar(&c.topk)
	conns.Flag("subject", "Limits responses only to those connections with matching subscription interest").StringVar(&c.subject)

	backup := act.Command("backup", "Creates a backup of all  JetStream Streams over the NATS network").Alias("snapshot").Action(c.backupAction)
	backup.Arg("target", "Directory to create the backup in").Required().StringVar(&c.backupDirectory)
	backup.Flag("check", "Checks the Stream for health prior to backup").Default("false").BoolVar(&c.healthCheck)
	backup.Flag("consumers", "Enable or disable consumer backups").Default("true").BoolVar(&c.snapShotConsumers)
	backup.Flag("force", "Perform backup without prompting").Short('f').BoolVar(&c.force)

	restore := act.Command("restore", "Restore an account backup over the NATS network").Action(c.restoreAction)
	restore.Arg("directory", "The directory holding the account backup to restore").Required().ExistingDirVar(&c.backupDirectory)
	restore.Flag("cluster", "Place the stream in a specific cluster").StringVar(&c.placementCluster)
	restore.Flag("tag", "Place the stream on servers that has specific tags (pass multiple times)").StringsVar(&c.placementTags)

	cheats["account"] = `# To view account information and connection
nats account info

# To report connections for your command
nats account report connections

# To backup all JetStream streams
nats account backup /path/to/backup --check
`
}

func init() {
	registerCommand("account", 0, configureActCommand)
}

func (c *actCmd) backupAction(_ *kingpin.ParseContext) error {
	var err error

	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	streams, err := mgr.Streams()
	if err != nil {
		return err
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

	var errors []error
	for _, s := range streams {
		err = backupStream(s, false, c.snapShotConsumers, c.healthCheck, filepath.Join(c.backupDirectory, s.Name()))
		if err != nil {
			fmt.Printf("Backup of %s failed: %s\n", s.Name(), err)
			errors = append(errors, fmt.Errorf("%s: %s", s.Name(), err))
		}
		fmt.Println()
	}

	if len(errors) > 0 {
		fmt.Printf("Backup failures: \n")
		for _, err := range errors {
			fmt.Printf("  %s", err)
		}
		return fmt.Errorf("backup failed")
	}

	return nil
}

func (c *actCmd) restoreAction(kp *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")
	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return err
	}
	existingStreams := map[string]struct{}{}
	for _, n := range streams {
		existingStreams[n] = struct{}{}
	}
	de, err := os.ReadDir(c.backupDirectory)
	kingpin.FatalIfError(err, "setup failed")
	for _, d := range de {
		if !d.IsDir() {
			kingpin.FatalIfError(err, "expected a directory")
		}
		if _, ok := existingStreams[d.Name()]; ok {
			kingpin.Fatalf("stream %q exists already", d.Name())
		}
		_, err := os.Stat(filepath.Join(c.backupDirectory, d.Name(), "backup.json"))
		kingpin.FatalIfError(err, "expected backup.json")
	}
	fmt.Printf("Restoring backup of all %d streams in directory %q\n\n", len(de), c.backupDirectory)
	s := &streamCmd{msgID: -1, showProgress: false, placementCluster: c.placementCluster, placementTags: c.placementTags}
	for _, d := range de {
		s.backupDirectory = filepath.Join(c.backupDirectory, d.Name())
		err := s.restoreAction(kp)
		kingpin.FatalIfError(err, "restore for %s failed", d.Name())
	}
	return nil
}

func (c *actCmd) reportConnectionsAction(pc *kingpin.ParseContext) error {
	cmd := SrvReportCmd{
		topk:    c.topk,
		sort:    c.sort,
		subject: c.subject,
	}

	return cmd.reportConnections(pc)
}

func (c *actCmd) renderTier(name string, tier *api.JetStreamTier) {
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

func (c *actCmd) infoAction(_ *kingpin.ParseContext) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	kingpin.FatalIfError(err, "setup failed")

	id, _ := nc.GetClientID()
	ip, _ := nc.GetClientIP()
	rtt, _ := nc.RTT()

	fmt.Println("Connection Information:")
	fmt.Println()
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

	fmt.Println()

	info, err := mgr.JetStreamAccountInfo()

	fmt.Println("JetStream Account Information:")
	fmt.Println()
	switch err {
	case nil:
		tiered := len(info.Tiers) > 0

		fmt.Printf("Account Usage:\n\n")
		fmt.Printf("    Storage: %s\n", humanize.IBytes(info.Store))
		fmt.Printf("     Memory: %s\n", humanize.IBytes(info.Memory))
		fmt.Printf("    Streams: %s\n", humanize.Comma(int64(info.Streams)))
		fmt.Printf("  Consumers: %s\n", humanize.Comma(int64(info.Consumers)))
		fmt.Println()

		fmt.Printf("Account Limits:\n\n")

		fmt.Printf("   Max Message Payload: %s \n\n", humanize.IBytes(uint64(nc.MaxPayload())))

		if tiered {
			for n, t := range info.Tiers {
				c.renderTier(n, &t)
			}
		} else {
			c.renderTier("Default", &info.JetStreamTier)
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

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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvCheckCmd struct {
	connectWarning  time.Duration
	connectCritical time.Duration
	rttWarning      time.Duration
	rttCritical     time.Duration
	reqWarning      time.Duration
	reqCritical     time.Duration

	sourcesStream       string
	sourcesLagCritical  uint64
	sourcesSeenCritical time.Duration
	sourcesMinSources   int
	sourcesMaxSources   int
	sourcesMessagesWarn uint64
	sourcesMessagesCrit uint64

	raftExpect       int
	raftLagCritical  uint64
	raftSeenCritical time.Duration
}

func configureServerCheckCommand(srv *kingpin.CmdClause) {
	c := &SrvCheckCmd{}

	help := `Nagios protocol health check for NATS servers

   connection  - connects and does a request-reply check
   stream      - checks JetStream streams for source, mirror and cluster health
   meta        - JetStream Meta Cluster health
`
	check := srv.Command("check", help)

	conn := check.Command("connection", "Checks basic server connection").Alias("conn").Default().Action(c.checkConnection)
	conn.Flag("connect-warn", "Warning threshold to allow for establishing connections").Default("500ms").PlaceHolder("DURATION").DurationVar(&c.connectWarning)
	conn.Flag("connect-critical", "Critical threshold to allow for establishing connections").Default("1s").PlaceHolder("DURATION").DurationVar(&c.connectCritical)
	conn.Flag("rtt-warn", "Warning threshold to allow for server RTT").Default("500ms").PlaceHolder("DURATION").DurationVar(&c.rttWarning)
	conn.Flag("rtt-critical", "Critical threshold to allow for server RTT").Default("1s").PlaceHolder("DURATION").DurationVar(&c.rttCritical)
	conn.Flag("req-warn", "Warning threshold to allow for full round trip test").PlaceHolder("DURATION").Default("500ms").DurationVar(&c.reqWarning)
	conn.Flag("req-critical", "Critical threshold to allow for full round trip test").PlaceHolder("DURATION").Default("1s").DurationVar(&c.reqCritical)

	stream := check.Command("stream", "Checks the health of mirrored streams, streams with sources or clustered streams").Action(c.checkStream)
	stream.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	stream.Flag("lag-critical", "Critical threshold to allow for lag on any source or mirror").PlaceHolder("MSGS").Uint64Var(&c.sourcesLagCritical)
	stream.Flag("seen-critical", "Critical threshold for how long ago the source or mirror should have been seen").PlaceHolder("DURATION").DurationVar(&c.sourcesSeenCritical)
	stream.Flag("min-sources", "Minimum number of sources to expect").PlaceHolder("SOURCES").Default("1").IntVar(&c.sourcesMinSources)
	stream.Flag("max-sources", "Maximum number of sources to expect").PlaceHolder("SOURCES").Default("1").IntVar(&c.sourcesMaxSources)
	stream.Flag("peer-expect", "Number of cluster replicas to expect").Required().PlaceHolder("SERVERS").IntVar(&c.raftExpect)
	stream.Flag("peer-lag-critical", "Critical threshold to allow for cluster peer lag").PlaceHolder("OPS").Uint64Var(&c.raftLagCritical)
	stream.Flag("peer-seen-critical", "Critical threshold for how long ago a cluster peer should have been seen").PlaceHolder("DURATION").Default("10s").DurationVar(&c.raftSeenCritical)
	stream.Flag("msgs-warn", "Warn if there are fewer than this many messages in the stream").PlaceHolder("MSGS").Uint64Var(&c.sourcesMessagesWarn)
	stream.Flag("msgs-crit", "Critical if there are fewer than this many messages in the stream").PlaceHolder("MSGS").Uint64Var(&c.sourcesMessagesCrit)

	meta := check.Command("meta", "Check JetStream cluster state").Alias("raft").Action(c.checkRaft)
	meta.Flag("expect", "Number of servers to expect").Required().PlaceHolder("SERVERS").IntVar(&c.raftExpect)
	meta.Flag("lag-critical", "Critical threshold to allow for lag").PlaceHolder("OPS").Required().Uint64Var(&c.raftLagCritical)
	meta.Flag("seen-critical", "Critical threshold for how long ago a peer should have been seen").Required().PlaceHolder("DURATION").DurationVar(&c.raftSeenCritical)
}

func (c *SrvCheckCmd) ok(format string, a ...interface{}) {
	fmt.Printf("OK: "+format, a...)
	fmt.Println()
	os.Exit(0)
}

func (c *SrvCheckCmd) warning(format string, a ...interface{}) {
	fmt.Printf("WARNING: "+format, a...)
	fmt.Println()
	os.Exit(1)
}

func (c *SrvCheckCmd) critical(format string, a ...interface{}) {
	fmt.Printf("CRITICAL: "+format, a...)
	fmt.Println()
	os.Exit(2)
}

func (c *SrvCheckCmd) criticalIfErr(err error, format string, a ...interface{}) {
	if err == nil {
		return
	}

	c.critical(format, a...)
}

func (c *SrvCheckCmd) checkRaft(_ *kingpin.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	c.criticalIfErr(err, "connection failed: %s", err)

	res, err := doReq(&server.JSzOptions{LeaderOnly: true}, "$SYS.REQ.SERVER.PING.JSZ", 1, nc)
	c.criticalIfErr(err, "JSZ API request failed: %s", err)

	if len(res) != 1 {
		c.critical("JSZ API request returned %d results", len(res))
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	jszresp := &jszr{}
	err = json.Unmarshal(res[0], jszresp)
	c.criticalIfErr(err, "invalid result received: %s", err)

	criticals, pd, err := c.checkClusterInfo(jszresp.Data.Meta)
	c.criticalIfErr(err, "invalid result received: %s", err)

	if len(criticals) > 0 {
		c.critical("JetStream Cluster %s | %s", strings.Join(criticals, ", "), pd)
	}

	c.ok("JetStream Cluster with %d peers led by %s | %s", len(jszresp.Data.Meta.Replicas)+1, jszresp.Data.Meta.Leader, pd)

	return nil
}

func (c *SrvCheckCmd) checkClusterInfo(ci *server.ClusterInfo) (criticals []string, pd string, err error) {
	if ci == nil {
		return []string{"no cluster information"}, "", nil
	}

	if ci.Leader == "" {
		return []string{"No leader"}, "", nil
	}

	pd = fmt.Sprintf("peers=%d;%d;%d ", len(ci.Replicas)+1, c.raftExpect, c.raftExpect)
	if len(ci.Replicas)+1 != c.raftExpect {
		criticals = append(criticals, fmt.Sprintf("%d peers of expected %d", len(ci.Replicas)+1, c.raftExpect))
	}

	notCurrent := 0
	inactive := 0
	offline := 0
	lagged := 0
	for _, peer := range ci.Replicas {
		if !peer.Current {
			notCurrent++
		}
		if peer.Offline {
			offline++
		}
		if peer.Active > c.raftSeenCritical {
			inactive++
		}
		if peer.Lag > c.raftLagCritical {
			lagged++
		}
	}

	pd = fmt.Sprintf("%s offline=%d not_current=%d inactive=%d lagged=%d", pd, offline, notCurrent, inactive, lagged)
	if notCurrent > 0 {
		criticals = append(criticals, fmt.Sprintf("%d not current", notCurrent))
	}
	if inactive > 0 {
		criticals = append(criticals, fmt.Sprintf("%d inactive more than %s", inactive, c.raftSeenCritical))
	}
	if offline > 0 {
		criticals = append(criticals, fmt.Sprintf("%d offline", offline))
	}
	if lagged > 0 {
		criticals = append(criticals, fmt.Sprintf("%d lagged more than %d ops", lagged, c.raftLagCritical))
	}

	return criticals, pd, nil
}

func (c *SrvCheckCmd) checkStream(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	c.criticalIfErr(err, "connection failed: %s", err)

	stream, err := mgr.LoadStream(c.sourcesStream)
	c.criticalIfErr(err, "could not load stream %s: %s", c.sourcesStream, err)

	info, err := stream.LatestInformation()
	c.criticalIfErr(err, "could not load stream %s info: %s", c.sourcesStream, err)

	var (
		criticals []string
		warnings  []string
		pd        string
		msg       string
		cmsg      string
	)

	if info.Cluster != nil {
		var sci server.ClusterInfo
		cij, _ := json.Marshal(info.Cluster)
		json.Unmarshal(cij, &sci)
		criticals, pd, err = c.checkClusterInfo(&sci)
		c.criticalIfErr(err, "Invalid cluster data: %s", err)

		pd = strings.TrimSpace(pd)
		if len(criticals) == 0 {
			cmsg = fmt.Sprintf(", %d current replicas", len(info.Cluster.Replicas)+1)
		}
	} else if c.raftExpect > 0 {
		criticals = append(criticals, fmt.Sprintf("not clustered expected %d peers", c.raftExpect))
	}

	mpd := fmt.Sprintf("messages=%d", info.State.Msgs)
	if c.sourcesMessagesWarn > 0 && info.State.Msgs <= c.sourcesMessagesWarn {
		warnings = append(warnings, fmt.Sprintf("%d messages", info.State.Msgs))
		mpd += fmt.Sprintf(";%d", c.sourcesMessagesWarn)
	} else {
		mpd += ";"
	}
	if c.sourcesMessagesCrit > 0 && info.State.Msgs <= c.sourcesMessagesCrit {
		criticals = append(criticals, fmt.Sprintf("%d messages", info.State.Msgs))
		mpd += fmt.Sprintf(";%d", c.sourcesMessagesCrit)
	} else {
		mpd += ";"
	}
	pd += " " + mpd

	switch {
	case stream.IsMirror():
		mcrit, mpd, err := c.checkMirror(info)
		c.criticalIfErr(err, "Invalid mirror data: %s", err)

		criticals = append(criticals, mcrit...)
		pd = pd + " " + mpd

		if len(criticals) == 0 {
			msg = fmt.Sprintf("%s mirror of %s is %d lagged, last seen %s ago%s | %s", c.sourcesStream, info.Mirror.Name, info.Mirror.Lag, info.Mirror.Active.Round(time.Millisecond), cmsg, pd)
		}

	case stream.IsSourced():
		scrit, spd, err := c.checkSources(info)
		c.criticalIfErr(err, "Invalid source data: %s", err)

		criticals = append(criticals, scrit...)
		pd = pd + " " + spd

		if len(criticals) == 0 {
			msg = fmt.Sprintf("%s with %d sources%s | %s", c.sourcesStream, len(info.Sources), cmsg, pd)
		}

	default:
		msg = fmt.Sprintf("%s%s | %s", c.sourcesStream, cmsg, pd)
	}

	switch {
	case len(criticals) > 0:
		c.critical("%s %s%s | %s", c.sourcesStream, strings.Join(criticals, ", "), cmsg, pd)
	case len(warnings) > 0:
		c.warning("%s %s%s | %s", c.sourcesStream, strings.Join(warnings, ", "), cmsg, pd)
	default:
		c.ok(msg)
	}

	return nil
}

func (c *SrvCheckCmd) checkMirror(info *api.StreamInfo) (criticals []string, pd string, err error) {
	if info.Mirror == nil {
		return []string{"not mirrored"}, "", nil
	}

	pd = fmt.Sprintf("lag=%d;;%d active=%fs;;%.2f", info.Mirror.Lag, c.sourcesLagCritical, info.Mirror.Active.Seconds(), c.sourcesSeenCritical.Seconds())

	if c.sourcesLagCritical > 0 && info.Mirror.Lag > c.sourcesLagCritical {
		criticals = append(criticals, fmt.Sprintf("%d messages behind", info.Mirror.Lag))
	}

	if c.sourcesSeenCritical > 0 && info.Mirror.Active > c.sourcesSeenCritical {
		criticals = append(criticals, fmt.Sprintf("last active %s", info.Mirror.Active))
	}

	return criticals, pd, nil
}

func (c *SrvCheckCmd) checkSources(info *api.StreamInfo) (criticals []string, pd string, err error) {
	pd = fmt.Sprintf("sources=%d;%d;%d; ", len(info.Sources), c.sourcesMinSources, c.sourcesMaxSources)

	if len(info.Sources) == 0 {
		return []string{"no sources defined"}, "", nil
	}

	lagged := 0
	inactive := 0

	for _, s := range info.Sources {
		if c.sourcesLagCritical > 0 && s.Lag > c.sourcesLagCritical {
			lagged++
		}

		if c.sourcesSeenCritical > 0 && s.Active > c.sourcesSeenCritical {
			inactive++
		}
	}

	pd = pd + fmt.Sprintf("lagged=%d inactive=%d", lagged, inactive)

	if lagged > 0 {
		criticals = append(criticals, fmt.Sprintf("%d lagged sources", lagged))
	}
	if inactive > 0 {
		criticals = append(criticals, fmt.Sprintf("%d inactive sources", inactive))
	}
	if len(info.Sources) < c.sourcesMinSources {
		criticals = append(criticals, fmt.Sprintf("%d sources of min expected %d", len(info.Sources), c.sourcesMinSources))
	}
	if len(info.Sources) > c.sourcesMaxSources {
		criticals = append(criticals, fmt.Sprintf("%d sources of max expected %d", len(info.Sources), c.sourcesMaxSources))
	}

	return criticals, pd, nil
}

func (c *SrvCheckCmd) checkConnection(_ *kingpin.ParseContext) error {
	connStart := time.Now()
	nc, _, err := prepareHelper("", natsOpts()...)
	c.criticalIfErr(err, "connection failed")

	ct := time.Since(connStart)
	pd := fmt.Sprintf("connect_time=%fs;%.2f;%.2f ", ct.Seconds(), c.connectWarning.Seconds(), c.connectCritical.Seconds())

	if ct >= c.connectCritical {
		c.critical("connected to %s, connect time exceeded %v | %s", nc.ConnectedUrl(), c.connectCritical, pd)
	} else if ct >= c.connectWarning {
		c.warning("connected to %s, connect time exceeded %v | %s", nc.ConnectedUrl(), c.connectWarning, pd)
	}

	rtt, err := nc.RTT()
	c.criticalIfErr(err, "connected to %s, rtt failed | %s", nc.ConnectedUrl(), pd)

	pd += fmt.Sprintf("rtt=%fs;%.2f;%.2f ", rtt.Seconds(), c.rttWarning.Seconds(), c.rttCritical.Seconds())
	if rtt >= c.rttCritical {
		c.critical("connect time exceeded %v | %s", c.connectCritical, pd)
	} else if rtt >= c.rttWarning {
		c.warning("rtt time exceeded %v | %s", c.connectCritical, pd)
	}

	msg := []byte(randomPassword(100))
	ib := nats.NewInbox()
	sub, err := nc.SubscribeSync(ib)
	c.criticalIfErr(err, "could not subscribe to %s: %s | %s", ib, err, pd)
	sub.AutoUnsubscribe(1)

	start := time.Now()
	err = nc.Publish(ib, msg)
	c.criticalIfErr(err, "could not publish to %s: %s | %s", ib, err, pd)

	received, err := sub.NextMsg(timeout)
	c.criticalIfErr(err, "did not receive from %s: %s | %s", ib, err, pd)

	reqt := time.Since(start)
	pd += fmt.Sprintf("request_time=%fs;%.2f;%.2f", reqt.Seconds(), c.reqWarning.Seconds(), c.reqCritical.Seconds())

	if !bytes.Equal(received.Data, msg) {
		c.critical("did not receive expected message on %s | %s", nc.ConnectedUrl(), pd)
	}

	if reqt >= c.reqCritical {
		c.critical("round trip request took %f | %s", reqt.Seconds(), pd)
	} else if reqt >= c.reqWarning {
		c.warning("round trip request took %f | %s", reqt.Seconds(), pd)
	}

	fmt.Printf("OK: connected to %s | %s\n", nc.ConnectedUrl(), pd)

	return nil
}

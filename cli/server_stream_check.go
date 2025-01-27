// Copyright 2024 The NATS Authors
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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/sysclient"
)

type (
	streamDetail struct {
		StreamName   string
		Account      string
		AccountID    string
		RaftGroup    string
		State        server.StreamState
		Cluster      *server.ClusterInfo
		HealthStatus string
		ServerID     string
	}

	StreamCheckCmd struct {
		raftGroup      string
		streamName     string
		unsyncedFilter bool
		health         bool
		expected       int
		stdin          bool
		readTimeout    int
		csv            bool
	}
)

func configureStreamCheckCommand(app commandHost) {
	sc := &StreamCheckCmd{}
	streamCheck := app.Command("stream-check", "Check and display stream information").Action(sc.streamCheck).Hidden()
	streamCheck.Flag("stream", "Filter results by stream").StringVar(&sc.streamName)
	streamCheck.Flag("raft-group", "Filter results by raft group").StringVar(&sc.raftGroup)
	streamCheck.Flag("health", "Check health from streams").UnNegatableBoolVar(&sc.health)
	streamCheck.Flag("expected", "Expected number of servers").IntVar(&sc.expected)
	streamCheck.Flag("unsynced", "Filter results by streams that are out of sync").UnNegatableBoolVar(&sc.unsyncedFilter)
	streamCheck.Flag("stdin", "Process the contents from STDIN").UnNegatableBoolVar(&sc.stdin)
	streamCheck.Flag("read-timeout", "Read timeout in seconds").Default("5").IntVar(&sc.readTimeout)
	streamCheck.Flag("csv", "Renders CSV format").UnNegatableBoolVar(&sc.csv)
}

func (c *StreamCheckCmd) streamCheck(_ *fisk.ParseContext) error {
	start := time.Now()

	var nc *nats.Conn
	var err error

	if !c.stdin {
		nc, _, err = prepareHelper(opts().Servers, natsOpts()...)
		if err != nil {
			return err
		}
		fmt.Printf("Connected in %.3fs\n", time.Since(start).Seconds())

		if c.expected == 0 {
			c.expected, err = currentActiveServers(nc)
			if err != nil {
				return fmt.Errorf("failed to get current active servers: %s", err)
			}
		}
	}

	sys := sysclient.New(nc)

	start = time.Now()
	servers, err := sys.FindServers(c.stdin, c.expected, opts().Timeout, time.Duration(c.readTimeout), false)
	if err != nil {
		return fmt.Errorf("failed to find servers: %s", err)
	}

	if !c.csv {
		fmt.Printf("Response took %.3fs\n", time.Since(start).Seconds())
		fmt.Printf("Servers: %d\n", len(servers))
	}

	// Collect all info from servers.
	streams := make(map[string]map[string]*streamDetail)
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var ok bool
				var m map[string]*streamDetail
				key := fmt.Sprintf("%s|%s", acc.Name, stream.RaftGroup)
				if m, ok = streams[key]; !ok {
					m = make(map[string]*streamDetail)
					streams[key] = m
				}
				m[server.Name] = &streamDetail{
					ServerID:   server.ID,
					StreamName: stream.Name,
					Account:    acc.Name,
					AccountID:  acc.Id,
					RaftGroup:  stream.RaftGroup,
					State:      stream.State,
					Cluster:    stream.Cluster,
				}
			}
		}
	}
	keys := make([]string, 0)
	for k := range streams {
		for kk := range streams[k] {
			keys = append(keys, fmt.Sprintf("%s/%s", k, kk))
		}
	}
	sort.Strings(keys)

	title := ""
	if !c.csv {
		fmt.Printf("Streams: %d\n", len(keys))
		title = "Streams"
	}

	table := util.NewTableWriter(opts(), title)
	if c.health {
		table.AddHeaders("Stream Replica", "Raft", "Account", "Account ID", "Node", "Messages", "Bytes", "Subjects", "Deleted", "Consumers", "First", "Last", "Status", "Leader", "Peers", "Health")
	} else {
		table.AddHeaders("Stream Replica", "Raft", "Account", "Account ID", "Node", "Messages", "Bytes", "Subjects", "Deleted", "Consumers", "First", "Last", "Status", "Leader", "Peers")
	}

	var prev, prevAccount string
	for i, k := range keys {
		var unsynced bool
		av := strings.Split(k, "|")
		accName := av[0]
		v := strings.Split(av[1], "/")
		raftName, serverName := v[0], v[1]
		if c.raftGroup != "" && raftName != c.raftGroup {
			continue
		}

		key := fmt.Sprintf("%s|%s", accName, raftName)
		stream := streams[key]
		replica := stream[serverName]
		status := "IN SYNC"

		if c.streamName != "" && replica.StreamName != c.streamName {
			continue
		}

		// Make comparisons against other peers.
		for _, peer := range stream {
			if peer.State.Msgs != replica.State.Msgs && peer.State.Bytes != replica.State.Bytes {
				status = "UNSYNCED"
				unsynced = true
			}
			if peer.State.FirstSeq != replica.State.FirstSeq {
				status = "UNSYNCED"
				unsynced = true
			}
			if peer.State.LastSeq != replica.State.LastSeq {
				status = "UNSYNCED"
				unsynced = true
			}
			// Cannot trust results unless coming from the stream leader.
			// Need Stream INFO and collect multiple responses instead.
			if peer.Cluster.Leader != replica.Cluster.Leader {
				status = "MULTILEADER"
				unsynced = true
			}
		}
		if c.unsyncedFilter && !unsynced {
			continue
		}

		if replica == nil {
			status = "?"
			unsynced = true
			continue
		}
		var alen int
		if len(replica.Account) > 10 {
			alen = 10
		} else {
			alen = len(replica.Account)
		}

		account := strings.Replace(replica.Account[:alen], " ", "_", -1)

		// Mark it in case it is a leader.
		var suffix string
		var isStreamLeader bool
		if serverName == replica.Cluster.Leader {
			isStreamLeader = true
			suffix = "*"
		} else if replica.Cluster.Leader == "" {
			status = "LEADERLESS"
			unsynced = true
		}

		var replicasInfo string // PEER
		for _, r := range replica.Cluster.Replicas {
			if isStreamLeader && r.Name == replica.Cluster.Leader {
				status = "LEADER_IS_FOLLOWER"
				unsynced = true
			}
			info := fmt.Sprintf("%s(current=%-5v,offline=%v)", r.Name, r.Current, r.Offline)
			replicasInfo = fmt.Sprintf("%-40s %s", info, replicasInfo)
		}

		// Include Healthz if option added.
		var healthStatus string
		if c.health {
			hstatus, err := sys.Healthz(replica.ServerID, server.HealthzOptions{
				Account: replica.Account,
				Stream:  replica.StreamName,
			})
			if err != nil {
				healthStatus = err.Error()
			} else {
				healthStatus = fmt.Sprintf(":%s:%s", hstatus.Healthz.Status, hstatus.Healthz.Error)
			}
		}

		if i > 0 && prev != replica.StreamName || prevAccount != accName {
			table.AddSeparator()
		}

		prev = replica.StreamName
		prevAccount = accName

		table.AddRow(replica.StreamName, replica.RaftGroup, account, replica.AccountID, fmt.Sprintf("%s%s", serverName, suffix), replica.State.Msgs, replica.State.Bytes, replica.State.NumSubjects, replica.State.NumDeleted, replica.State.Consumers, replica.State.FirstSeq,
			replica.State.LastSeq, status, replica.Cluster.Leader, strings.TrimSpace(replicasInfo), healthStatus)
	}

	if c.csv {
		fmt.Println(table.RenderCSV())
	} else {
		fmt.Println(table.Render())
	}

	return nil
}

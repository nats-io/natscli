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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
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
		archivePath    string
	}
)

func configureStreamCheckCommand(app commandHost) {
	sc := &StreamCheckCmd{}
	streamCheck := app.Command("stream-check", "Check and display stream information").Action(sc.streamCheck).Hidden()
	streamCheck.Tag("scope:system", "impact:ro")
	streamCheck.Flag("stream", "Filter results by stream").StringVar(&sc.streamName)
	streamCheck.Flag("raft-group", "Filter results by raft group").StringVar(&sc.raftGroup)
	streamCheck.Flag("health", "Check health from streams").UnNegatableBoolVar(&sc.health)
	streamCheck.Flag("expected", "Expected number of servers").IntVar(&sc.expected)
	streamCheck.Flag("unsynced", "Filter results by streams that are out of sync").UnNegatableBoolVar(&sc.unsyncedFilter)
	streamCheck.Flag("stdin", "Process the result of 'nats server request jsz --all --config' from STDIN").UnNegatableBoolVar(&sc.stdin)
	streamCheck.Flag("read-timeout", "Read timeout in seconds").Default("5").IntVar(&sc.readTimeout)
	streamCheck.Flag("csv", "Renders CSV format").UnNegatableBoolVar(&sc.csv)
	streamCheck.Flag("archive", "Read data from an archive file").StringVar(&sc.archivePath)
}

func (c *StreamCheckCmd) dataSource(nc *nats.Conn) (serverdata.Source, error) {
	if c.archivePath != "" {
		return serverdata.NewAuditArchive(c.archivePath)
	}
	timeout := opts().Timeout
	if c.readTimeout > 0 {
		timeout = time.Duration(c.readTimeout) * time.Second
	}
	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, timeout, traceLogger())
	}
	return serverdata.NewLive(nc, reqFn, c.expected)
}

func (c *StreamCheckCmd) streamCheck(_ *fisk.ParseContext) error {
	if c.health && c.archivePath != "" {
		return fmt.Errorf("--health requires a live server connection")
	}

	start := time.Now()

	var err error
	var nc *nats.Conn
	var ds serverdata.Source
	var responses []*server.ServerAPIJszResponse

	if c.stdin {
		decoder := json.NewDecoder(os.Stdin)
		for {
			var resp server.ServerAPIJszResponse
			if err := decoder.Decode(&resp); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			responses = append(responses, &resp)
			if c.expected > 0 && len(responses) >= c.expected {
				break
			}
		}
	} else {
		if c.archivePath == "" {
			nc, _, err = prepareHelper(opts().Servers, natsOpts()...)
			if err != nil {
				return err
			}
			fmt.Printf("Connected in %.3fs\n", time.Since(start).Seconds())

			if c.expected == 0 {
				c.expected, err = serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
				if err != nil {
					return fmt.Errorf("failed to get current active servers: %s", err)
				}
			}
		}

		ds, err = c.dataSource(nc)
		if err != nil {
			return err
		}
		defer ds.Close()

		start = time.Now()
		responses, err = ds.Jsz(server.JszEventOptions{
			JSzOptions: server.JSzOptions{Streams: true, RaftGroups: true},
		})
		if err != nil {
			return err
		}
	}

	if len(responses) == 0 {
		return fmt.Errorf("no JSZ responses received")
	}

	if !c.csv {
		fmt.Printf("Response took %.3fs\n", time.Since(start).Seconds())
		fmt.Printf("Servers: %d\n", len(responses))
		if c.expected > 0 && len(responses) < c.expected {
			fmt.Printf("Warning: expected %d responses got %d\n", c.expected, len(responses))
		}
	}

	// Collect all info from servers.
	streams := make(map[string]map[string]*streamDetail)
	for _, resp := range responses {
		if resp.Server == nil || resp.Data == nil {
			continue
		}
		server := resp.Server
		jsz := resp.Data
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var ok bool
				var m map[string]*streamDetail
				if stream.RaftGroup == "" && stream.Cluster != nil {
					stream.RaftGroup = stream.Cluster.RaftGroup
				}
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
			if peer.Cluster.Leader != "" && replica.Cluster.Leader != "" && peer.Cluster.Leader != replica.Cluster.Leader {
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
		} else if replica.RaftGroup == "" {
			status = "MISSING_GROUP"
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
		if c.health && nc != nil {
			payload := server.HealthzEventOptions{
				HealthzOptions: server.HealthzOptions{
					Account: replica.Account,
					Stream:  replica.StreamName,
				},
			}
			subj := fmt.Sprintf("$SYS.REQ.SERVER.%s.HEALTHZ", replica.ServerID)
			raw, err := serverdata.DoReq(ctx, payload, subj, 1, nc, opts().Timeout, traceLogger())
			if err != nil {
				healthStatus = err.Error()
			} else if len(raw) > 0 {
				var resp server.ServerAPIHealthzResponse
				if err := json.Unmarshal(raw[0], &resp); err != nil {
					healthStatus = err.Error()
				} else if resp.Data != nil {
					healthStatus = fmt.Sprintf(":%s:%s", resp.Data.Status, resp.Data.Error)
				}
			}
		}

		if i > 0 && prev != replica.StreamName || prevAccount != accName {
			table.AddSeparator()
		}

		prev = replica.StreamName
		prevAccount = accName

		node := fmt.Sprintf("%s%s", serverName, suffix)

		if c.unsyncedFilter && !isStreamLeader {
			ld := stream[replica.Cluster.Leader]
			table.AddRow(replica.StreamName, replica.RaftGroup, account, replica.AccountID, node,
				util.FmtReplicaDrift(float64(replica.State.Msgs), float64(ld.State.Msgs)),
				util.FmtReplicaDrift(float64(replica.State.Bytes), float64(ld.State.Bytes)),
				util.FmtReplicaDrift(float64(replica.State.NumSubjects), float64(ld.State.NumSubjects)),
				util.FmtReplicaDrift(float64(replica.State.NumDeleted), float64(ld.State.NumDeleted)),
				util.FmtReplicaDrift(float64(replica.State.Consumers), float64(ld.State.Consumers)),
				util.FmtReplicaDrift(float64(replica.State.FirstSeq), float64(ld.State.FirstSeq)),
				util.FmtReplicaDrift(float64(replica.State.LastSeq), float64(ld.State.LastSeq)),
				status, replica.Cluster.Leader, strings.TrimSpace(replicasInfo), healthStatus)
		} else {
			table.AddRow(replica.StreamName, replica.RaftGroup, account, replica.AccountID, node,
				replica.State.Msgs, replica.State.Bytes, replica.State.NumSubjects,
				replica.State.NumDeleted, replica.State.Consumers, replica.State.FirstSeq,
				replica.State.LastSeq, status, replica.Cluster.Leader,
				strings.TrimSpace(replicasInfo), healthStatus)
		}
	}

	if c.csv {
		fmt.Println(table.RenderCSV())
	} else {
		fmt.Println(table.Render())
	}

	return nil
}

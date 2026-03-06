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

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/serverdata"
	"github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
)

type (
	ConsumerDetail struct {
		ServerID             string
		StreamName           string
		ConsumerName         string
		Account              string
		AccountID            string
		RaftGroup            string
		State                server.StreamState
		Cluster              *server.ClusterInfo
		StreamCluster        *server.ClusterInfo
		DeliveredStreamSeq   uint64
		DeliveredConsumerSeq uint64
		AckFloorStreamSeq    uint64
		AckFloorConsumerSeq  uint64
		NumAckPending        int
		NumRedelivered       int
		NumWaiting           int
		NumPending           uint64
		HealthStatus         string
	}

	ConsumerCheckCmd struct {
		raftGroup      string
		streamName     string
		consumerName   string
		unsyncedFilter bool
		health         bool
		expected       int
		stdin          bool
		readTimeout    int
		csv            bool
		archivePath    string
	}
)

func configureConsumerCheckCommand(app commandHost) {
	cc := &ConsumerCheckCmd{}
	consumerCheck := app.Command("consumer-check", "Check and display consumer information").Action(cc.consumerCheck).Hidden()
	consumerCheck.Tag("scope:system", "impact:ro")
	consumerCheck.Flag("stream", "Filter results by stream").StringVar(&cc.streamName)
	consumerCheck.Flag("consumer", "Filter results by consumer").StringVar(&cc.consumerName)
	consumerCheck.Flag("raft-group", "Filter results by raft group").StringVar(&cc.raftGroup)
	consumerCheck.Flag("health", "Check health from consumers").UnNegatableBoolVar(&cc.health)
	consumerCheck.Flag("expected", "Expected number of servers").IntVar(&cc.expected)
	consumerCheck.Flag("unsynced", "Filter results by streams that are out of sync").UnNegatableBoolVar(&cc.unsyncedFilter)
	consumerCheck.Flag("stdin", "Process the result of 'nats server request jsz --all --config' from STDIN").UnNegatableBoolVar(&cc.stdin)
	consumerCheck.Flag("read-timeout", "Read timeout in seconds").Default("5").IntVar(&cc.readTimeout)
	consumerCheck.Flag("csv", "Renders CSV format").UnNegatableBoolVar(&cc.csv)
	consumerCheck.Flag("archive", "Read data from an archive file").StringVar(&cc.archivePath)
}

func (c *ConsumerCheckCmd) dataSource(nc *nats.Conn) (serverdata.DataSource, error) {
	if c.archivePath != "" {
		return serverdata.NewArchive(c.archivePath)
	}
	timeout := opts().Timeout
	if c.readTimeout > 0 {
		timeout = time.Duration(c.readTimeout) * time.Second
	}
	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, timeout, opts().Trace)
	}
	return serverdata.NewServer(nc, reqFn, c.expected), nil
}

func (c *ConsumerCheckCmd) consumerCheck(_ *fisk.ParseContext) error {
	start := time.Now()

	var err error
	var nc *nats.Conn
	var ds serverdata.DataSource
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
				c.expected, err = serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, opts().Trace)
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
			JSzOptions: server.JSzOptions{Streams: true, Consumer: true, RaftGroups: true},
		})
		if err != nil {
			return err
		}
	}

	if !c.csv {
		fmt.Printf("Response took %.3fs\n", time.Since(start).Seconds())
		fmt.Printf("Servers: %d\n", len(responses))
		if c.expected > 0 && len(responses) < c.expected {
			fmt.Printf("Warning: expected %d responses got %d\n", c.expected, len(responses))
		}
	}

	streams := make(map[string]map[string]*streamDetail)
	consumers := make(map[string]map[string]*ConsumerDetail)
	// Collect all info from servers.
	for _, resp := range responses {
		if resp.Server == nil || resp.Data == nil {
			continue
		}
		server := resp.Server
		jsz := resp.Data
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var mok bool
				var ms map[string]*streamDetail
				if stream.RaftGroup == "" && stream.Cluster != nil {
					stream.RaftGroup = stream.Cluster.RaftGroup
				}
				mkey := fmt.Sprintf("%s|%s", acc.Name, stream.RaftGroup)
				if ms, mok = streams[mkey]; !mok {
					ms = make(map[string]*streamDetail)
					streams[mkey] = ms
				}
				ms[server.Name] = &streamDetail{
					ServerID:   server.ID,
					StreamName: stream.Name,
					Account:    acc.Name,
					AccountID:  acc.Id,
					RaftGroup:  stream.RaftGroup,
					State:      stream.State,
					Cluster:    stream.Cluster,
				}

				for _, consumer := range stream.Consumer {
					var raftGroup string
					for _, cr := range stream.ConsumerRaftGroups {
						if cr.Name == consumer.Name {
							raftGroup = cr.RaftGroup
							break
						}
					}
					if raftGroup == "" && consumer.Cluster != nil {
						raftGroup = consumer.Cluster.RaftGroup
					}

					var ok bool
					var m map[string]*ConsumerDetail
					key := fmt.Sprintf("%s|%s", acc.Name, raftGroup)
					if m, ok = consumers[key]; !ok {
						m = make(map[string]*ConsumerDetail)
						consumers[key] = m
					}

					m[server.Name] = &ConsumerDetail{
						ServerID:             server.ID,
						StreamName:           consumer.Stream,
						ConsumerName:         consumer.Name,
						Account:              acc.Name,
						AccountID:            acc.Id,
						RaftGroup:            raftGroup,
						State:                stream.State,
						DeliveredStreamSeq:   consumer.Delivered.Stream,
						DeliveredConsumerSeq: consumer.Delivered.Consumer,
						AckFloorStreamSeq:    consumer.AckFloor.Stream,
						AckFloorConsumerSeq:  consumer.AckFloor.Consumer,
						Cluster:              consumer.Cluster,
						StreamCluster:        stream.Cluster,
						NumAckPending:        consumer.NumAckPending,
						NumRedelivered:       consumer.NumRedelivered,
						NumWaiting:           consumer.NumWaiting,
						NumPending:           consumer.NumPending,
					}
				}
			}
		}
	}

	keys := make([]string, 0)
	for k := range consumers {
		for kk := range consumers[k] {
			key := fmt.Sprintf("%s/%s", k, kk)
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	title := ""
	if !c.csv {
		fmt.Printf("Consumers: %d\n", len(keys))
		title = "Consumers"
	}

	table := util.NewTableWriter(opts(), title)

	if c.health {
		table.AddHeaders("Consumer", "Stream", "Raft", "Account", "Account ID", "Node", "Delivered (S,C)", "ACK Floor (S,C)", "Counters", "Status", "Leader", "Stream Cluster Leader", "Peers", "Health")
	} else {
		table.AddHeaders("Consumer", "Stream", "Raft", "Account", "Account ID", "Node", "Delivered (S,C)", "ACK Floor (S,C)", "Counters", "Status", "Leader", "Stream Cluster Leader", "Peers")
	}

	var prev, prevAccount string
	for i, k := range keys {
		var unsynced bool
		av := strings.Split(k, "|")
		accName := av[0]
		v := strings.Split(av[1], "/")
		raftGroup, serverName := v[0], v[1]

		if c.raftGroup != "" && raftGroup != c.raftGroup {
			continue
		}

		key := fmt.Sprintf("%s|%s", accName, raftGroup)
		consumer := consumers[key]
		replica := consumer[serverName]
		var status string
		statuses := make(map[string]bool)

		if c.consumerName != "" && replica.ConsumerName != c.consumerName {
			continue
		}

		if c.streamName != "" && replica.StreamName != c.streamName {
			continue
		}

		if replica.State.LastSeq < replica.DeliveredStreamSeq {
			statuses["UNSYNCED:DELIVERED_AHEAD_OF_STREAM_SEQ"] = true
			unsynced = true
		}

		if replica.State.LastSeq < replica.AckFloorStreamSeq {
			statuses["UNSYNCED:ACKFLOOR_AHEAD_OF_STREAM_SEQ"] = true
			unsynced = true
		}

		// Make comparisons against other peers.
		for _, peer := range consumer {
			if peer.DeliveredStreamSeq != replica.DeliveredStreamSeq ||
				peer.DeliveredConsumerSeq != replica.DeliveredConsumerSeq {
				statuses["UNSYNCED:DELIVERED"] = true
				unsynced = true
			}
			if peer.AckFloorStreamSeq != replica.AckFloorStreamSeq ||
				peer.AckFloorConsumerSeq != replica.AckFloorConsumerSeq {
				statuses["UNSYNCED:ACK_FLOOR"] = true
				unsynced = true
			}
			if peer.Cluster == nil {
				statuses["NO_CLUSTER"] = true
				unsynced = true
			} else {
				if replica.Cluster == nil {
					statuses["NO_CLUSTER_R"] = true
					unsynced = true
				}
				if peer.Cluster.Leader != "" && replica.Cluster.Leader != "" && peer.Cluster.Leader != replica.Cluster.Leader {
					statuses["MULTILEADER"] = true
					unsynced = true
				}
			}
		}
		if replica.AckFloorStreamSeq == 0 || replica.AckFloorConsumerSeq == 0 ||
			replica.DeliveredConsumerSeq == 0 || replica.DeliveredStreamSeq == 0 {
			statuses["EMPTY"] = true
		}
		if len(statuses) > 0 {
			for k := range statuses {
				status = fmt.Sprintf("%s%s,", status, k)
			}
		} else {
			status = "IN SYNC"
		}

		if replica.Cluster != nil {
			if serverName == replica.Cluster.Leader && replica.Cluster.Leader == replica.StreamCluster.Leader {
				status += " / INTERSECT"
			}
		}

		if c.unsyncedFilter && !unsynced {
			continue
		}
		var alen int
		if len(replica.Account) > 10 {
			alen = 10
		} else {
			alen = len(replica.Account)
		}

		accountname := strings.Replace(replica.Account[:alen], " ", "_", -1)

		// Mark it in case it is a leader.
		var suffix string
		if replica.Cluster == nil {
			status = "NO_CLUSTER"
			unsynced = true
		} else if serverName == replica.Cluster.Leader {
			suffix = "*"
		} else if replica.Cluster.Leader == "" {
			status = "LEADERLESS"
			unsynced = true
		} else if replica.RaftGroup == "" {
			status = "MISSING_GROUP"
			unsynced = true
		}
		node := fmt.Sprintf("%s%s", serverName, suffix)

		progress := "0%"
		if replica.State.LastSeq > 0 {
			result := (float64(replica.DeliveredStreamSeq) / float64(replica.State.LastSeq)) * 100
			progress = fmt.Sprintf("%-3.0f%%", result)
		}

		isLeader := replica.Cluster != nil && serverName == replica.Cluster.Leader

		var delivered, ackfloor string
		if c.unsyncedFilter && !isLeader {
			lc := consumer[replica.Cluster.Leader]
			delivered = fmt.Sprintf("%s [%d, %d] %-3s | %s",
				util.FmtReplicaDrift(float64(replica.DeliveredStreamSeq), float64(lc.DeliveredStreamSeq)),
				replica.State.FirstSeq, replica.State.LastSeq, progress,
				util.FmtReplicaDrift(float64(replica.DeliveredConsumerSeq), float64(lc.DeliveredConsumerSeq)))
			ackfloor = fmt.Sprintf("%s | %s",
				util.FmtReplicaDrift(float64(replica.AckFloorStreamSeq), float64(lc.AckFloorStreamSeq)),
				util.FmtReplicaDrift(float64(replica.AckFloorConsumerSeq), float64(lc.AckFloorConsumerSeq)))
		} else {
			delivered = fmt.Sprintf("%d [%d, %d] %-3s | %d",
				replica.DeliveredStreamSeq, replica.State.FirstSeq, replica.State.LastSeq, progress, replica.DeliveredConsumerSeq)
			ackfloor = fmt.Sprintf("%d | %d", replica.AckFloorStreamSeq, replica.AckFloorConsumerSeq)
		}
		counters := fmt.Sprintf("(ap:%d, nr:%d, nw:%d, np:%d)", replica.NumAckPending, replica.NumRedelivered, replica.NumWaiting, replica.NumPending)

		var replicasInfo string
		if replica.Cluster != nil {
			for _, r := range replica.Cluster.Replicas {
				info := fmt.Sprintf("%s(current=%-5v,offline=%v)", r.Name, r.Current, r.Offline)
				replicasInfo = fmt.Sprintf("%-40s %s", info, replicasInfo)
			}
		}

		// Include Healthz if option added.
		var healthStatus string
		if c.health && ds != nil {
			hresps, err := ds.Healthz(server.HealthzEventOptions{
				HealthzOptions: server.HealthzOptions{
					Account:  replica.Account,
					Stream:   replica.StreamName,
					Consumer: replica.ConsumerName,
				},
				EventFilterOptions: server.EventFilterOptions{Name: replica.ServerID, ExactMatch: true},
			})
			if err != nil {
				healthStatus = err.Error()
			} else if len(hresps) > 0 && hresps[0].Data != nil {
				healthStatus = fmt.Sprintf(":%s:%s", hresps[0].Data.Status, hresps[0].Data.Error)
			}
		}

		clusterLeader := ""

		if replica.Cluster != nil {
			clusterLeader = replica.Cluster.Leader
		}

		if i > 0 && prev != replica.ConsumerName || prevAccount != accName {
			table.AddSeparator()
		}

		prev = replica.ConsumerName
		prevAccount = accName

		table.AddRow(replica.ConsumerName, replica.StreamName, replica.RaftGroup, accountname, replica.AccountID, node, delivered, ackfloor, counters, status, clusterLeader, replica.StreamCluster.Leader, strings.TrimSpace(replicasInfo), healthStatus)
	}

	if c.csv {
		fmt.Println(table.RenderCSV())
	} else {
		fmt.Println(table.Render())
	}

	return nil
}

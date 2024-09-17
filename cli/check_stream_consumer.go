package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats.go"
)

const (
	srvHealthzSubj = "$SYS.REQ.SERVER.%s.HEALTHZ"
	srvJszSubj     = "$SYS.REQ.SERVER.%s.JSZ"
	// Healthz checks server health status.
	DefaultRequestTimeout = 60 * time.Second
)

const (
	StatusOK HealthStatus = iota
	StatusUnavailable
	StatusError
)

var (
	ErrValidation      = errors.New("validation error")
	ErrInvalidServerID = errors.New("server with given ID does not exist")
)

type (
	// ServerInfo identifies remote servers.
	ServerInfo struct {
		Name      string    `json:"name"`
		Host      string    `json:"host"`
		ID        string    `json:"id"`
		Cluster   string    `json:"cluster,omitempty"`
		Domain    string    `json:"domain,omitempty"`
		Version   string    `json:"ver"`
		Tags      []string  `json:"tags,omitempty"`
		Seq       uint64    `json:"seq"`
		JetStream bool      `json:"jetstream"`
		Time      time.Time `json:"time"`
	}

	JSZResp struct {
		Server ServerInfo `json:"server"`
		JSInfo JSInfo     `json:"data"`
	}

	JSInfo struct {
		ID       string          `json:"server_id"`
		Now      time.Time       `json:"now"`
		Disabled bool            `json:"disabled,omitempty"`
		Config   JetStreamConfig `json:"config,omitempty"`
		JetStreamStats
		Streams   int              `json:"streams"`
		Consumers int              `json:"consumers"`
		Messages  uint64           `json:"messages"`
		Bytes     uint64           `json:"bytes"`
		Meta      *MetaClusterInfo `json:"meta_cluster,omitempty"`

		// aggregate raft info
		AccountDetails []*AccountDetail `json:"account_details,omitempty"`
	}

	AccountDetail struct {
		Name string `json:"name"`
		Id   string `json:"id"`
		JetStreamStats
		Streams []StreamDetail `json:"stream_detail,omitempty"`
	}

	StreamDetail struct {
		Name               string                   `json:"name"`
		Created            time.Time                `json:"created"`
		Cluster            *nats.ClusterInfo        `json:"cluster,omitempty"`
		Config             *nats.StreamConfig       `json:"config,omitempty"`
		State              nats.StreamState         `json:"state,omitempty"`
		Consumer           []*nats.ConsumerInfo     `json:"consumer_detail,omitempty"`
		Mirror             *nats.StreamSourceInfo   `json:"mirror,omitempty"`
		Sources            []*nats.StreamSourceInfo `json:"sources,omitempty"`
		RaftGroup          string                   `json:"stream_raft_group,omitempty"`
		ConsumerRaftGroups []*RaftGroupDetail       `json:"consumer_raft_groups,omitempty"`
	}

	RaftGroupDetail struct {
		Name      string `json:"name"`
		RaftGroup string `json:"raft_group,omitempty"`
	}

	JszEventOptions struct {
		JszOptions
		EventFilterOptions
	}

	JszOptions struct {
		Account    string `json:"account,omitempty"`
		Accounts   bool   `json:"accounts,omitempty"`
		Streams    bool   `json:"streams,omitempty"`
		Consumer   bool   `json:"consumer,omitempty"`
		Config     bool   `json:"config,omitempty"`
		LeaderOnly bool   `json:"leader_only,omitempty"`
		Offset     int    `json:"offset,omitempty"`
		Limit      int    `json:"limit,omitempty"`
		RaftGroups bool   `json:"raft,omitempty"`
	}

	// JetStreamVarz contains basic runtime information about jetstream
	JetStreamVarz struct {
		Config *JetStreamConfig `json:"config,omitempty"`
		Stats  *JetStreamStats  `json:"stats,omitempty"`
		Meta   *MetaClusterInfo `json:"meta,omitempty"`
	}

	// Statistics about JetStream for this server.
	JetStreamStats struct {
		Memory         uint64            `json:"memory"`
		Store          uint64            `json:"storage"`
		ReservedMemory uint64            `json:"reserved_memory"`
		ReservedStore  uint64            `json:"reserved_storage"`
		Accounts       int               `json:"accounts"`
		HAAssets       int               `json:"ha_assets"`
		API            JetStreamAPIStats `json:"api"`
	}

	// JetStreamConfig determines this server's configuration.
	// MaxMemory and MaxStore are in bytes.
	JetStreamConfig struct {
		MaxMemory  int64  `json:"max_memory"`
		MaxStore   int64  `json:"max_storage"`
		StoreDir   string `json:"store_dir,omitempty"`
		Domain     string `json:"domain,omitempty"`
		CompressOK bool   `json:"compress_ok,omitempty"`
		UniqueTag  string `json:"unique_tag,omitempty"`
	}

	JetStreamAPIStats struct {
		Total    uint64 `json:"total"`
		Errors   uint64 `json:"errors"`
		Inflight uint64 `json:"inflight,omitempty"`
	}

	// MetaClusterInfo shows information about the meta group.
	MetaClusterInfo struct {
		Name     string      `json:"name,omitempty"`
		Leader   string      `json:"leader,omitempty"`
		Peer     string      `json:"peer,omitempty"`
		Replicas []*PeerInfo `json:"replicas,omitempty"`
		Size     int         `json:"cluster_size"`
	}

	// PeerInfo shows information about all the peers in the cluster that
	// are supporting the stream or consumer.
	PeerInfo struct {
		Name    string        `json:"name"`
		Current bool          `json:"current"`
		Offline bool          `json:"offline,omitempty"`
		Active  time.Duration `json:"active"`
		Lag     uint64        `json:"lag,omitempty"`
		Peer    string        `json:"peer"`
	}

	// Common filter options for system requests STATSZ VARZ SUBSZ CONNZ ROUTEZ GATEWAYZ LEAFZ
	EventFilterOptions struct {
		Name    string   `json:"server_name,omitempty"` // filter by server name
		Cluster string   `json:"cluster,omitempty"`     // filter by cluster name
		Host    string   `json:"host,omitempty"`        // filter by host name
		Tags    []string `json:"tags,omitempty"`        // filter by tags (must match all tags)
		Domain  string   `json:"domain,omitempty"`      // filter by JS domain
	}

	HealthzResp struct {
		Server  ServerInfo `json:"server"`
		Healthz Healthz    `json:"data"`
	}

	Healthz struct {
		Status HealthStatus `json:"status"`
		Error  string       `json:"error,omitempty"`
	}

	HealthStatus int

	// HealthzOptions are options passed to Healthz
	HealthzOptions struct {
		JSEnabledOnly bool   `json:"js-enabled-only,omitempty"`
		JSServerOnly  bool   `json:"js-server-only,omitempty"`
		Account       string `json:"account,omitempty"`
		Stream        string `json:"stream,omitempty"`
		Consumer      string `json:"consumer,omitempty"`
	}

	streamDetail struct {
		StreamName   string
		Account      string
		AccountID    string
		RaftGroup    string
		State        nats.StreamState
		Cluster      *nats.ClusterInfo
		HealthStatus string
		ServerID     string
	}

	FetchOpts struct {
		Timeout     time.Duration
		ReadTimeout time.Duration
		Expected    int
	}

	ConsumerDetail struct {
		ServerID             string
		StreamName           string
		ConsumerName         string
		Account              string
		AccountID            string
		RaftGroup            string
		State                nats.StreamState
		Cluster              *nats.ClusterInfo
		StreamCluster        *nats.ClusterInfo
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

	StreamCheckCmd struct {
		raftGroup      string
		streamName     string
		unsyncedFilter bool
		health         bool
		expected       int
		stdin          bool
		readTimeout    int
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
	}

	FetchOpt func(*FetchOpts) error
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

	cc := &ConsumerCheckCmd{}
	consumerCheck := app.Command("consumer-check", "Check and display consumer information").Action(cc.consumerCheck).Hidden()
	consumerCheck.Flag("stream", "Filter results by stream").StringVar(&cc.streamName)
	consumerCheck.Flag("consumer", "Filter results by consumer").StringVar(&cc.consumerName)
	consumerCheck.Flag("raft-group", "Filter results by raft group").StringVar(&cc.raftGroup)
	consumerCheck.Flag("health", "Check health from consumers").UnNegatableBoolVar(&cc.health)
	consumerCheck.Flag("expected", "Expected number of servers").IntVar(&cc.expected)
	consumerCheck.Flag("unsynced", "Filter results by streams that are out of sync").UnNegatableBoolVar(&cc.unsyncedFilter)
	consumerCheck.Flag("stdin", "Process the contents from STDIN").UnNegatableBoolVar(&cc.stdin)
	consumerCheck.Flag("read-timeout", "Read timeout in seconds").Default("5").IntVar(&cc.readTimeout)
}

func init() {
	registerCommand("stream_check", 22, configureStreamCheckCommand)
}

func (c *ConsumerCheckCmd) consumerCheck(_ *fisk.ParseContext) error {
	var err error
	start := time.Now()

	nc, _, err := prepareHelper(opts().Servers, natsOpts()...)
	if err != nil {
		return err
	}

	sys := Sys(nc)

	if c.expected == 0 {
		c.expected, err = currentActiveServers(nc)
		if err != nil {
			return fmt.Errorf("failed to get current active servers: %s", err)
		}
	}

	servers, err := findServers(c.stdin, c.expected, start, sys, true)
	if err != nil {
		return fmt.Errorf("failed to find servers: %s", err)
	}

	streams := make(map[string]map[string]*streamDetail)
	consumers := make(map[string]map[string]*ConsumerDetail)
	// Collect all info from servers.
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var mok bool
				var ms map[string]*streamDetail
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

					var ok bool
					var m map[string]*ConsumerDetail
					key := fmt.Sprintf("%s|%s", acc.Name, raftGroup)
					if m, ok = consumers[key]; !ok {
						m = make(map[string]*ConsumerDetail)
						consumers[key] = m
					}

					m[server.Name] = &ConsumerDetail{
						ServerID:     server.ID,
						StreamName:   consumer.Stream,
						ConsumerName: consumer.Name,
						Account:      acc.Name,
						AccountID:    acc.Id,
						RaftGroup:    raftGroup,
						// StreamRaftGroup:      stream.RaftGroup,
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

	table := newTableWriter("Consumers")
	table.AddHeaders("CONSUMER", "STREAM", "RAFT", "ACCOUNT", "ACC_ID", "NODE", "DELIVERED (S,C)", "ACK_FLOOR (S,C)", "COUNTERS", "STATUS", "LEADER", "STREAM CLUSTER LEADER", "PEERS")

	if c.health {
		table.AddHeaders("HEALTH")
	}

	for _, k := range keys {
		var unsynced bool
		av := strings.Split(k, "|")
		accName := av[0]
		v := strings.Split(av[1], "/")
		raftGroup, serverName := v[0], v[1]

		if c.raftGroup != "" && raftGroup == c.raftGroup {
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
				if peer.Cluster.Leader != replica.Cluster.Leader {
					statuses["MULTILEADER"] = true
					unsynced = true
				}
			}
		}
		if replica.AckFloorStreamSeq == 0 || replica.AckFloorConsumerSeq == 0 ||
			replica.DeliveredConsumerSeq == 0 || replica.DeliveredStreamSeq == 0 {
			statuses["EMPTY"] = true
			// unsynced = true
		}
		if len(statuses) > 0 {
			for k, _ := range statuses {
				status = fmt.Sprintf("%s%s,", status, k)
			}
		} else {
			status = "IN SYNC"
		}

		if serverName == replica.Cluster.Leader && replica.Cluster.Leader == replica.StreamCluster.Leader {
			status += " / INTERSECT"
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
		}
		node := fmt.Sprintf("%s%s", serverName, suffix)

		progress := "0%"
		if replica.State.LastSeq > 0 {
			result := (float64(replica.DeliveredStreamSeq) / float64(replica.State.LastSeq)) * 100
			progress = fmt.Sprintf("%-3.0f%%", result)
		}

		delivered := fmt.Sprintf("%d [%d, %d] %-3s | %d",
			replica.DeliveredStreamSeq, replica.State.FirstSeq, replica.State.LastSeq, progress, replica.DeliveredConsumerSeq)
		ackfloor := fmt.Sprintf("%d | %d", replica.AckFloorStreamSeq, replica.AckFloorConsumerSeq)
		counters := fmt.Sprintf("(ap:%d, nr:%d, nw:%d, np:%d)", replica.NumAckPending, replica.NumRedelivered, replica.NumWaiting, replica.NumPending)

		var replicasInfo string
		for _, r := range replica.Cluster.Replicas {
			info := fmt.Sprintf("%s(current=%-5v,offline=%v)", r.Name, r.Current, r.Offline)
			replicasInfo = fmt.Sprintf("%-40s %s", info, replicasInfo)
		}

		// Include Healthz if option added.
		var healthStatus string
		if c.health {
			hstatus, err := sys.Healthz(replica.ServerID, HealthzOptions{
				Account:  replica.Account,
				Stream:   replica.StreamName,
				Consumer: replica.ConsumerName,
			})
			if err != nil {
				healthStatus = err.Error()
			} else {
				healthStatus = fmt.Sprintf(":%s:%s", hstatus.Healthz.Status, hstatus.Healthz.Error)
			}
		}

		table.AddRow(replica.ConsumerName, replica.StreamName, replica.RaftGroup, accountname, replica.AccountID, node, delivered, ackfloor, counters, status, replica.Cluster.Leader, replica.StreamCluster.Leader, replicasInfo, healthStatus)
	}

	fmt.Println(table.Render())
	return nil
}

func (c *StreamCheckCmd) streamCheck(_ *fisk.ParseContext) error {
	var err error
	start := time.Now()

	nc, _, err := prepareHelper(opts().Servers, natsOpts()...)
	if err != nil {
		return err
	}

	sys := Sys(nc)

	if c.expected == 0 {
		c.expected, err = currentActiveServers(nc)
		if err != nil {
			return fmt.Errorf("failed to get current active servers: %s", err)
		}
	}

	servers, err := findServers(c.stdin, c.expected, start, sys, false)
	if err != nil {
		return fmt.Errorf("failed to find servers: %s", err)
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

	fmt.Printf("Streams: %d\n", len(streams))

	table := newTableWriter("Streams")
	table.AddHeaders("STREAM REPLICA", "RAFT", "ACCOUNT", "ACC_ID", "NODE", "MESSAGES", "BYTES", "SUBJECTS", "DELETED", "CONSUMERS", "FIRST", "LAST", "STATUS", "LEADER", "PEERS")

	if c.health {
		table.AddHeaders("HEALTH")
	}

	for _, k := range keys {
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
			hstatus, err := sys.Healthz(replica.ServerID, HealthzOptions{
				Account: replica.Account,
				Stream:  replica.StreamName,
			})
			if err != nil {
				healthStatus = err.Error()
			} else {
				healthStatus = fmt.Sprintf(":%s:%s", hstatus.Healthz.Status, hstatus.Healthz.Error)
			}
		}

		table.AddRow(replica.StreamName, replica.RaftGroup, account, replica.AccountID, fmt.Sprintf("%s%s", serverName, suffix), replica.State.Msgs, replica.State.Bytes, replica.State.NumSubjects, replica.State.NumDeleted, replica.State.Consumers, replica.State.FirstSeq,
			replica.State.LastSeq, status, replica.Cluster.Leader, replicasInfo, healthStatus)
	}

	fmt.Println(table.Render())
	return nil
}

func findServers(stdin bool, expected int, start time.Time, sys SysClient, getConsumer bool) ([]JSZResp, error) {
	var err error
	servers := []JSZResp{}

	if stdin {
		reader := bufio.NewReader(os.Stdin)

		for i := 0; i < expected; i++ {
			data, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return servers, err
			}
			if len(data) > 0 {
				var jszResp JSZResp
				if err := json.Unmarshal([]byte(data), &jszResp); err != nil {
					return servers, err
				}
				servers = append(servers, jszResp)
			}
		}
	} else {
		fmt.Printf("Connected in %.3fs\n", time.Since(start).Seconds())
		jszOptions := JszOptions{
			Streams:    true,
			RaftGroups: true,
		}

		if getConsumer {
			jszOptions.Consumer = true
		}

		start = time.Now()
		fetchTimeout := FetchTimeout(time.Duration(opts().Timeout) * time.Second)
		fetchExpected := FetchExpected(expected)
		fetchReadTimeout := FetchReadTimeout(time.Duration(opts().Timeout) * time.Second)
		servers, err = sys.JszPing(JszEventOptions{
			JszOptions: jszOptions,
		}, fetchTimeout, fetchReadTimeout, fetchExpected)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Response took %.3fs\n", time.Since(start).Seconds())
	}

	fmt.Printf("Servers: %d\n", len(servers))
	return servers, nil
}

// SysClient can be used to request monitoring data from the server.
type SysClient struct {
	nc *nats.Conn
}

func Sys(nc *nats.Conn) SysClient {
	return SysClient{
		nc: nc,
	}
}

func (s *SysClient) JszPing(opts JszEventOptions, fopts ...FetchOpt) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.Fetch(subj, payload, fopts...)
	if err != nil {
		return nil, err
	}
	srvJsz := make([]JSZResp, 0, len(resp))
	for _, msg := range resp {
		var jszResp JSZResp
		if err := json.Unmarshal(msg.Data, &jszResp); err != nil {
			return nil, err
		}
		srvJsz = append(srvJsz, jszResp)
	}
	return srvJsz, nil
}

func FetchTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: timeout has to be greater than 0", ErrValidation)
		}
		opts.Timeout = timeout
		return nil
	}
}

func FetchReadTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: read timeout has to be greater than 0", ErrValidation)
		}
		opts.ReadTimeout = timeout
		return nil
	}
}

func FetchExpected(expected int) FetchOpt {
	return func(opts *FetchOpts) error {
		if expected <= 0 {
			return fmt.Errorf("%w: expected request count has to be greater than 0", ErrValidation)
		}
		opts.Expected = expected
		return nil
	}
}

func (s *SysClient) Fetch(subject string, data []byte, opts ...FetchOpt) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: expected subject 0", ErrValidation)
	}

	conn := s.nc
	reqOpts := &FetchOpts{}
	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	inbox := nats.NewInbox()
	res := make([]*nats.Msg, 0)
	msgsChan := make(chan *nats.Msg, 100)

	readTimer := time.NewTimer(reqOpts.ReadTimeout)
	sub, err := conn.Subscribe(inbox, func(msg *nats.Msg) {
		readTimer.Reset(reqOpts.ReadTimeout)
		msgsChan <- msg
	})
	defer sub.Unsubscribe()

	if err := conn.PublishRequest(subject, inbox, data); err != nil {
		return nil, err
	}

	for {
		select {
		case msg := <-msgsChan:
			if msg.Header.Get("Status") == "503" {
				return nil, fmt.Errorf("server request on subject %q failed: %w", subject, err)
			}
			res = append(res, msg)
			if reqOpts.Expected != -1 && len(res) == reqOpts.Expected {
				return res, nil
			}
		case <-readTimer.C:
			return res, nil
		case <-time.After(reqOpts.Timeout):
			return res, nil
		}
	}
}

func jsonString(s string) string {
	return "\"" + s + "\""
}

func (hs *HealthStatus) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("ok"):
		*hs = StatusOK
	case jsonString("na"), jsonString("unavailable"):
		*hs = StatusUnavailable
	case jsonString("error"):
		*hs = StatusError
	default:
		return fmt.Errorf("cannot unmarshal %q", data)
	}

	return nil
}

func (hs HealthStatus) MarshalJSON() ([]byte, error) {
	switch hs {
	case StatusOK:
		return json.Marshal("ok")
	case StatusUnavailable:
		return json.Marshal("na")
	case StatusError:
		return json.Marshal("error")
	default:
		return nil, fmt.Errorf("unknown health status: %v", hs)
	}
}

func (hs HealthStatus) String() string {
	switch hs {
	case StatusOK:
		return "ok"
	case StatusUnavailable:
		return "na"
	case StatusError:
		return "error"
	default:
		return "unknown health status"
	}
}

func (s *SysClient) Healthz(id string, opts HealthzOptions) (*HealthzResp, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: server id cannot be empty", ErrValidation)
	}
	subj := fmt.Sprintf(srvHealthzSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.nc.Request(subj, payload, DefaultRequestTimeout)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidServerID, id)
		}
		return nil, err
	}
	var healthzResp HealthzResp
	if err := json.Unmarshal(resp.Data, &healthzResp); err != nil {
		return nil, err
	}

	return &healthzResp, nil
}

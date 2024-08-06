// Copyright 2020-2024 The NATS Authors
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
	"regexp"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/monitor"
	"github.com/nats-io/nats-server/v2/server"
)

type SrvCheckCmd struct {
	connectWarning  time.Duration
	connectCritical time.Duration
	rttWarning      time.Duration
	rttCritical     time.Duration
	reqWarning      time.Duration
	reqCritical     time.Duration

	sourcesStream            string
	sourcesLagCritical       uint64
	sourcesLagCriticalIsSet  bool
	sourcesSeenCritical      time.Duration
	sourcesSeenCriticalIsSet bool
	sourcesMinSources        int
	sourcesMinSourcesIsSet   bool
	sourcesMaxSources        int
	sourcesMaxSourcesIsSet   bool
	streamMessagesWarn       uint64
	streamMessagesWarnIsSet  bool
	streamMessagesCrit       uint64
	streamMessagesCritIsSet  bool
	subjectsWarn             int
	subjectsWarnIsSet        bool
	subjectsCrit             int
	subjectsCritIsSet        bool

	consumerName                        string
	consumerAckOutstandingCritical      int
	consumerAckOutstandingCriticalIsSet bool
	consumerWaitingCritical             int
	consumerWaitingCriticalIsSet        bool
	consumerUnprocessedCritical         int
	consumerUnprocessedCriticalIsSet    bool
	consumerLastDeliveryCritical        time.Duration
	consumerLastDeliveryCriticalIsSet   bool
	consumerLastAckCritical             time.Duration
	consumerLastAckCriticalIsSet        bool
	consumerRedeliveryCritical          int
	consumerRedeliveryCriticalIsSet     bool

	raftExpect            int
	raftExpectIsSet       bool
	raftLagCritical       uint64
	raftLagCriticalIsSet  bool
	raftSeenCritical      time.Duration
	raftSeenCriticalIsSet bool

	jsMemWarn             int
	jsMemCritical         int
	jsStoreWarn           int
	jsStoreCritical       int
	jsStreamsWarn         int
	jsStreamsCritical     int
	jsConsumersWarn       int
	jsConsumersCritical   int
	jsReplicas            bool
	jsReplicaSeenCritical time.Duration
	jsReplicaLagCritical  uint64

	srvName        string
	srvCPUWarn     int
	srvCPUCrit     int
	srvMemWarn     int
	srvMemCrit     int
	srvConnWarn    int
	srvConnCrit    int
	srvSubsWarn    int
	srvSubCrit     int
	srvUptimeWarn  time.Duration
	srvUptimeCrit  time.Duration
	srvAuthRequire bool
	srvTLSRequired bool
	srvJSRequired  bool

	msgSubject  string
	msgAgeWarn  time.Duration
	msgAgeCrit  time.Duration
	msgRegexp   *regexp.Regexp
	msgBodyAsTs bool

	kvBucket                 string
	kvValuesCrit             int64
	kvValuesWarn             int64
	kvKey                    string
	credentialValidityCrit   time.Duration
	credentialValidityWarn   time.Duration
	credentialRequiresExpire bool
	credential               string

	useMetadata bool
}

func configureServerCheckCommand(srv *fisk.CmdClause) {
	c := &SrvCheckCmd{}

	check := srv.Command("check", "Health check for NATS servers")
	check.Flag("format", "Render the check in a specific format (nagios, json, prometheus, text)").Default("nagios").EnumVar(&checkRenderFormatText, "nagios", "json", "prometheus", "text")
	check.Flag("namespace", "The prometheus namespace to use in output").Default(opts().PrometheusNamespace).StringVar(&opts().PrometheusNamespace)
	check.Flag("outfile", "Save output to a file rather than STDOUT").StringVar(&checkRenderOutFile)
	check.PreAction(c.parseRenderFormat)

	conn := check.Command("connection", "Checks basic server connection").Alias("conn").Action(c.checkConnection)
	conn.Flag("connect-warn", "Warning threshold to allow for establishing connections").Default("500ms").PlaceHolder("DURATION").DurationVar(&c.connectWarning)
	conn.Flag("connect-critical", "Critical threshold to allow for establishing connections").Default("1s").PlaceHolder("DURATION").DurationVar(&c.connectCritical)
	conn.Flag("rtt-warn", "Warning threshold to allow for server RTT").Default("500ms").PlaceHolder("DURATION").DurationVar(&c.rttWarning)
	conn.Flag("rtt-critical", "Critical threshold to allow for server RTT").Default("1s").PlaceHolder("DURATION").DurationVar(&c.rttCritical)
	conn.Flag("req-warn", "Warning threshold to allow for full round trip test").PlaceHolder("DURATION").Default("500ms").DurationVar(&c.reqWarning)
	conn.Flag("req-critical", "Critical threshold to allow for full round trip test").PlaceHolder("DURATION").Default("1s").DurationVar(&c.reqCritical)

	stream := check.Command("stream", "Checks the health of mirrored streams, streams with sources or clustered streams").Action(c.checkStream)
	stream.HelpLong(`These settings can be set using Stream Metadata in the following form:

	io.nats.monitor.lag-critical: 200

When set these settings will be used, but can be overridden using --lag-critical.`)
	stream.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	stream.Flag("lag-critical", "Critical threshold to allow for lag on any source or mirror").PlaceHolder("MSGS").IsSetByUser(&c.sourcesLagCriticalIsSet).Uint64Var(&c.sourcesLagCritical)
	stream.Flag("seen-critical", "Critical threshold for how long ago the source or mirror should have been seen").PlaceHolder("DURATION").IsSetByUser(&c.sourcesSeenCriticalIsSet).DurationVar(&c.sourcesSeenCritical)
	stream.Flag("min-sources", "Minimum number of sources to expect").PlaceHolder("SOURCES").Default("1").IsSetByUser(&c.sourcesMinSourcesIsSet).IntVar(&c.sourcesMinSources)
	stream.Flag("max-sources", "Maximum number of sources to expect").PlaceHolder("SOURCES").Default("1").IsSetByUser(&c.sourcesMaxSourcesIsSet).IntVar(&c.sourcesMaxSources)
	stream.Flag("peer-expect", "Number of cluster replicas to expect").Default("1").PlaceHolder("SERVERS").IsSetByUser(&c.raftExpectIsSet).IntVar(&c.raftExpect)
	stream.Flag("peer-lag-critical", "Critical threshold to allow for cluster peer lag").PlaceHolder("OPS").IsSetByUser(&c.raftLagCriticalIsSet).Uint64Var(&c.raftLagCritical)
	stream.Flag("peer-seen-critical", "Critical threshold for how long ago a cluster peer should have been seen").PlaceHolder("DURATION").IsSetByUser(&c.raftSeenCriticalIsSet).Default("10s").DurationVar(&c.raftSeenCritical)
	stream.Flag("msgs-warn", "Warn if there are fewer than this many messages in the stream").PlaceHolder("MSGS").IsSetByUser(&c.streamMessagesWarnIsSet).Uint64Var(&c.streamMessagesWarn)
	stream.Flag("msgs-critical", "Critical if there are fewer than this many messages in the stream").PlaceHolder("MSGS").IsSetByUser(&c.streamMessagesCritIsSet).Uint64Var(&c.streamMessagesCrit)
	stream.Flag("subjects-warn", "Critical threshold for subjects in the stream").PlaceHolder("SUBJECTS").Default("-1").IsSetByUser(&c.subjectsWarnIsSet).IntVar(&c.subjectsWarn)
	stream.Flag("subjects-critical", "Warning threshold for subjects in the stream").PlaceHolder("SUBJECTS").Default("-1").IsSetByUser(&c.subjectsCritIsSet).IntVar(&c.subjectsCrit)
	stream.Flag("metadata", "Sets monitoring thresholds from Stream metadata").Default("true").BoolVar(&c.useMetadata)

	consumer := check.Command("consumer", "Checks the health of a consumer").Action(c.checkConsumer)
	consumer.HelpLong(`These settings can be set using Consumer Metadata in the following form:

	io.nats.monitor.waiting-critical: 20

When set these settings will be used, but can be overridden using --waiting-critical.`)
	consumer.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	consumer.Flag("consumer", "The consumer to check").Required().StringVar(&c.consumerName)
	consumer.Flag("outstanding-ack-critical", "Maximum number of outstanding acks to allow").Default("-1").IsSetByUser(&c.consumerAckOutstandingCriticalIsSet).IntVar(&c.consumerAckOutstandingCritical)
	consumer.Flag("waiting-critical", "Maximum number of waiting pulls to allow").Default("-1").IsSetByUser(&c.consumerWaitingCriticalIsSet).IntVar(&c.consumerWaitingCritical)
	consumer.Flag("unprocessed-critical", "Maximum number of unprocessed messages to allow").Default("-1").IsSetByUser(&c.consumerUnprocessedCriticalIsSet).IntVar(&c.consumerUnprocessedCritical)
	consumer.Flag("last-delivery-critical", "Time to allow since the last delivery").Default("0s").IsSetByUser(&c.consumerLastDeliveryCriticalIsSet).DurationVar(&c.consumerLastDeliveryCritical)
	consumer.Flag("last-ack-critical", "Time to allow since the last ack").Default("0s").IsSetByUser(&c.consumerLastAckCriticalIsSet).DurationVar(&c.consumerLastAckCritical)
	consumer.Flag("redelivery-critical", "Maximum number of redeliveries to allow").Default("-1").IsSetByUser(&c.consumerRedeliveryCriticalIsSet).IntVar(&c.consumerRedeliveryCritical)
	consumer.Flag("metadata", "Sets monitoring thresholds from Consumer metadata").Default("true").BoolVar(&c.useMetadata)

	msg := check.Command("message", "Checks properties of a message stored in a stream").Action(c.checkMsg)
	msg.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	msg.Flag("subject", "The subject to fetch a message from").Default(">").StringVar(&c.msgSubject)
	msg.Flag("age-warn", "Warning threshold for message age as a duration").PlaceHolder("DURATION").DurationVar(&c.msgAgeWarn)
	msg.Flag("age-critical", "Critical threshold for message age as a duration").PlaceHolder("DURATION").DurationVar(&c.msgAgeCrit)
	msg.Flag("content", "Regular expression to check the content against").PlaceHolder("REGEX").RegexpVar(&c.msgRegexp)
	msg.Flag("body-timestamp", "Use message body as a unix timestamp instead of message metadata").UnNegatableBoolVar(&c.msgBodyAsTs)

	meta := check.Command("meta", "Check JetStream cluster state").Alias("raft").Action(c.checkRaft)
	meta.Flag("expect", "Number of servers to expect").Required().PlaceHolder("SERVERS").IntVar(&c.raftExpect)
	meta.Flag("lag-critical", "Critical threshold to allow for lag").PlaceHolder("OPS").Required().Uint64Var(&c.raftLagCritical)
	meta.Flag("seen-critical", "Critical threshold for how long ago a peer should have been seen").Required().PlaceHolder("DURATION").DurationVar(&c.raftSeenCritical)

	js := check.Command("jetstream", "Check JetStream account state").Alias("js").Action(c.checkJS)
	js.Flag("mem-warn", "Warning threshold for memory storage, in percent").Default("75").IntVar(&c.jsMemWarn)
	js.Flag("mem-critical", "Critical threshold for memory storage, in percent").Default("90").IntVar(&c.jsMemCritical)
	js.Flag("store-warn", "Warning threshold for disk storage, in percent").Default("75").IntVar(&c.jsStoreWarn)
	js.Flag("store-critical", "Critical threshold for memory storage, in percent").Default("90").IntVar(&c.jsStoreCritical)
	js.Flag("streams-warn", "Warning threshold for number of streams used, in percent").Default("-1").IntVar(&c.jsStreamsWarn)
	js.Flag("streams-critical", "Critical threshold for number of streams used, in percent").Default("-1").IntVar(&c.jsStreamsCritical)
	js.Flag("consumers-warn", "Warning threshold for number of consumers used, in percent").Default("-1").IntVar(&c.jsConsumersWarn)
	js.Flag("consumers-critical", "Critical threshold for number of consumers used, in percent").Default("-1").IntVar(&c.jsConsumersCritical)
	js.Flag("replicas", "Checks if all streams have healthy replicas").Default("true").BoolVar(&c.jsReplicas)
	js.Flag("replica-seen-critical", "Critical threshold for when a stream replica should have been seen, as a duration").Default("5s").DurationVar(&c.jsReplicaSeenCritical)
	js.Flag("replica-lag-critical", "Critical threshold for how many operations behind a peer can be").Default("200").Uint64Var(&c.jsReplicaLagCritical)

	serv := check.Command("server", "Checks a NATS Server health").Action(c.checkSrv)
	serv.Flag("name", "Server name to require in the result").Required().StringVar(&c.srvName)
	serv.Flag("cpu-warn", "Warning threshold for CPU usage, in percent").IntVar(&c.srvCPUWarn)
	serv.Flag("cpu-critical", "Critical threshold for CPU usage, in percent").IntVar(&c.srvCPUCrit)
	serv.Flag("mem-warn", "Warning threshold for Memory usage, in percent").IntVar(&c.srvMemWarn)
	serv.Flag("mem-critical", "Critical threshold Memory CPU usage, in percent").IntVar(&c.srvMemCrit)
	serv.Flag("conn-warn", "Warning threshold for connections, supports inversion").IntVar(&c.srvConnWarn)
	serv.Flag("conn-critical", "Critical threshold for connections, supports inversion").IntVar(&c.srvConnCrit)
	serv.Flag("subs-warn", "Warning threshold for number of active subscriptions, supports inversion").IntVar(&c.srvSubsWarn)
	serv.Flag("subs-critical", "Critical threshold for number of active subscriptions, supports inversion").IntVar(&c.srvSubCrit)
	serv.Flag("uptime-warn", "Warning threshold for server uptime as duration").DurationVar(&c.srvUptimeWarn)
	serv.Flag("uptime-critical", "Critical threshold for server uptime as duration").DurationVar(&c.srvUptimeCrit)
	serv.Flag("auth-required", "Checks that authentication is enabled").UnNegatableBoolVar(&c.srvAuthRequire)
	serv.Flag("tls-required", "Checks that TLS is required").UnNegatableBoolVar(&c.srvTLSRequired)
	serv.Flag("js-required", "Checks that JetStream is enabled").UnNegatableBoolVar(&c.srvJSRequired)

	kv := check.Command("kv", "Checks a NATS KV Bucket").Action(c.checkKV)
	kv.Flag("bucket", "Checks a specific bucket").Required().StringVar(&c.kvBucket)
	kv.Flag("values-critical", "Critical threshold for number of values in the bucket").Default("-1").Int64Var(&c.kvValuesCrit)
	kv.Flag("values-warn", "Warning threshold for number of values in the bucket").Default("-1").Int64Var(&c.kvValuesWarn)
	kv.Flag("key", "Requires a key to have any non-delete value set").StringVar(&c.kvKey)

	cred := check.Command("credential", "Checks the validity of a NATS credential file").Action(c.checkCredentialAction)
	cred.Flag("credential", "The file holding the NATS credential").Required().StringVar(&c.credential)
	cred.Flag("validity-warn", "Warning threshold for time before expiry").DurationVar(&c.credentialValidityWarn)
	cred.Flag("validity-critical", "Critical threshold for time before expiry").DurationVar(&c.credentialValidityCrit)
	cred.Flag("require-expiry", "Requires the credential to have expiry set").Default("true").BoolVar(&c.credentialRequiresExpire)
}

var (
	checkRenderFormatText = "nagios"
	checkRenderFormat     = monitor.NagiosFormat
	checkRenderOutFile    = ""
)

func (c *SrvCheckCmd) parseRenderFormat(_ *fisk.ParseContext) error {
	switch checkRenderFormatText {
	case "prometheus":
		checkRenderFormat = monitor.PrometheusFormat
	case "text":
		checkRenderFormat = monitor.TextFormat
	case "json":
		checkRenderFormat = monitor.JSONFormat
	}

	return nil
}

func (c *SrvCheckCmd) checkConsumer(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: fmt.Sprintf("%s_%s", c.sourcesStream, c.consumerName), Check: "consumer", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	cons, err := mgr.LoadConsumer(c.sourcesStream, c.consumerName)
	if err != nil {
		check.Critical("consumer load failure: %v", err)
		return nil
	}

	checkOpts := &jsm.ConsumerHealthCheckOptions{}
	if c.useMetadata {
		checkOpts, err = cons.HealthCheckOptions()
		if err != nil {
			return fmt.Errorf("invalid metadata: %v", err)
		}
	}
	if !c.useMetadata || c.consumerAckOutstandingCriticalIsSet {
		checkOpts.AckOutstandingCritical = c.consumerAckOutstandingCritical
	}
	if !c.useMetadata || c.consumerWaitingCriticalIsSet {
		checkOpts.WaitingCritical = c.consumerWaitingCritical
	}
	if !c.useMetadata || c.consumerUnprocessedCriticalIsSet {
		checkOpts.UnprocessedCritical = c.consumerUnprocessedCritical
	}
	if !c.useMetadata || c.consumerLastDeliveryCriticalIsSet {
		checkOpts.LastDeliveryCritical = c.consumerLastDeliveryCritical
	}
	if !c.useMetadata || c.consumerLastAckCriticalIsSet {
		checkOpts.LastAckCritical = c.consumerLastAckCritical
	}
	if !c.useMetadata || c.consumerRedeliveryCriticalIsSet {
		checkOpts.RedeliveryCritical = c.consumerRedeliveryCritical
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}
	_, err = cons.HealthCheck(*checkOpts, check, logger)
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}

	return nil
}

func (c *SrvCheckCmd) checkKV(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.kvBucket, Check: "kv", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	nc, _, err := prepareHelper("", natsOpts()...)
	if check.CriticalIfErr(err, "connection failed: %s", err) {
		return nil
	}

	return monitor.CheckKVBucketAndKey(nc, check, monitor.KVCheckOptions{
		Bucket:         c.kvBucket,
		Key:            c.kvKey,
		ValuesCritical: c.kvValuesCrit,
		ValuesWarning:  c.kvValuesWarn,
	})
}

func (c *SrvCheckCmd) checkSrv(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.srvName, Check: "server", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	nc, _, err := prepareHelper("", natsOpts()...)
	if check.CriticalIfErr(err, "connection failed: %s", err) {
		return nil
	}

	return monitor.CheckServer(nc, check, opts().Timeout, monitor.ServerCheckOptions{})
}

func (c *SrvCheckCmd) checkJS(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "JetStream", Check: "jetstream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	info, err := mgr.JetStreamAccountInfo()
	check.CriticalIfErr(err, "JetStream not available: %s", err)

	err = c.checkAccountInfo(check, info)
	check.CriticalIfErr(err, "JetStream not available: %s", err)

	if c.jsReplicas {
		streams, _, err := mgr.Streams(nil)
		check.CriticalIfErr(err, "JetStream not available: %s", err)

		err = c.checkStreamClusterHealth(check, streams)
		check.CriticalIfErr(err, "JetStream not available: %s", err)
	}

	return nil
}

func (c *SrvCheckCmd) checkStreamClusterHealth(check *monitor.Result, info []*jsm.Stream) error {
	var okCnt, noLeaderCnt, notEnoughReplicasCnt, critCnt, lagCritCnt, seenCritCnt int

	for _, s := range info {
		nfo, err := s.LatestInformation()
		if err != nil {
			critCnt++
			continue
		}

		if nfo.Config.Replicas == 1 {
			okCnt++
			continue
		}

		if nfo.Cluster == nil {
			critCnt++
			continue
		}

		if nfo.Cluster.Leader == "" {
			noLeaderCnt++
			continue
		}

		if len(nfo.Cluster.Replicas) != s.Replicas()-1 {
			notEnoughReplicasCnt++
			continue
		}

		for _, r := range nfo.Cluster.Replicas {
			if r.Offline {
				critCnt++
				continue
			}

			if r.Active > c.jsReplicaSeenCritical {
				seenCritCnt++
				continue
			}
			if r.Lag > c.jsReplicaLagCritical {
				lagCritCnt++
				continue
			}
		}

		okCnt++
	}

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_ok",
		Value: float64(okCnt),
		Help:  "Streams with healthy cluster state",
	})

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_no_leader",
		Value: float64(noLeaderCnt),
		Help:  "Streams with no leader elected",
	})

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_missing_replicas",
		Value: float64(notEnoughReplicasCnt),
		Help:  "Streams where there are fewer known replicas than configured",
	})

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_lagged",
		Value: float64(lagCritCnt),
		Crit:  float64(c.jsReplicaLagCritical),
		Help:  fmt.Sprintf("Streams with > %d lagged replicas", c.jsReplicaLagCritical),
	})

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_not_seen",
		Value: float64(seenCritCnt),
		Crit:  c.jsReplicaSeenCritical.Seconds(),
		Unit:  "s",
		Help:  fmt.Sprintf("Streams with replicas seen > %d ago", c.jsReplicaSeenCritical),
	})

	check.Pd(&monitor.PerfDataItem{
		Name:  "replicas_fail",
		Value: float64(critCnt),
		Help:  "Streams unhealthy cluster state",
	})

	if critCnt > 0 || notEnoughReplicasCnt > 0 || noLeaderCnt > 0 || seenCritCnt > 0 || lagCritCnt > 0 {
		check.Critical("%d unhealthy streams", critCnt+notEnoughReplicasCnt+noLeaderCnt)
	}

	return nil
}

func (c *SrvCheckCmd) checkAccountInfo(check *monitor.Result, info *api.JetStreamAccountStats) error {
	if info == nil {
		return fmt.Errorf("invalid account status")
	}

	checkVal := func(item string, unit string, warn int, crit int, max int64, current uint64) {
		pct := 0
		if max > 0 {
			pct = int(float64(current) / float64(max) * 100)
		}

		check.Pd(&monitor.PerfDataItem{Name: item, Value: float64(current), Unit: unit, Help: fmt.Sprintf("JetStream %s resource usage", item)})
		check.Pd(&monitor.PerfDataItem{
			Name:  fmt.Sprintf("%s_pct", item),
			Value: float64(pct),
			Unit:  "%",
			Warn:  float64(warn),
			Crit:  float64(crit),
			Help:  fmt.Sprintf("JetStream %s resource usage in percent", item),
		})

		if warn != -1 && crit != -1 && warn >= crit {
			check.Critical("%s: invalid thresholds", item)
			return
		}

		if pct > 100 {
			check.Critical("%s: exceed server limits", item)
			return
		}

		if warn >= 0 && crit >= 0 {
			switch {
			case pct > crit:
				check.Critical("%d%% %s", pct, item)
			case pct > warn:
				check.Warn("%d%% %s", pct, item)
			}
		}
	}

	checkVal("memory", "B", c.jsMemWarn, c.jsMemCritical, info.Limits.MaxMemory, info.Memory)
	checkVal("storage", "B", c.jsStoreWarn, c.jsStoreCritical, info.Limits.MaxStore, info.Store)
	checkVal("streams", "", c.jsStreamsWarn, c.jsStreamsCritical, int64(info.Limits.MaxStreams), uint64(info.Streams))
	checkVal("consumers", "", c.jsConsumersWarn, c.jsConsumersCritical, int64(info.Limits.MaxConsumers), uint64(info.Consumers))

	return nil
}

func (c *SrvCheckCmd) checkRaft(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "JetStream Meta Cluster", Check: "meta"}
	defer check.GenericExit()

	nc, _, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	res, err := doReq(&server.JSzOptions{LeaderOnly: true}, "$SYS.REQ.SERVER.PING.JSZ", 1, nc)
	check.CriticalIfErr(err, "JSZ API request failed: %s", err)

	if len(res) != 1 {
		check.CriticalExit(fmt.Sprintf("JSZ API request returned %d results", len(res)))
		return nil
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	// works around 2.7.0 breaking chances
	type jszrCompact struct {
		Data struct {
			Streams   int    `json:"total_streams,omitempty"`
			Consumers int    `json:"total_consumers,omitempty"`
			Messages  uint64 `json:"total_messages,omitempty"`
			Bytes     uint64 `json:"total_message_bytes,omitempty"`
		} `json:"data"`
	}

	jszresp := &jszr{}
	err = json.Unmarshal(res[0], jszresp)
	check.CriticalIfErr(err, "invalid result received: %s", err)

	// we may have a pre 2.7.0 machine and will try get data with old struct names, if all of these are
	// 0 it might be that they are 0 or that we had data in the old format, so we try parse the old
	// and set what is in there.  If it's not an old server 0s will stay 0s, otherwise we pull in old format values
	if jszresp.Data.Streams == 0 && jszresp.Data.Consumers == 0 && jszresp.Data.Messages == 0 && jszresp.Data.Bytes == 0 {
		cresp := jszrCompact{}
		if json.Unmarshal(res[0], &cresp) == nil {
			jszresp.Data.Streams = cresp.Data.Streams
			jszresp.Data.Consumers = cresp.Data.Consumers
			jszresp.Data.Messages = cresp.Data.Messages
			jszresp.Data.Bytes = cresp.Data.Bytes
		}
	}

	err = c.checkMetaClusterInfo(check, jszresp.Data.Meta)
	check.CriticalIfErr(err, "invalid result received: %s", err)

	if len(check.Criticals) == 0 && len(check.Warnings) == 0 {
		check.Ok("%d peers led by %s", len(jszresp.Data.Meta.Replicas)+1, jszresp.Data.Meta.Leader)
	}

	return nil
}

func (c *SrvCheckCmd) checkMetaClusterInfo(check *monitor.Result, ci *server.MetaClusterInfo) error {
	return c.checkClusterInfo(check, &server.ClusterInfo{
		Name:     ci.Name,
		Leader:   ci.Leader,
		Replicas: ci.Replicas,
	})
}

func (c *SrvCheckCmd) checkClusterInfo(check *monitor.Result, ci *server.ClusterInfo) error {
	if ci == nil {
		check.Critical("no cluster information")
		return nil
	}

	if ci.Leader == "" {
		check.Critical("No leader")
		return nil
	}

	check.Pd(&monitor.PerfDataItem{
		Name:  "peers",
		Value: float64(len(ci.Replicas) + 1),
		Warn:  float64(c.raftExpect),
		Crit:  float64(c.raftExpect),
		Help:  "Configured RAFT peers",
	})

	if len(ci.Replicas)+1 != c.raftExpect {
		check.Critical("%d peers of expected %d", len(ci.Replicas)+1, c.raftExpect)
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

	check.Pd(
		&monitor.PerfDataItem{Name: "peer_offline", Value: float64(offline), Help: "Offline RAFT peers"},
		&monitor.PerfDataItem{Name: "peer_not_current", Value: float64(notCurrent), Help: "RAFT peers that are not current"},
		&monitor.PerfDataItem{Name: "peer_inactive", Value: float64(inactive), Help: "Inactive RAFT peers"},
		&monitor.PerfDataItem{Name: "peer_lagged", Value: float64(lagged), Help: "RAFT peers that are lagged more than configured threshold"},
	)

	if notCurrent > 0 {
		check.Critical("%d not current", notCurrent)
	}
	if inactive > 0 {
		check.Critical("%d inactive more than %s", inactive, c.raftSeenCritical)
	}
	if offline > 0 {
		check.Critical("%d offline", offline)
	}
	if lagged > 0 {
		check.Critical("%d lagged more than %d ops", lagged, c.raftLagCritical)
	}

	return nil
}

func (c *SrvCheckCmd) checkStream(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.sourcesStream, Check: "stream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	stream, err := mgr.LoadStream(c.sourcesStream)
	check.CriticalIfErr(err, "could not load stream %s: %s", c.sourcesStream, err)

	checkOpts := &jsm.StreamHealthCheckOptions{}
	if c.useMetadata {
		checkOpts, err = stream.HealthCheckOptions()
		check.CriticalIfErr(err, "Invalid metadata: %s", err)
	}

	if !c.useMetadata || c.sourcesLagCriticalIsSet {
		checkOpts.SourcesLagCritical = c.sourcesLagCritical
	}
	if !c.useMetadata || c.sourcesSeenCriticalIsSet {
		checkOpts.SourcesSeenCritical = c.sourcesSeenCritical
	}
	if !c.useMetadata || c.sourcesMinSourcesIsSet {
		checkOpts.MinSources = c.sourcesMinSources
	}
	if !c.useMetadata || c.sourcesMaxSourcesIsSet {
		checkOpts.MaxSources = c.sourcesMaxSources
	}
	if !c.useMetadata || c.raftExpectIsSet {
		checkOpts.ClusterExpectedPeers = c.raftExpect
	}
	if !c.useMetadata || c.raftLagCriticalIsSet {
		checkOpts.ClusterLagCritical = c.raftLagCritical
	}
	if !c.useMetadata || c.raftSeenCriticalIsSet {
		checkOpts.ClusterSeenCritical = c.raftSeenCritical
	}
	if !c.useMetadata || c.streamMessagesWarnIsSet {
		checkOpts.MessagesWarn = c.streamMessagesWarn
	}
	if !c.useMetadata || c.streamMessagesCritIsSet {
		checkOpts.MessagesCrit = c.streamMessagesCrit
	}
	if !c.useMetadata || c.subjectsWarnIsSet {
		checkOpts.SubjectsWarn = c.subjectsWarn
	}
	if !c.useMetadata || c.subjectsCritIsSet {
		checkOpts.SubjectsCrit = c.subjectsCrit
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}
	_, err = stream.HealthCheck(*checkOpts, check, logger)
	check.CriticalIfErr(err, "Healthcheck failed: %s", err)

	return nil
}

func (c *SrvCheckCmd) checkMsg(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Stream Message", Check: "message", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed")

	return monitor.CheckStreamMessage(mgr, check, monitor.CheckStreamMessageOptions{
		StreamName:      c.sourcesStream,
		Subject:         c.msgSubject,
		AgeWarning:      c.msgAgeWarn,
		AgeCritical:     c.msgAgeCrit,
		Content:         c.msgRegexp.String(),
		BodyAsTimestamp: c.msgBodyAsTs,
	})
}

func (c *SrvCheckCmd) checkConnection(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Connection", Check: "connections", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	if opts().Config == nil {
		err := loadContext(false)
		if check.CriticalIfErr(err, "loading context failed: %v", err) {
			return nil
		}
	}

	return monitor.CheckConnection(opts().Config.ServerURL(), natsOpts(), opts().Timeout, check, monitor.ConnectionCheckOptions{
		ConnectTimeWarning:  c.connectWarning,
		ConnectTimeCritical: c.connectCritical,
		ServerRttWarning:    c.rttWarning,
		ServerRttCritical:   c.rttCritical,
		RequestRttWarning:   c.reqWarning,
		RequestRttCritical:  c.reqCritical,
	})
}

func (c *SrvCheckCmd) checkCredentialAction(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Credential", Check: "credential", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	return monitor.CheckCredential(check, monitor.CredentialCheckOptions{
		File:             c.credential,
		ValidityWarning:  c.credentialValidityWarn,
		ValidityCritical: c.credentialValidityCrit,
		RequiresExpiry:   c.credentialRequiresExpire,
	})
}

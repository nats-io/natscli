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
	"fmt"
	"regexp"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/monitor"
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
	consumerPinned                      bool

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

	srvName         string
	srvCPUWarn      int
	srvCPUCrit      int
	srvMemWarn      int
	srvMemCrit      int
	srvConnWarn     int
	srvConnCrit     int
	srvSubsWarn     int
	srvSubCrit      int
	srvUptimeWarn   time.Duration
	srvUptimeCrit   time.Duration
	srvAuthRequired bool
	srvTLSRequired  bool
	srvJSRequired   bool

	msgSubject      string
	msgAgeWarn      time.Duration
	msgAgeCrit      time.Duration
	msgRegexp       *regexp.Regexp
	msgBodyAsTs     bool
	msgHeaders      map[string]string
	msgHeadersMatch map[string]string
	msgPayload      string
	msgCrit         time.Duration
	msgWarn         time.Duration

	kvBucket                 string
	kvValuesCrit             int64
	kvValuesWarn             int64
	kvKey                    string
	credentialValidityCrit   time.Duration
	credentialValidityWarn   time.Duration
	credentialRequiresExpire bool
	credential               string

	exporterConfigFile  string
	exporterPort        int
	exporterCertificate string
	exporterKey         string
}

func configureServerCheckCommand(srv *fisk.CmdClause) {
	c := &SrvCheckCmd{
		msgHeaders:      make(map[string]string),
		msgHeadersMatch: make(map[string]string),
	}

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
	consumer.Flag("pinned", "Requires Pinned Client priority with all groups having a pinned client").UnNegatableBoolVar(&c.consumerPinned)

	msg := check.Command("message", "Checks properties of a message stored in a stream").Action(c.checkMsg)
	msg.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	msg.Flag("subject", "The subject to fetch a message from").Default(">").StringVar(&c.msgSubject)
	msg.Flag("age-warn", "Warning threshold for message age as a duration").PlaceHolder("DURATION").DurationVar(&c.msgAgeWarn)
	msg.Flag("age-critical", "Critical threshold for message age as a duration").PlaceHolder("DURATION").DurationVar(&c.msgAgeCrit)
	msg.Flag("content", "Regular expression to check the content against").PlaceHolder("REGEX").Default(".").RegexpVar(&c.msgRegexp)
	msg.Flag("body-timestamp", "Use message body as a unix timestamp instead of message metadata").UnNegatableBoolVar(&c.msgBodyAsTs)

	meta := check.Command("meta", "Check JetStream cluster state").Alias("raft").Action(c.checkRaft)
	meta.Flag("expect", "Number of servers to expect").Required().PlaceHolder("SERVERS").IntVar(&c.raftExpect)
	meta.Flag("lag-critical", "Critical threshold to allow for lag").PlaceHolder("OPS").Required().Uint64Var(&c.raftLagCritical)
	meta.Flag("seen-critical", "Critical threshold for how long ago a peer should have been seen").Required().PlaceHolder("DURATION").DurationVar(&c.raftSeenCritical)

	req := check.Command("request", "Checks a request-reply service").Alias("req").Action(c.checkRequest)
	req.Flag("subject", "The subject to send the request to").Required().StringVar(&c.msgSubject)
	req.Flag("payload", "Payload to send in the request").StringVar(&c.msgPayload)
	req.Flag("headers", "Headers to publish in the request").StringMapVar(&c.msgHeaders)
	req.Flag("match-payload", "Regular expression the response should match").RegexpVar(&c.msgRegexp)
	req.Flag("match-headers", "Headers to publish in the request").StringMapVar(&c.msgHeaders)
	req.Flag("response-critical", "Critical threshold for response time").DurationVar(&c.msgCrit)
	req.Flag("response-warn", "Warning threshold for response time").DurationVar(&c.msgWarn)

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
	serv.Flag("auth-required", "Checks that authentication is enabled").UnNegatableBoolVar(&c.srvAuthRequired)
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

	exporter := check.Command("exporter", "Prometheus exporter for server checks").Hidden().Action(c.exporterAction)
	exporter.Flag("config", "Exporter configuration").Required().ExistingFileVar(&c.exporterConfigFile)
	exporter.Flag("port", "Port to listen on").Default("8080").IntVar(&c.exporterPort)
	exporter.Flag("https-key", "Key for HTTPS").ExistingFileVar(&c.exporterKey)
	exporter.Flag("https-certificate", "Certificate for HTTPS").ExistingFileVar(&c.exporterCertificate)
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

func (c *SrvCheckCmd) checkRequest(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.msgSubject, Check: "request", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := monitor.CheckRequestOptions{
		Subject:              c.msgSubject,
		Payload:              c.msgPayload,
		Header:               c.msgHeaders,
		HeaderMatch:          c.msgHeadersMatch,
		ResponseTimeWarn:     c.msgWarn.Seconds(),
		ResponseTimeCritical: c.msgCrit.Seconds(),
	}

	if c.msgRegexp != nil {
		checkOpts.ResponseMatch = c.msgRegexp.String()
	}

	err := monitor.CheckRequest(opts().Config.ServerURL(), natsOpts(), check, opts().Timeout, checkOpts)
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}

	return nil
}

func (c *SrvCheckCmd) checkConsumer(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: fmt.Sprintf("%s_%s", c.sourcesStream, c.consumerName), Check: "consumer", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := &monitor.ConsumerHealthCheckOptions{
		StreamName:   c.sourcesStream,
		ConsumerName: c.consumerName,
		Pinned:       c.consumerPinned,
	}

	if c.consumerAckOutstandingCriticalIsSet {
		checkOpts.AckOutstandingCritical = c.consumerAckOutstandingCritical
	}
	if c.consumerWaitingCriticalIsSet {
		checkOpts.WaitingCritical = c.consumerWaitingCritical
	}
	if c.consumerUnprocessedCriticalIsSet {
		checkOpts.UnprocessedCritical = c.consumerUnprocessedCritical
	}
	if c.consumerLastDeliveryCriticalIsSet {
		checkOpts.LastDeliveryCritical = c.consumerLastDeliveryCritical.Seconds()
	}
	if c.consumerLastAckCriticalIsSet {
		checkOpts.LastAckCritical = c.consumerLastAckCritical.Seconds()
	}
	if c.consumerRedeliveryCriticalIsSet {
		checkOpts.RedeliveryCritical = c.consumerRedeliveryCritical
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}
	err := monitor.ConsumerHealthCheck(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, *checkOpts, logger)
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}

	return nil
}

func (c *SrvCheckCmd) checkKV(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.kvBucket, Check: "kv", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckKVBucketAndKey(opts().Config.ServerURL(), natsOpts(), check, monitor.CheckKVBucketAndKeyOptions{
		Bucket:         c.kvBucket,
		Key:            c.kvKey,
		ValuesCritical: c.kvValuesCrit,
		ValuesWarning:  c.kvValuesWarn,
	})
}

func (c *SrvCheckCmd) checkSrv(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.srvName, Check: "server", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckServer(opts().Config.ServerURL(), natsOpts(), check, opts().Timeout, monitor.CheckServerOptions{
		Name:                   c.srvName,
		CPUWarning:             c.srvCPUWarn,
		CPUCritical:            c.srvCPUCrit,
		MemoryWarning:          c.srvMemWarn,
		MemoryCritical:         c.srvMemCrit,
		ConnectionsWarning:     c.srvConnWarn,
		ConnectionsCritical:    c.srvConnCrit,
		SubscriptionsWarning:   c.srvSubsWarn,
		SubscriptionsCritical:  c.srvSubCrit,
		UptimeWarning:          c.srvUptimeWarn.Seconds(),
		UptimeCritical:         c.srvUptimeCrit.Seconds(),
		AuthenticationRequired: c.srvAuthRequired,
		TLSRequired:            c.srvTLSRequired,
		JetStreamRequired:      c.srvJSRequired,
	})
}

func (c *SrvCheckCmd) checkJS(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "JetStream", Check: "jetstream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckJetStreamAccount(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, monitor.CheckJetStreamAccountOptions{
		MemoryWarning:       c.jsMemWarn,
		MemoryCritical:      c.jsMemCritical,
		FileWarning:         c.jsStoreWarn,
		FileCritical:        c.jsStoreCritical,
		StreamWarning:       c.jsStreamsWarn,
		StreamCritical:      c.jsStreamsCritical,
		ConsumersWarning:    c.jsConsumersWarn,
		ConsumersCritical:   c.jsConsumersCritical,
		CheckReplicas:       c.jsReplicas,
		ReplicaSeenCritical: c.jsReplicaSeenCritical.Seconds(),
		ReplicaLagCritical:  c.jsReplicaLagCritical,
	})
}

func (c *SrvCheckCmd) checkRaft(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "JetStream Meta Cluster", Check: "meta", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckJetstreamMeta(opts().Config.ServerURL(), natsOpts(), check, monitor.CheckJetstreamMetaOptions{
		ExpectServers: c.raftExpect,
		LagCritical:   c.raftLagCritical,
		SeenCritical:  c.raftSeenCritical.Seconds(),
	})
}

func (c *SrvCheckCmd) checkStream(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.sourcesStream, Check: "stream", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	checkOpts := &monitor.CheckStreamHealthOptions{
		StreamName: c.sourcesStream,
	}

	if c.sourcesLagCriticalIsSet {
		checkOpts.SourcesLagCritical = c.sourcesLagCritical
	}
	if c.sourcesSeenCriticalIsSet {
		checkOpts.SourcesSeenCritical = c.sourcesSeenCritical.Seconds()
	}
	if c.sourcesMinSourcesIsSet {
		checkOpts.MinSources = c.sourcesMinSources
	}
	if c.sourcesMaxSourcesIsSet {
		checkOpts.MaxSources = c.sourcesMaxSources
	}
	if c.raftExpectIsSet {
		checkOpts.ClusterExpectedPeers = c.raftExpect
	}
	if c.raftLagCriticalIsSet {
		checkOpts.ClusterLagCritical = c.raftLagCritical
	}
	if c.raftSeenCriticalIsSet {
		checkOpts.ClusterSeenCritical = c.raftSeenCritical.Seconds()
	}
	if c.streamMessagesWarnIsSet {
		checkOpts.MessagesWarn = c.streamMessagesWarn
	}
	if c.streamMessagesCritIsSet {
		checkOpts.MessagesCrit = c.streamMessagesCrit
	}
	if c.subjectsWarnIsSet {
		checkOpts.SubjectsWarn = c.subjectsWarn
	}
	if c.subjectsCritIsSet {
		checkOpts.SubjectsCrit = c.subjectsCrit
	}

	logger := api.NewDiscardLogger()
	if opts().Trace {
		logger = api.NewDefaultLogger(api.TraceLevel)
	}
	err := monitor.CheckStreamHealth(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, *checkOpts, logger)
	check.CriticalIfErr(err, "Healthcheck failed: %s", err)

	return nil
}

func (c *SrvCheckCmd) checkMsg(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Stream Message", Check: "message", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckStreamMessage(opts().Config.ServerURL(), natsOpts(), jsmOpts(), check, monitor.CheckStreamMessageOptions{
		StreamName:      c.sourcesStream,
		Subject:         c.msgSubject,
		AgeWarning:      c.msgAgeWarn.Seconds(),
		AgeCritical:     c.msgAgeCrit.Seconds(),
		Content:         c.msgRegexp.String(),
		BodyAsTimestamp: c.msgBodyAsTs,
	})
}

func (c *SrvCheckCmd) checkConnection(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Connection", Check: "connections", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	if opts().Config == nil {
		err := loadContext(false)
		if check.CriticalIfErr(err, "loading context failed: %v", err) {
			return nil
		}
	}

	return monitor.CheckConnection(opts().Config.ServerURL(), natsOpts(), opts().Timeout, check, monitor.CheckConnectionOptions{
		ConnectTimeWarning:  c.connectWarning.Seconds(),
		ConnectTimeCritical: c.connectCritical.Seconds(),
		ServerRttWarning:    c.rttWarning.Seconds(),
		ServerRttCritical:   c.rttCritical.Seconds(),
		RequestRttWarning:   c.reqWarning.Seconds(),
		RequestRttCritical:  c.reqCritical.Seconds(),
	})
}

func (c *SrvCheckCmd) checkCredentialAction(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Credential", Check: "credential", OutFile: checkRenderOutFile, NameSpace: opts().PrometheusNamespace, RenderFormat: checkRenderFormat, Trace: opts().Trace}
	defer check.GenericExit()

	return monitor.CheckCredential(check, monitor.CheckCredentialOptions{
		File:             c.credential,
		ValidityWarning:  c.credentialValidityWarn.Seconds(),
		ValidityCritical: c.credentialValidityCrit.Seconds(),
		RequiresExpiry:   c.credentialRequiresExpire,
	})
}

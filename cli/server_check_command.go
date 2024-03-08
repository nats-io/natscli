// Copyright 2020-2022 The NATS Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/mprimi/natscli/monitor"
	"github.com/nats-io/nkeys"
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
	subjectsWarn        int
	subjectsCrit        int

	consumerName                   string
	consumerAckOutstandingCritical int
	consumerWaitingCritical        int
	consumerUnprocessedCritical    int
	consumerLastDeliveryCritical   time.Duration
	consumerLastAckCritical        time.Duration
	consumerRedeliveryCritical     int

	raftExpect       int
	raftLagCritical  uint64
	raftSeenCritical time.Duration

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
	srvURL         *url.URL

	msgSubject  string
	msgAgeWarn  time.Duration
	msgAgeCrit  time.Duration
	msgRegexp   *regexp.Regexp
	msgBodyAsTs bool

	kvBucket                 string
	kvValuesCrit             int
	kvValuesWarn             int
	kvKey                    string
	credentialValidityCrit   time.Duration
	credentialValidityWarn   time.Duration
	credentialRequiresExpire bool
	credential               string
}

func configureServerCheckCommand(srv *fisk.CmdClause) {
	c := &SrvCheckCmd{}

	check := srv.Command("check", "Health check for NATS servers")
	check.Flag("format", "Render the check in a specific format (nagios, json, prometheus, text)").Default("nagios").EnumVar(&checkRenderFormatText, "nagios", "json", "prometheus", "text")
	check.Flag("namespace", "The prometheus namespace to use in output").Default(opts.PrometheusNamespace).StringVar(&opts.PrometheusNamespace)
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
	stream.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	stream.Flag("lag-critical", "Critical threshold to allow for lag on any source or mirror").PlaceHolder("MSGS").Uint64Var(&c.sourcesLagCritical)
	stream.Flag("seen-critical", "Critical threshold for how long ago the source or mirror should have been seen").PlaceHolder("DURATION").DurationVar(&c.sourcesSeenCritical)
	stream.Flag("min-sources", "Minimum number of sources to expect").PlaceHolder("SOURCES").Default("1").IntVar(&c.sourcesMinSources)
	stream.Flag("max-sources", "Maximum number of sources to expect").PlaceHolder("SOURCES").Default("1").IntVar(&c.sourcesMaxSources)
	stream.Flag("peer-expect", "Number of cluster replicas to expect").Required().PlaceHolder("SERVERS").IntVar(&c.raftExpect)
	stream.Flag("peer-lag-critical", "Critical threshold to allow for cluster peer lag").PlaceHolder("OPS").Uint64Var(&c.raftLagCritical)
	stream.Flag("peer-seen-critical", "Critical threshold for how long ago a cluster peer should have been seen").PlaceHolder("DURATION").Default("10s").DurationVar(&c.raftSeenCritical)
	stream.Flag("msgs-warn", "Warn if there are fewer than this many messages in the stream").PlaceHolder("MSGS").Uint64Var(&c.sourcesMessagesWarn)
	stream.Flag("msgs-critical", "Critical if there are fewer than this many messages in the stream").PlaceHolder("MSGS").Uint64Var(&c.sourcesMessagesCrit)
	stream.Flag("subjects-warn", "Critical threshold for subjects in the stream").PlaceHolder("SUBJECTS").Default("-1").IntVar(&c.subjectsWarn)
	stream.Flag("subjects-critical", "Warning threshold for subjects in the stream").PlaceHolder("SUBJECTS").Default("-1").IntVar(&c.subjectsCrit)

	consumer := check.Command("consumer", "Checks the health of a consumer").Action(c.checkConsumer)
	consumer.Flag("stream", "The streams to check").Required().StringVar(&c.sourcesStream)
	consumer.Flag("consumer", "The consumer to check").Required().StringVar(&c.consumerName)
	consumer.Flag("outstanding-ack-critical", "Maximum number of outstanding acks to allow").Default("-1").IntVar(&c.consumerAckOutstandingCritical)
	consumer.Flag("waiting-critical", "Maximum number of waiting pulls to allow").Default("-1").IntVar(&c.consumerWaitingCritical)
	consumer.Flag("unprocessed-critical", "Maximum number of unprocessed messages to allow").Default("-1").IntVar(&c.consumerUnprocessedCritical)
	consumer.Flag("last-delivery-critical", "Time to allow since the last delivery").Default("0s").DurationVar(&c.consumerLastDeliveryCritical)
	consumer.Flag("last-ack-critical", "Time to allow since the last ack").Default("0s").DurationVar(&c.consumerLastAckCritical)
	consumer.Flag("redelivery-critical", "Maximum number of redeliveries to allow").Default("-1").IntVar(&c.consumerRedeliveryCritical)

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
	kv.Flag("values-critical", "Critical threshold for number of values in the bucket").Default("-1").IntVar(&c.kvValuesCrit)
	kv.Flag("values-warn", "Warning threshold for number of values in the bucket").Default("-1").IntVar(&c.kvValuesWarn)
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

func (c *SrvCheckCmd) checkKVStatusAndBucket(check *monitor.Result, nc *nats.Conn) {
	js, err := nc.JetStream()
	check.CriticalIfErr(err, "connection failed: %v", err)

	kv, err := js.KeyValue(c.kvBucket)
	if err == nats.ErrBucketNotFound {
		check.Critical("bucket %v not found", c.kvBucket)
		return
	} else if err != nil {
		check.Critical("could not load bucket: %v", err)
		return
	}

	check.Ok("bucket %s", c.kvBucket)

	status, err := kv.Status()
	check.CriticalIfErr(err, "could not obtain bucket status: %v", err)

	check.Pd(
		&monitor.PerfDataItem{Name: "values", Value: float64(status.Values()), Warn: float64(c.kvValuesWarn), Crit: float64(c.kvValuesCrit), Help: "How many values are stored in the bucket"},
	)

	if c.kvKey != "" {
		v, err := kv.Get(c.kvKey)
		if err == nats.ErrKeyNotFound {
			check.Critical("key %s not found", c.kvKey)
		} else if err != nil {
			check.Critical("key %s not loaded: %v", c.kvKey, err)
		} else {
			switch v.Operation() {
			case nats.KeyValueDelete:
				check.Critical("key %v is deleted", c.kvKey)
			case nats.KeyValuePurge:
				check.Critical("key %v is purged", c.kvKey)
			default:
				check.Ok("key %s found", c.kvKey)
			}
		}
	}

	if c.kvValuesWarn > -1 || c.kvValuesCrit > -1 {
		if c.kvValuesCrit < c.kvValuesWarn {
			if c.kvValuesCrit > -1 && status.Values() <= uint64(c.kvValuesCrit) {
				check.Critical("%d values", status.Values())
			} else if c.kvValuesWarn > -1 && status.Values() <= uint64(c.kvValuesWarn) {
				check.Warn("%d values", status.Values())
			} else {
				check.Ok("%d values", status.Values())
			}
		} else {
			if c.kvValuesCrit > -1 && status.Values() >= uint64(c.kvValuesCrit) {
				check.Critical("%d values", status.Values())
			} else if c.kvValuesWarn > -1 && status.Values() >= uint64(c.kvValuesWarn) {
				check.Warn("%d values", status.Values())
			} else {
				check.Ok("%d values", status.Values())
			}
		}
	}

	if status.BackingStore() == "JetStream" {
		nfo := status.(*nats.KeyValueBucketStatus).StreamInfo()
		check.Pd(
			&monitor.PerfDataItem{Name: "bytes", Value: float64(nfo.State.Bytes), Unit: "B", Help: "Bytes stored in the bucket"},
			&monitor.PerfDataItem{Name: "replicas", Value: float64(nfo.Config.Replicas)},
		)
	}
}

func (c *SrvCheckCmd) checkConsumerStatus(check *monitor.Result, nfo api.ConsumerInfo) {
	check.Pd(&monitor.PerfDataItem{Name: "ack_pending", Value: float64(nfo.NumAckPending), Help: "The number of messages waiting to be Acknowledged", Crit: float64(c.consumerAckOutstandingCritical)})
	check.Pd(&monitor.PerfDataItem{Name: "pull_waiting", Value: float64(nfo.NumWaiting), Help: "The number of waiting Pull requests", Crit: float64(c.consumerWaitingCritical)})
	check.Pd(&monitor.PerfDataItem{Name: "pending", Value: float64(nfo.NumPending), Help: "The number of messages that have not yet been consumed", Crit: float64(c.consumerUnprocessedCritical)})
	check.Pd(&monitor.PerfDataItem{Name: "redelivered", Value: float64(nfo.NumRedelivered), Help: "The number of messages currently being redelivered", Crit: float64(c.consumerRedeliveryCritical)})
	if nfo.Delivered.Last != nil {
		check.Pd(&monitor.PerfDataItem{Name: "last_delivery", Value: time.Since(*nfo.Delivered.Last).Seconds(), Unit: "s", Help: "Seconds since the last message was delivered", Crit: float64(c.consumerLastDeliveryCritical)})
	}
	if nfo.AckFloor.Last != nil {
		check.Pd(&monitor.PerfDataItem{Name: "last_ack", Value: time.Since(*nfo.AckFloor.Last).Seconds(), Unit: "s", Help: "Seconds since the last message was acknowledged", Crit: float64(c.consumerLastDeliveryCritical)})
	}

	if c.consumerAckOutstandingCritical > 0 && nfo.NumAckPending >= c.consumerAckOutstandingCritical {
		check.Critical("Ack Pending: %d", nfo.NumAckPending)
	}

	if c.consumerWaitingCritical > 0 && nfo.NumWaiting >= c.consumerWaitingCritical {
		check.Critical("Waiting Pulls: %d", nfo.NumWaiting)
	}

	if c.consumerUnprocessedCritical > 0 && nfo.NumPending >= uint64(c.consumerUnprocessedCritical) {
		check.Critical("Unprocessed Messages: %d", nfo.NumPending)
	}

	if c.consumerRedeliveryCritical > 0 && nfo.NumRedelivered > c.consumerRedeliveryCritical {
		check.Critical("Redelivered Messages: %d", nfo.NumRedelivered)
	}

	switch {
	case c.consumerLastDeliveryCritical <= 0:
	case nfo.Delivered.Last == nil:
		check.Critical("No deliveries")
	case time.Since(*nfo.Delivered.Last) >= c.consumerLastDeliveryCritical:
		check.Critical("Last delivery %v ago", time.Since(*nfo.Delivered.Last).Round(time.Second))
	}

	switch {
	case c.consumerLastAckCritical <= 0:
	case nfo.AckFloor.Last == nil:
		check.Critical("No acknowledgements")
	case time.Since(*nfo.AckFloor.Last) >= c.consumerLastAckCritical:
		check.Critical("Last ack %v ago", time.Since(*nfo.AckFloor.Last).Round(time.Second))
	}
}

func (c *SrvCheckCmd) checkConsumer(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: fmt.Sprintf("%s_%s", c.sourcesStream, c.consumerName), Check: "consumer", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	cons, err := mgr.LoadConsumer(c.sourcesStream, c.consumerName)
	if err != nil {
		check.Critical("consumer load failure: %v", err)
		return nil
	}

	nfo, err := cons.LatestState()
	if err != nil {
		check.Critical("consumer state failure: %v", err)
		return nil
	}

	c.checkConsumerStatus(check, nfo)

	return nil
}

func (c *SrvCheckCmd) checkKV(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.kvBucket, Check: "kv", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	nc, _, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	c.checkKVStatusAndBucket(check, nc)

	return nil
}

func (c *SrvCheckCmd) checkSrv(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: c.srvName, Check: "server", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	vz, err := c.fetchVarz()
	check.CriticalIfErr(err, "could not retrieve VARZ information: %s", err)

	err = c.checkVarz(check, vz)
	check.CriticalIfErr(err, "check failed: %s", err)

	return nil
}

func (c *SrvCheckCmd) checkVarz(check *monitor.Result, vz *server.Varz) error {
	if vz == nil {
		return fmt.Errorf("no data received")
	}

	if vz.Name != c.srvName {
		return fmt.Errorf("result from %s", vz.Name)
	}

	if c.srvJSRequired {
		if vz.JetStream.Config == nil {
			check.Critical("JetStream not enabled")
		} else {
			check.Ok("JetStream enabled")
		}
	}

	if c.srvTLSRequired {
		if vz.TLSRequired {
			check.Ok("TLS required")
		} else {
			check.Critical("TLS not required")
		}
	}

	if c.srvAuthRequire {
		if vz.AuthRequired {
			check.Ok("Authentication required")
		} else {
			check.Critical("Authentication not required")
		}
	}

	up := vz.Now.Sub(vz.Start)
	if c.srvUptimeWarn > 0 || c.srvUptimeCrit > 0 {
		if c.srvUptimeCrit > c.srvUptimeWarn {
			check.Critical("Up invalid thresholds")
			return nil
		}

		if up <= c.srvUptimeCrit {
			check.Critical("Up %s", f(up))
		} else if up <= c.srvUptimeWarn {
			check.Warn("Up %s", f(up))
		} else {
			check.Ok("Up %s", f(up))
		}
	}

	check.Pd(
		&monitor.PerfDataItem{Name: "uptime", Value: up.Seconds(), Warn: c.srvUptimeWarn.Seconds(), Crit: c.srvUptimeCrit.Seconds(), Unit: "s", Help: "NATS Server uptime in seconds"},
		&monitor.PerfDataItem{Name: "cpu", Value: vz.CPU, Warn: float64(c.srvCPUWarn), Crit: float64(c.srvCPUCrit), Unit: "%", Help: "NATS Server CPU usage in percentage"},
		&monitor.PerfDataItem{Name: "mem", Value: float64(vz.Mem), Warn: float64(c.srvMemWarn), Crit: float64(c.srvMemCrit), Help: "NATS Server memory usage in bytes"},
		&monitor.PerfDataItem{Name: "connections", Value: float64(vz.Connections), Warn: float64(c.srvConnWarn), Crit: float64(c.srvConnCrit), Help: "Active connections"},
		&monitor.PerfDataItem{Name: "subscriptions", Value: float64(vz.Subscriptions), Warn: float64(c.srvSubsWarn), Crit: float64(c.srvSubCrit), Help: "Active subscriptions"},
	)

	checkVal := func(name string, crit float64, warn float64, value float64, r bool) {
		if crit == 0 && warn == 0 {
			return
		}

		if !r && crit < warn {
			check.Critical("%s invalid thresholds", name)
			return
		}

		if r && crit < warn {
			if value <= crit {
				check.Critical("%s %.2f", name, value)
			} else if value <= warn {
				check.Warn("%s %.2f", name, value)
			} else {
				check.Ok("%s %.2f", name, value)
			}
		} else {
			if value >= crit {
				check.Critical("%s %.2f", name, value)
			} else if value >= warn {
				check.Warn("%s %.2f", name, value)
			} else {
				check.Ok("%s %.2f", name, value)
			}
		}
	}

	checkVal("CPU", float64(c.srvCPUCrit), float64(c.srvCPUWarn), vz.CPU, false)
	checkVal("Memory", float64(c.srvMemCrit), float64(c.srvMemWarn), float64(vz.Mem), false)
	checkVal("Connections", float64(c.srvConnCrit), float64(c.srvConnWarn), float64(vz.Connections), true)
	checkVal("Subscriptions", float64(c.srvSubCrit), float64(c.srvSubsWarn), float64(vz.Subscriptions), true)

	return nil
}

func (c *SrvCheckCmd) fetchVarz() (*server.Varz, error) {
	var vz json.RawMessage

	if c.srvURL == nil {
		nc, _, err := prepareHelper("", natsOpts()...)
		if err != nil {
			return nil, err
		}

		if c.srvName == "" {
			return nil, fmt.Errorf("server name is required")
		}

		res, err := doReq(server.VarzEventOptions{EventFilterOptions: server.EventFilterOptions{Name: c.srvName}}, "$SYS.REQ.SERVER.PING.VARZ", 1, nc)
		if err != nil {
			return nil, err
		}

		if len(res) != 1 {
			return nil, fmt.Errorf("received %d responses for %s", len(res), c.srvName)
		}

		reqresp := map[string]json.RawMessage{}
		err = json.Unmarshal(res[0], &reqresp)
		if err != nil {
			return nil, err
		}

		errresp, ok := reqresp["error"]
		if ok {
			return nil, fmt.Errorf("invalid response received: %#v", errresp)
		}

		vz = reqresp["data"]
	} else {
		return nil, fmt.Errorf("not implemented")
	}

	if len(vz) == 0 {
		return nil, fmt.Errorf("no data received for %s", c.srvName)
	}

	varz := &server.Varz{}
	err := json.Unmarshal(vz, varz)
	if err != nil {
		return nil, err
	}

	return varz, nil
}

func (c *SrvCheckCmd) checkJS(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "JetStream", Check: "jetstream", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
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
	check := &monitor.Result{Name: c.sourcesStream, Check: "stream", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed: %s", err)

	stream, err := mgr.LoadStream(c.sourcesStream)
	check.CriticalIfErr(err, "could not load stream %s: %s", c.sourcesStream, err)

	info, err := stream.LatestInformation()
	check.CriticalIfErr(err, "could not load stream %s info: %s", c.sourcesStream, err)

	if info.Cluster != nil {
		var sci server.ClusterInfo
		cij, _ := json.Marshal(info.Cluster)
		json.Unmarshal(cij, &sci)
		err = c.checkClusterInfo(check, &sci)
		check.CriticalIfErr(err, "Invalid cluster data: %s", err)

		if len(check.Criticals) == 0 {
			check.Ok("%d current replicas", len(info.Cluster.Replicas)+1)
		}
	} else if c.raftExpect > 0 {
		check.Critical("not clustered expected %d peers", c.raftExpect)
	}

	check.Pd(&monitor.PerfDataItem{Name: "messages", Value: float64(info.State.Msgs), Warn: float64(c.sourcesMessagesWarn), Crit: float64(c.sourcesMessagesCrit), Help: "Messages stored in the stream"})
	if c.sourcesMessagesWarn > 0 && info.State.Msgs <= c.sourcesMessagesWarn {
		check.Warn("%d messages", info.State.Msgs)
	}
	if c.sourcesMessagesCrit > 0 && info.State.Msgs <= c.sourcesMessagesCrit {
		check.Critical("%d messages", info.State.Msgs)
	}

	check.Pd(&monitor.PerfDataItem{Name: "subjects", Value: float64(info.State.NumSubjects), Warn: float64(c.subjectsWarn), Crit: float64(c.subjectsCrit), Help: "Number of subjects stored in the stream"})
	if c.subjectsWarn > 0 || c.subjectsCrit > 0 {
		ns := info.State.NumSubjects
		if c.subjectsWarn < c.subjectsCrit { // it means we're asserting that there are fewer subjects than thresholds
			if ns >= c.subjectsCrit {
				check.Critical("%d subjects", info.State.NumSubjects)
			} else if ns >= c.subjectsWarn {
				check.Warn("%d subjects", info.State.NumSubjects)
			}
		} else { // it means we're asserting that there are more subjects than thresholds
			if ns <= c.subjectsCrit {
				check.Critical("%d subjects", info.State.NumSubjects)
			} else if ns <= c.subjectsWarn {
				check.Warn("%d subjects", info.State.NumSubjects)
			}
		}
	}

	switch {
	case stream.IsMirror():
		err = c.checkMirror(check, info)
		check.CriticalIfErr(err, "Invalid mirror data: %s", err)

		if len(check.Criticals) == 0 {
			check.Ok("%s mirror of %s is %d lagged, last seen %s ago", c.sourcesStream, info.Mirror.Name, info.Mirror.Lag, info.Mirror.Active.Round(time.Millisecond))
		}

	case stream.IsSourced():
		err = c.checkSources(check, info)
		check.CriticalIfErr(err, "Invalid source data: %s", err)

		if len(check.Criticals) == 0 {
			check.Ok("%d sources", len(info.Sources))
		}
	}

	return nil
}

func (c *SrvCheckCmd) checkMirror(check *monitor.Result, info *api.StreamInfo) error {
	if info.Mirror == nil {
		check.Critical("not mirrored")
		return nil
	}

	check.Pd(
		&monitor.PerfDataItem{Name: "lag", Crit: float64(c.sourcesLagCritical), Value: float64(info.Mirror.Lag), Help: "Number of operations this peer is behind its origin"},
		&monitor.PerfDataItem{Name: "active", Crit: c.sourcesSeenCritical.Seconds(), Unit: "s", Value: info.Mirror.Active.Seconds(), Help: "Indicates if this peer is active and catching up if lagged"},
	)

	if c.sourcesLagCritical > 0 && info.Mirror.Lag > c.sourcesLagCritical {
		check.Critical("%d messages behind", info.Mirror.Lag)
	}

	if c.sourcesSeenCritical > 0 && info.Mirror.Active > c.sourcesSeenCritical {
		check.Critical("last active %s", info.Mirror.Active)
	}

	return nil
}

func (c *SrvCheckCmd) checkSources(check *monitor.Result, info *api.StreamInfo) error {
	check.Pd(&monitor.PerfDataItem{Name: "sources", Value: float64(len(info.Sources)), Warn: float64(c.sourcesMinSources), Crit: float64(c.sourcesMaxSources), Help: "Number of sources being consumed by this stream"})

	if len(info.Sources) == 0 {
		check.Critical("no sources defined")
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

	check.Pd(
		&monitor.PerfDataItem{Name: "sources_lagged", Value: float64(lagged), Help: "Number of sources that are behind more than the configured threshold"},
		&monitor.PerfDataItem{Name: "sources_inactive", Value: float64(inactive), Help: "Number of sources that are inactive"},
	)

	if lagged > 0 {
		check.Critical("%d lagged sources", lagged)
	}
	if inactive > 0 {
		check.Critical("%d inactive sources", inactive)
	}
	if len(info.Sources) < c.sourcesMinSources {
		check.Critical("%d sources of min expected %d", len(info.Sources), c.sourcesMinSources)
	}
	if len(info.Sources) > c.sourcesMaxSources {
		check.Critical("%d sources of max expected %d", len(info.Sources), c.sourcesMaxSources)
	}

	return nil
}

func (c *SrvCheckCmd) checkStreamMessage(mgr *jsm.Manager, check *monitor.Result) error {
	msg, err := mgr.ReadLastMessageForSubject(c.sourcesStream, c.msgSubject)
	if jsm.IsNatsError(err, 10037) {
		check.Critical("no message found")
		return nil
	}
	check.CriticalIfErr(err, "msg load failed: %v", err)

	ts := msg.Time
	if c.msgBodyAsTs {
		i, err := strconv.ParseInt(string(bytes.TrimSpace(msg.Data)), 10, 64)
		check.CriticalIfErr(err, "invalid timestamp body: %v", err)
		ts = time.Unix(i, 0)
	}

	check.Pd(&monitor.PerfDataItem{
		Help:  "The age of the message",
		Name:  "age",
		Value: time.Since(ts).Round(time.Millisecond).Seconds(),
		Warn:  c.msgAgeWarn.Seconds(),
		Crit:  c.msgAgeCrit.Seconds(),
		Unit:  "s",
	})

	check.Pd(&monitor.PerfDataItem{
		Help:  "The size of the message",
		Name:  "size",
		Value: float64(len(msg.Data)),
		Unit:  "B",
	})

	since := time.Since(ts)

	if c.msgAgeWarn > 0 || c.msgAgeCrit > 0 {
		if c.msgAgeCrit > 0 && since > c.msgAgeCrit {
			check.Critical("%v old", since.Round(time.Millisecond))
		} else if c.msgAgeWarn > 0 && since > c.msgAgeWarn {
			check.Warn("%v old", time.Since(ts).Round(time.Millisecond))
		}
	}

	if c.msgRegexp != nil && !c.msgRegexp.Match(msg.Data) {
		check.Critical("does not match regex: %s", c.msgRegexp.String())
	}

	check.Ok("Valid message on %s > %s", c.sourcesStream, c.msgSubject)

	return nil
}

func (c *SrvCheckCmd) checkMsg(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Stream Message", Check: "message", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed")

	return c.checkStreamMessage(mgr, check)
}

func (c *SrvCheckCmd) checkConnection(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Connection", Check: "connections", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	connStart := time.Now()
	nc, _, err := prepareHelper("", natsOpts()...)
	check.CriticalIfErr(err, "connection failed")

	ct := time.Since(connStart)
	check.Pd(&monitor.PerfDataItem{Name: "connect_time", Value: ct.Seconds(), Warn: c.connectWarning.Seconds(), Crit: c.connectCritical.Seconds(), Unit: "s", Help: "Time taken to connect to NATS"})

	if ct >= c.connectCritical {
		check.Critical("connected to %s, connect time exceeded %v", nc.ConnectedUrl(), c.connectCritical)
	} else if ct >= c.connectWarning {
		check.Warn("connected to %s, connect time exceeded %v", nc.ConnectedUrl(), c.connectWarning)
	} else {
		check.Ok("connected to %s in %s", nc.ConnectedUrl(), ct)
	}

	rtt, err := nc.RTT()
	check.CriticalIfErr(err, "rtt failed: %s", err)

	check.Pd(&monitor.PerfDataItem{Name: "rtt", Value: rtt.Seconds(), Warn: c.rttWarning.Seconds(), Crit: c.rttCritical.Seconds(), Unit: "s", Help: "The round-trip-time of the connection"})
	if rtt >= c.rttCritical {
		check.Critical("rtt time exceeded %v", c.rttCritical)
	} else if rtt >= c.rttWarning {
		check.Critical("rtt time exceeded %v", c.rttWarning)
	} else {
		check.Ok("rtt time %v", rtt)
	}

	msg := []byte(randomPassword(100))
	ib := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(ib)
	check.CriticalIfErr(err, "could not subscribe to %s: %s", ib, err)
	sub.AutoUnsubscribe(1)

	start := time.Now()
	err = nc.Publish(ib, msg)
	check.CriticalIfErr(err, "could not publish to %s: %s", ib, err)

	received, err := sub.NextMsg(opts.Timeout)
	check.CriticalIfErr(err, "did not receive from %s: %s", ib, err)

	reqt := time.Since(start)
	check.Pd(&monitor.PerfDataItem{Name: "request_time", Value: reqt.Seconds(), Warn: c.reqWarning.Seconds(), Crit: c.reqCritical.Seconds(), Unit: "s", Help: "Time taken for a full Request-Reply operation"})

	if !bytes.Equal(received.Data, msg) {
		check.Critical("did not receive expected message")
	}

	if reqt >= c.reqCritical {
		check.Critical("round trip request took %f", reqt.Seconds())
	} else if reqt >= c.reqWarning {
		check.Warn("round trip request took %f", reqt.Seconds())
	} else {
		check.Ok("round trip took %fs", reqt.Seconds())
	}

	return nil
}

func (c *SrvCheckCmd) checkCredential(check *monitor.Result) error {
	ok, err := fileAccessible(c.credential)
	if err != nil {
		check.Critical("credential not accessible: %v", err)
		return nil
	}

	if !ok {
		check.Critical("credential not accessible")
		return nil
	}

	cb, err := os.ReadFile(c.credential)
	if err != nil {
		check.Critical("credential not accessible: %v", err)
		return nil
	}

	token, err := nkeys.ParseDecoratedJWT(cb)
	if err != nil {
		check.Critical("invalid credential: %v", err)
		return nil
	}

	claims, err := jwt.Decode(token)
	if err != nil {
		check.Critical("invalid credential: %v", err)
	}

	now := time.Now().UTC().Unix()
	cd := claims.Claims()
	until := cd.Expires - now
	crit := int64(c.credentialValidityCrit.Seconds())
	warn := int64(c.credentialValidityWarn.Seconds())

	check.Pd(&monitor.PerfDataItem{Help: "Expiry time in seconds", Name: "expiry", Value: float64(until), Warn: float64(warn), Crit: float64(crit), Unit: "s"})

	switch {
	case cd.Expires == 0 && c.credentialRequiresExpire:
		check.Critical("never expires")
	case c.credentialValidityCrit > 0 && (until <= crit):
		check.Critical("expires sooner than %s", f(c.credentialValidityCrit))
	case c.credentialValidityWarn > 0 && (until <= warn):
		check.Warn("expires sooner than %s", f(c.credentialValidityWarn))
	default:
		check.Ok("expires in %s", time.Unix(cd.Expires, 0).UTC())
	}

	return nil
}

func (c *SrvCheckCmd) checkCredentialAction(_ *fisk.ParseContext) error {
	check := &monitor.Result{Name: "Credential", Check: "credential", OutFile: checkRenderOutFile, NameSpace: opts.PrometheusNamespace, RenderFormat: checkRenderFormat}
	defer check.GenericExit()

	return c.checkCredential(check)
}

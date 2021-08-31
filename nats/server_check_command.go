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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
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

	jsMemWarn           int
	jsMemCritical       int
	jsStoreWarn         int
	jsStoreCritical     int
	jsStreamsWarn       int
	jsStreamsCritical   int
	jsConsumersWarn     int
	jsConsumersCritical int

	srvName        string
	srvCPUWarn     int
	srvCPUCrit     int
	srvMemWarn     int
	srvMemCrit     int
	srvConnWarn    int
	srvConnCrit    int
	srvRouteWarn   int
	srvRouteCrit   int
	srvGwayWarn    int
	srvGwayCrit    int
	srvSubsWarn    int
	srvSubCrit     int
	srvUptimeWarn  time.Duration
	srvUptimeCrit  time.Duration
	srvAuthRequire bool
	srvTLSRequired bool
	srvJSRequired  bool
	srvURL         *url.URL
}

func configureServerCheckCommand(srv *kingpin.CmdClause) {
	c := &SrvCheckCmd{}

	help := `Nagios protocol health check for NATS servers

   connection  - connects and does a request-reply check
   stream      - checks JetStream streams for source, mirror and cluster health
   meta        - JetStream Meta Cluster health
`
	check := srv.Command("check", help)
	check.Flag("format", "Render the check in a specific format").Default("nagios").EnumVar(&checkRenderFormat, "nagios", "json", "prometheus")
	check.Flag("outfile", "Save output to a file rather than STDOUT").StringVar(&checkRenderOutFile)

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
	stream.Flag("msgs-critical", "Critical if there are fewer than this many messages in the stream").PlaceHolder("MSGS").Uint64Var(&c.sourcesMessagesCrit)

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

	serv := check.Command("server", "Checks a NATS Server health").Action(c.checkSrv)
	serv.Flag("name", "Server name to require in the result").Required().StringVar(&c.srvName)
	serv.Flag("cpu-warn", "Warning threshold for CPU usage, in percent").IntVar(&c.srvCPUWarn)
	serv.Flag("cpu-critical", "Critical threshold for CPU usage, in percent").IntVar(&c.srvCPUCrit)
	serv.Flag("mem-warn", "Warning threshold for Memory usage, in percent").IntVar(&c.srvMemWarn)
	serv.Flag("mem-critical", "Critical threshold Memory CPU usage, in percent").IntVar(&c.srvMemCrit)
	serv.Flag("conn-warn", "Warning threshold for connections, supports inversion").IntVar(&c.srvConnWarn)
	serv.Flag("conn-critical", "Critical threshold for connections, supports inversion").IntVar(&c.srvConnCrit)
	serv.Flag("route-warn", "Warning threshold for number of active routes, supports inversion").IntVar(&c.srvRouteWarn)
	serv.Flag("route-critical", "Critical threshold for number of active routes, supports inversion").IntVar(&c.srvRouteCrit)
	serv.Flag("gateway-warn", "Warning threshold for gateway connections, supports inversion").IntVar(&c.srvGwayWarn)
	serv.Flag("gateway-critical", "Critical threshold for gateway connections, supports inversion").IntVar(&c.srvGwayCrit)
	serv.Flag("subs-warn", "Warning threshold for number of active subscriptions, supports inversion").IntVar(&c.srvSubsWarn)
	serv.Flag("subs-critical", "Critical threshold for number of active subscriptions, supports inversion").IntVar(&c.srvSubCrit)
	serv.Flag("uptime-warn", "Warning threshold for server uptime as duration").DurationVar(&c.srvUptimeWarn)
	serv.Flag("uptime-critical", "Critical threshold for server uptime as duration").DurationVar(&c.srvUptimeCrit)
	serv.Flag("auth-required", "Checks that authentication is enabled").Default("false").BoolVar(&c.srvAuthRequire)
	serv.Flag("tls-required", "Checks that TLS is required").Default("false").BoolVar(&c.srvTLSRequired)
	serv.Flag("js-required", "Checks that JetStream is enabled").Default("false").BoolVar(&c.srvJSRequired)
	// serv.Flag("url", "Fetch varz over this URL instead of over the NATS protocol").URLVar(&c.srvURL)
}

type checkStatus string

var (
	checkRenderFormat              = "nagios"
	checkRenderOutFile             = ""
	okCheckStatus      checkStatus = "OK"
	warnCheckStatus    checkStatus = "WARNING"
	critCheckStatus    checkStatus = "CRITICAL"
	unknownCheckStatus checkStatus = "UNKNOWN"
)

type result struct {
	Output    string      `json:"output,omitempty"`
	Status    checkStatus `json:"status"`
	Check     string      `json:"check_suite"`
	Name      string      `json:"check_name"`
	Warnings  []string    `json:"warning,omitempty"`
	Criticals []string    `json:"critical,omitempty"`
	OKs       []string    `json:"ok,omitempty"`
	PerfData  perfData    `json:"perf_data"`
}

func (r *result) pd(pd ...*perfDataItem) {
	r.PerfData = append(r.PerfData, pd...)
}

func (r *result) criticalExit(format string, a ...interface{}) {
	r.critical(format, a...)
	r.GenericExit()
}

func (r *result) critical(format string, a ...interface{}) {
	r.Criticals = append(r.Criticals, fmt.Sprintf(format, a...))
}

func (r *result) warn(format string, a ...interface{}) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, a...))
}

func (r *result) ok(format string, a ...interface{}) {
	r.OKs = append(r.OKs, fmt.Sprintf(format, a...))
}

func (r *result) criticalIfErr(err error, format string, a ...interface{}) bool {
	if err == nil {
		return false
	}

	r.criticalExit(format, a...)

	return true
}

func (r *result) exitCode() int {
	if checkRenderFormat == "prometheus" {
		return 0
	}

	switch r.Status {
	case okCheckStatus:
		return 0
	case warnCheckStatus:
		return 1
	case critCheckStatus:
		return 2
	default:
		return 3
	}
}

func (r *result) Exit() {
	os.Exit(r.exitCode())
}

func (r *result) renderPrometheus() string {
	if r.Check == "" {
		r.Check = r.Name
	}

	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry

	sname := strings.ReplaceAll(r.Name, `"`, `.`)
	for _, pd := range r.PerfData {
		help := fmt.Sprintf("Data about the NATS CLI check %s", r.Check)
		if pd.Help != "" {
			help = pd.Help
		}

		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: fmt.Sprintf("nats_server_check_%s_%s", r.Check, pd.Name),
			Help: help,
		}, []string{"item"})
		prometheus.MustRegister(gauge)
		gauge.WithLabelValues(sname).Set(pd.Value)
	}

	status := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("nats_server_check_%s_status_code", r.Check),
		Help: fmt.Sprintf("Nagios compatible status code for %s", r.Check),
	}, []string{"item", "status"})
	prometheus.MustRegister(status)

	status.WithLabelValues(sname, string(r.Status)).Set(float64(r.exitCode()))

	var buf bytes.Buffer

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		panic(err)
	}

	for _, mf := range mfs {
		_, err = expfmt.MetricFamilyToText(&buf, mf)
		if err != nil {
			panic(err)
		}
	}

	return buf.String()
}

func (r *result) renderJSON() string {
	res, _ := json.MarshalIndent(r, "", "  ")
	return string(res)
}

func (r *result) renderNagios() string {
	res := []string{r.Name}
	for _, c := range r.Criticals {
		res = append(res, fmt.Sprintf("Crit:%s", c))
	}

	for _, w := range r.Warnings {
		res = append(res, fmt.Sprintf("Warn:%s", w))
	}

	if r.Output != "" {
		res = append(res, r.Output)
	} else if len(r.OKs) > 0 {
		for _, ok := range r.OKs {
			res = append(res, fmt.Sprintf("OK:%s", ok))
		}
	}

	if len(r.PerfData) == 0 {
		return fmt.Sprintf("%s %s", r.Status, strings.Join(res, " "))
	} else {
		return fmt.Sprintf("%s %s | %s", r.Status, strings.Join(res, " "), r.PerfData)
	}
}

func (r *result) String() string {
	if r.Status == "" {
		r.Status = unknownCheckStatus
	}
	if r.PerfData == nil {
		r.PerfData = perfData{}
	}

	switch {
	case len(r.Criticals) > 0:
		r.Status = critCheckStatus
	case len(r.Warnings) > 0:
		r.Status = warnCheckStatus
	default:
		r.Status = okCheckStatus
	}

	switch checkRenderFormat {
	case "json":
		return r.renderJSON()
	case "prometheus":
		return r.renderPrometheus()
	default:
		return r.renderNagios()
	}
}

func (r *result) GenericExit() {
	if checkRenderOutFile != "" {
		f, err := os.CreateTemp(filepath.Dir(checkRenderOutFile), "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp file failed: %s", err)
			os.Exit(1)
		}
		defer os.Remove(f.Name())

		_, err = fmt.Fprintln(f, r.String())
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp file write failed: %s", err)
			os.Exit(1)
		}

		err = f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp file write failed: %s", err)
			os.Exit(1)
		}

		err = os.Chmod(f.Name(), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp file mode change failed: %s", err)
			os.Exit(1)
		}

		err = os.Rename(f.Name(), checkRenderOutFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "temp file rename failed: %s", err)
		}

		os.Exit(1)
	}

	fmt.Println(r.String())

	r.Exit()
}

type perfDataItem struct {
	Help  string  `json:"-"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Warn  float64 `json:"warning"`
	Crit  float64 `json:"critical"`
	Unit  string  `json:"unit,omitempty"`
}

type perfData []*perfDataItem

func (p perfData) String() string {
	res := []string{}
	for _, i := range p {
		res = append(res, i.String())
	}

	return strings.TrimSpace(strings.Join(res, " "))
}

func (i *perfDataItem) String() string {
	valueFmt := "%0.0f"
	if i.Unit == "s" {
		valueFmt = "%0.4f"
	}

	pd := fmt.Sprintf("%s="+valueFmt, i.Name, i.Value)
	if i.Unit != "" {
		pd = pd + i.Unit
	}

	if i.Warn > 0 || i.Crit > 0 {
		if i.Warn != 0 {
			pd = fmt.Sprintf("%s;"+valueFmt, pd, i.Warn)
		} else if i.Crit > 0 {
			pd = fmt.Sprintf("%s;", pd)
		}

		if i.Crit != 0 {
			pd = fmt.Sprintf("%s;"+valueFmt, pd, i.Crit)
		}
	}

	return pd
}

func (c *SrvCheckCmd) checkSrv(_ *kingpin.ParseContext) error {
	check := &result{Name: c.srvName, Check: "server"}
	defer check.GenericExit()

	vz, err := c.fetchVarz()
	check.criticalIfErr(err, "could not retrieve VARZ information: %s", err)

	err = c.checkVarz(check, vz)
	check.criticalIfErr(err, "check failed: %s", err)

	return nil
}

func (c *SrvCheckCmd) checkVarz(check *result, vz *server.Varz) error {
	if vz == nil {
		return fmt.Errorf("no data received")
	}

	if vz.Name != c.srvName {
		return fmt.Errorf("result from %s", vz.Name)
	}

	if c.srvJSRequired {
		if vz.JetStream.Config == nil {
			check.critical("JetStream not enabled")
		} else {
			check.ok("JetStream enabled")
		}
	}

	if c.srvTLSRequired {
		if vz.TLSRequired {
			check.ok("TLS required")
		} else {
			check.critical("TLS not required")
		}
	}

	if c.srvAuthRequire {
		if vz.AuthRequired {
			check.ok("Authentication required")
		} else {
			check.critical("Authentication not required")
		}
	}

	up := time.Since(vz.Start)
	if c.srvUptimeWarn > 0 || c.srvUptimeCrit > 0 {
		if c.srvUptimeCrit > c.srvUptimeWarn {
			check.critical("Up invalid thresholds")
			return nil
		}

		if up <= c.srvUptimeCrit {
			check.critical("Up %s", humanizeDuration(up))
		} else if up <= c.srvUptimeWarn {
			check.warn("Up %s", humanizeDuration(up))
		} else {
			check.ok("Up %s", humanizeDuration(up))
		}
	}

	check.pd(
		&perfDataItem{Name: "uptime", Value: up.Round(time.Second).Seconds(), Warn: c.srvUptimeWarn.Seconds(), Crit: c.srvUptimeCrit.Seconds(), Unit: "s", Help: "NATS Server uptime in seconds"},
		&perfDataItem{Name: "cpu", Value: vz.CPU, Warn: float64(c.srvCPUWarn), Crit: float64(c.srvCPUCrit), Unit: "%", Help: "NATS Server CPU usage in percentage"},
		&perfDataItem{Name: "mem", Value: float64(vz.Mem), Warn: float64(c.srvMemWarn), Crit: float64(c.srvMemCrit), Help: "NATS Server memory usage in bytes"},
		&perfDataItem{Name: "connections", Value: float64(vz.Connections), Warn: float64(c.srvConnWarn), Crit: float64(c.srvConnCrit), Help: "Active connections"},
		&perfDataItem{Name: "routes", Value: float64(vz.Routes), Warn: float64(c.srvRouteWarn), Crit: float64(c.srvRouteCrit), Help: "Active connected route connects"},
		&perfDataItem{Name: "gateways", Value: float64(vz.Remotes), Warn: float64(c.srvGwayWarn), Crit: float64(c.srvGwayCrit), Help: "Active connected gateway connections"},
		&perfDataItem{Name: "subscriptions", Value: float64(vz.Subscriptions), Warn: float64(c.srvSubsWarn), Crit: float64(c.srvSubCrit), Help: "Active subscriptions"},
	)

	checkVal := func(name string, crit float64, warn float64, value float64, r bool) {
		if crit == 0 && warn == 0 {
			return
		}

		if !r && crit < warn {
			check.critical("%s invalid thresholds", name)
			return
		}

		if r && crit < warn {
			if value <= crit {
				check.critical("%s %.2f", name, value)
			} else if value <= warn {
				check.warn("%s %.2f", name, value)
			} else {
				check.ok("%s %.2f", name, value)
			}
		} else {
			if value >= crit {
				check.critical("%s %.2f", name, value)
			} else if value >= warn {
				check.warn("%s %.2f", name, value)
			} else {
				check.ok("%s %.2f", name, value)
			}
		}
	}

	checkVal("CPU", float64(c.srvCPUCrit), float64(c.srvCPUWarn), vz.CPU, false)
	checkVal("Memory", float64(c.srvMemCrit), float64(c.srvMemWarn), float64(vz.Mem), false)
	checkVal("Connections", float64(c.srvConnCrit), float64(c.srvConnWarn), float64(vz.Connections), true)
	checkVal("Routes", float64(c.srvRouteCrit), float64(c.srvRouteWarn), float64(vz.Routes), true)
	checkVal("Gateways", float64(c.srvGwayCrit), float64(c.srvGwayWarn), float64(len(vz.Gateway.Gateways)), true)
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

func (c *SrvCheckCmd) checkJS(_ *kingpin.ParseContext) error {
	check := &result{Name: "JetStream", Check: "jetstream"}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.criticalIfErr(err, "connection failed: %s", err)

	info, err := mgr.JetStreamAccountInfo()
	check.criticalIfErr(err, "JetStream not available: %s", err)

	err = c.checkAccountInfo(check, info)
	check.criticalIfErr(err, "JetStream not available: %s", err)

	return nil
}

func (c *SrvCheckCmd) checkAccountInfo(check *result, info *api.JetStreamAccountStats) error {
	if info == nil {
		return fmt.Errorf("invalid account status")
	}

	checkVal := func(item string, unit string, warn int, crit int, max int64, current uint64) {
		pct := 0
		if max > 0 {
			pct = int(float64(current) / float64(max) * 100)
		}

		check.pd(&perfDataItem{Name: item, Value: float64(current), Unit: unit, Help: fmt.Sprintf("JetStream %s resource usage", item)})
		check.pd(&perfDataItem{
			Name:  fmt.Sprintf("%s_pct", item),
			Value: float64(pct),
			Unit:  "%",
			Warn:  float64(warn),
			Crit:  float64(crit),
			Help:  fmt.Sprintf("JetStream %s resource usage in percent", item),
		})

		if warn != -1 && crit != -1 && warn >= crit {
			check.critical("%s: invalid thresholds", item)
			return
		}

		if pct > 100 {
			check.critical("%s: exceed server limits", item)
			return
		}

		if warn >= 0 && crit >= 0 {
			switch {
			case pct > crit:
				check.critical("%d%% %s", pct, item)
			case pct > warn:
				check.warn("%d%% %s", pct, item)
			}
		}
	}

	checkVal("memory", "B", c.jsMemWarn, c.jsMemCritical, info.Limits.MaxMemory, info.Memory)
	checkVal("storage", "B", c.jsStoreWarn, c.jsStoreCritical, info.Limits.MaxStore, info.Store)
	checkVal("streams", "", c.jsStreamsWarn, c.jsStreamsCritical, int64(info.Limits.MaxStreams), uint64(info.Streams))
	checkVal("consumers", "", c.jsConsumersWarn, c.jsConsumersCritical, int64(info.Limits.MaxConsumers), uint64(info.Consumers))

	return nil
}

func (c *SrvCheckCmd) checkRaft(_ *kingpin.ParseContext) error {
	check := &result{Name: "JetStream Meta Cluster", Check: "meta"}
	defer check.GenericExit()

	nc, _, err := prepareHelper("", natsOpts()...)
	check.criticalIfErr(err, "connection failed: %s", err)

	res, err := doReq(&server.JSzOptions{LeaderOnly: true}, "$SYS.REQ.SERVER.PING.JSZ", 1, nc)
	check.criticalIfErr(err, "JSZ API request failed: %s", err)

	if len(res) != 1 {
		check.criticalExit(fmt.Sprintf("JSZ API request returned %d results", len(res)))
		return nil
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	jszresp := &jszr{}
	err = json.Unmarshal(res[0], jszresp)
	check.criticalIfErr(err, "invalid result received: %s", err)

	err = c.checkClusterInfo(check, jszresp.Data.Meta)
	check.criticalIfErr(err, "invalid result received: %s", err)

	if len(check.Criticals) == 0 && len(check.Warnings) == 0 {
		check.ok("%d peers led by %s", len(jszresp.Data.Meta.Replicas)+1, jszresp.Data.Meta.Leader)
	}

	return nil
}

func (c *SrvCheckCmd) checkClusterInfo(check *result, ci *server.ClusterInfo) error {
	if ci == nil {
		check.critical("no cluster information")
		return nil
	}

	if ci.Leader == "" {
		check.critical("No leader")
		return nil
	}

	check.pd(&perfDataItem{
		Name:  "peers",
		Value: float64(len(ci.Replicas) + 1),
		Warn:  float64(c.raftExpect),
		Crit:  float64(c.raftExpect),
		Help:  "Configured RAFT peers",
	})

	if len(ci.Replicas)+1 != c.raftExpect {
		check.critical("%d peers of expected %d", len(ci.Replicas)+1, c.raftExpect)
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

	check.pd(
		&perfDataItem{Name: "peer_offline", Value: float64(offline), Help: "Offline RAFT peers"},
		&perfDataItem{Name: "peer_not_current", Value: float64(notCurrent), Help: "RAFT peers that are not current"},
		&perfDataItem{Name: "peer_inactive", Value: float64(inactive), Help: "Inactive RAFT peers"},
		&perfDataItem{Name: "peer_lagged", Value: float64(lagged), Help: "RAFT peers that are lagged more than configured threshold"},
	)

	if notCurrent > 0 {
		check.critical("%d not current", notCurrent)
	}
	if inactive > 0 {
		check.critical("%d inactive more than %s", inactive, c.raftSeenCritical)
	}
	if offline > 0 {
		check.critical("%d offline", offline)
	}
	if lagged > 0 {
		check.critical("%d lagged more than %d ops", lagged, c.raftLagCritical)
	}

	return nil
}

func (c *SrvCheckCmd) checkStream(_ *kingpin.ParseContext) error {
	check := &result{Name: c.sourcesStream, Check: "stream"}
	defer check.GenericExit()

	_, mgr, err := prepareHelper("", natsOpts()...)
	check.criticalIfErr(err, "connection failed: %s", err)

	stream, err := mgr.LoadStream(c.sourcesStream)
	check.criticalIfErr(err, "could not load stream %s: %s", c.sourcesStream, err)

	info, err := stream.LatestInformation()
	check.criticalIfErr(err, "could not load stream %s info: %s", c.sourcesStream, err)

	if info.Cluster != nil {
		var sci server.ClusterInfo
		cij, _ := json.Marshal(info.Cluster)
		json.Unmarshal(cij, &sci)
		err = c.checkClusterInfo(check, &sci)
		check.criticalIfErr(err, "Invalid cluster data: %s", err)

		if len(check.Criticals) == 0 {
			check.ok("%d current replicas", len(info.Cluster.Replicas)+1)
		}
	} else if c.raftExpect > 0 {
		check.critical("not clustered expected %d peers", c.raftExpect)
	}

	check.pd(&perfDataItem{Name: "messages", Value: float64(info.State.Msgs), Warn: float64(c.sourcesMessagesWarn), Crit: float64(c.sourcesMessagesCrit), Help: "Messages stored in the stream"})
	if c.sourcesMessagesWarn > 0 && info.State.Msgs <= c.sourcesMessagesWarn {
		check.warn("%d messages", info.State.Msgs)
	}
	if c.sourcesMessagesCrit > 0 && info.State.Msgs <= c.sourcesMessagesCrit {
		check.critical("%d messages", info.State.Msgs)
	}

	switch {
	case stream.IsMirror():
		err = c.checkMirror(check, info)
		check.criticalIfErr(err, "Invalid mirror data: %s", err)

		if len(check.Criticals) == 0 {
			check.ok("%s mirror of %s is %d lagged, last seen %s ago", c.sourcesStream, info.Mirror.Name, info.Mirror.Lag, info.Mirror.Active.Round(time.Millisecond))
		}

	case stream.IsSourced():
		err = c.checkSources(check, info)
		check.criticalIfErr(err, "Invalid source data: %s", err)

		if len(check.Criticals) == 0 {
			check.ok("%d sources", len(info.Sources))
		}
	}

	return nil
}

func (c *SrvCheckCmd) checkMirror(check *result, info *api.StreamInfo) error {
	if info.Mirror == nil {
		check.critical("not mirrored")
		return nil
	}

	check.pd(
		&perfDataItem{Name: "lag", Crit: float64(c.sourcesLagCritical), Value: float64(info.Mirror.Lag), Help: "Number of operations this peer is behind its origin"},
		&perfDataItem{Name: "active", Crit: c.sourcesSeenCritical.Seconds(), Unit: "s", Value: info.Mirror.Active.Seconds(), Help: "Indicates if this peer is active and catching up if lagged"},
	)

	if c.sourcesLagCritical > 0 && info.Mirror.Lag > c.sourcesLagCritical {
		check.critical("%d messages behind", info.Mirror.Lag)
	}

	if c.sourcesSeenCritical > 0 && info.Mirror.Active > c.sourcesSeenCritical {
		check.critical("last active %s", info.Mirror.Active)
	}

	return nil
}

func (c *SrvCheckCmd) checkSources(check *result, info *api.StreamInfo) error {
	check.pd(&perfDataItem{Name: "sources", Value: float64(len(info.Sources)), Warn: float64(c.sourcesMinSources), Crit: float64(c.sourcesMaxSources), Help: "Number of sources being consumed by this stream"})

	if len(info.Sources) == 0 {
		check.critical("no sources defined")
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

	check.pd(
		&perfDataItem{Name: "sources_lagged", Value: float64(lagged), Help: "Number of sources that are behind more than the configured threshold"},
		&perfDataItem{Name: "sources_inactive", Value: float64(inactive), Help: "Number of sources that are inactive"},
	)

	if lagged > 0 {
		check.critical("%d lagged sources", lagged)
	}
	if inactive > 0 {
		check.critical("%d inactive sources", inactive)
	}
	if len(info.Sources) < c.sourcesMinSources {
		check.critical("%d sources of min expected %d", len(info.Sources), c.sourcesMinSources)
	}
	if len(info.Sources) > c.sourcesMaxSources {
		check.critical("%d sources of max expected %d", len(info.Sources), c.sourcesMaxSources)
	}

	return nil
}

func (c *SrvCheckCmd) checkConnection(_ *kingpin.ParseContext) error {
	check := &result{Name: "Connection", Check: "connections"}
	defer check.GenericExit()

	connStart := time.Now()
	nc, _, err := prepareHelper("", natsOpts()...)
	check.criticalIfErr(err, "connection failed")

	ct := time.Since(connStart)
	check.pd(&perfDataItem{Name: "connect_time", Value: ct.Seconds(), Warn: c.connectWarning.Seconds(), Crit: c.connectCritical.Seconds(), Unit: "s", Help: "Time taken to connect to NATS"})

	if ct >= c.connectCritical {
		check.critical("connected to %s, connect time exceeded %v", nc.ConnectedUrl(), c.connectCritical)
	} else if ct >= c.connectWarning {
		check.warn("connected to %s, connect time exceeded %v", nc.ConnectedUrl(), c.connectWarning)
	} else {
		check.ok("connected to %s in %s", nc.ConnectedUrl(), ct)
	}

	rtt, err := nc.RTT()
	check.criticalIfErr(err, "rtt failed: %s", err)

	check.pd(&perfDataItem{Name: "rtt", Value: rtt.Seconds(), Warn: c.rttWarning.Seconds(), Crit: c.rttCritical.Seconds(), Unit: "s", Help: "The round-trip-time of the connection"})
	if rtt >= c.rttCritical {
		check.critical("rtt time exceeded %v", c.rttCritical)
	} else if rtt >= c.rttWarning {
		check.critical("rtt time exceeded %v", c.rttWarning)
	} else {
		check.ok("rtt time %v", rtt)
	}

	msg := []byte(randomPassword(100))
	ib := nats.NewInbox()
	sub, err := nc.SubscribeSync(ib)
	check.criticalIfErr(err, "could not subscribe to %s: %s", ib, err)
	sub.AutoUnsubscribe(1)

	start := time.Now()
	err = nc.Publish(ib, msg)
	check.criticalIfErr(err, "could not publish to %s: %s", ib, err)

	received, err := sub.NextMsg(timeout)
	check.criticalIfErr(err, "did not receive from %s: %s", ib, err)

	reqt := time.Since(start)
	check.pd(&perfDataItem{Name: "request_time", Value: reqt.Seconds(), Warn: c.reqWarning.Seconds(), Crit: c.reqCritical.Seconds(), Unit: "s", Help: "Time taken for a full Request-Reply operation"})

	if !bytes.Equal(received.Data, msg) {
		check.critical("did not receive expected message")
	}

	if reqt >= c.reqCritical {
		check.critical("round trip request took %f", reqt.Seconds())
	} else if reqt >= c.reqWarning {
		check.warn("round trip request took %f", reqt.Seconds())
	} else {
		check.ok("round trip took %fs", reqt.Seconds())
	}

	return nil
}

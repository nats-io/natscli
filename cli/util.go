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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/google/shlex"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/nats-io/natscli/options"
)

func selectConsumer(mgr *jsm.Manager, stream string, consumer string, force bool) (string, *jsm.Consumer, error) {
	if consumer != "" {
		c, err := mgr.LoadConsumer(stream, consumer)
		if err == nil {
			return c.Name(), c, err
		}
	}

	if force {
		return "", nil, fmt.Errorf("unknown consumer %q > %q", stream, consumer)
	}

	if !iu.IsTerminal() {
		return "", nil, fmt.Errorf("cannot pick a Consumer without a terminal and no Consumer name supplied")
	}

	consumers, err := mgr.ConsumerNames(stream)
	if err != nil {
		return "", nil, err
	}

	switch len(consumers) {
	case 0:
		return "", nil, fmt.Errorf("no Consumers are defined for Stream %s", stream)
	default:
		c := ""

		err = iu.AskOne(&survey.Select{
			Message:  "Select a Consumer",
			Options:  consumers,
			PageSize: iu.SelectPageSize(len(consumers)),
		}, &c)
		if err != nil {
			return "", nil, err
		}

		con, err := mgr.LoadConsumer(stream, c)
		if err != nil {
			return "", nil, err
		}
		return con.Name(), con, err
	}
}

func selectStream(mgr *jsm.Manager, stream string, force bool, all bool) (string, *jsm.Stream, error) {
	s, err := mgr.LoadStream(stream)
	if err == nil {
		return s.Name(), s, nil
	}

	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return "", nil, err
	}

	known := false
	var matched []string

	for _, s := range streams {
		if s == stream {
			known = true
			break
		}

		if all || !jsm.IsInternalStream(s) {
			matched = append(matched, s)
		}
	}

	if known {
		return stream, nil, nil
	}

	if !iu.IsTerminal() {
		return "", nil, fmt.Errorf("cannot pick a Stream without a terminal and no Stream name supplied")
	}

	if force {
		return "", nil, fmt.Errorf("unknown stream %q", stream)
	}

	switch len(matched) {
	case 0:
		return "", nil, errors.New("no Streams are defined")
	default:
		s := ""

		err = iu.AskOne(&survey.Select{
			Message:  "Select a Stream",
			Options:  matched,
			PageSize: iu.SelectPageSize(len(matched)),
		}, &s)
		if err != nil {
			return "", nil, err
		}

		return s, nil, nil
	}
}

func sinceRefOrNow(ref time.Time, ts time.Time) time.Duration {
	if ref.IsZero() {
		return time.Since(ts)
	}
	return ref.Sub(ts)
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	if !iu.IsTerminal() {
		return false, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	ans := dflt

	err := iu.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}

func askOneBytes(prompt string, dflt string, help string, required string) (int64, error) {
	if !iu.IsTerminal() {
		return 0, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	for {
		val := ""
		err := iu.AskOne(&survey.Input{
			Message: prompt,
			Default: dflt,
			Help:    help,
		}, &val, survey.WithValidator(survey.Required))
		if err != nil {
			return 0, err
		}

		if val == "-1" {
			val = "0"
		}

		i, err := iu.ParseStringAsBytes(val)
		if err != nil {
			return 0, err
		}

		if required != "" && i <= 0 {
			fmt.Println(required)
			continue
		}

		return i, nil
	}
}

func askOneInt(prompt string, dflt string, help string) (int64, error) {
	if !iu.IsTerminal() {
		return 0, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	val := ""
	err := iu.AskOne(&survey.Input{
		Message: prompt,
		Default: dflt,
		Help:    help,
	}, &val, survey.WithValidator(survey.Required))
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}

	return int64(i), nil
}

func natsOpts() []nats.Option {
	if opts().Config == nil {
		return []nats.Option{}
	}

	copts, err := opts().Config.NATSOptions()
	fisk.FatalIfError(err, "configuration error")

	connectionName := strings.TrimSpace(opts().ConnectionName)
	if len(connectionName) == 0 {
		connectionName = "NATS CLI Version " + Version
	}

	return append(copts, []nats.Option{
		nats.Name(connectionName),
		nats.MaxReconnects(-1),
		nats.ConnectHandler(func(conn *nats.Conn) {
			if opts().Trace {
				log.Printf(">>> Connected to %s", conn.ConnectedUrlRedacted())
			}
		}),
		nats.DiscoveredServersHandler(func(conn *nats.Conn) {
			if opts().Trace {
				log.Printf(">>> Discovered new servers, known servers are now %s", strings.Join(conn.Servers(), ", "))
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("Disconnected due to: %s, will attempt reconnect", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("Reconnected [%s]", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(nc *nats.Conn, _ *nats.Subscription, err error) {
			url := nc.ConnectedUrl()
			if url == "" {
				log.Printf("Unexpected NATS error: %s", err)
			} else {
				log.Printf("Unexpected NATS error from server %s: %s", nc.ConnectedUrlRedacted(), err)
			}
		}),
	}...)
}

// for new jetstream package
func jetstreamOpts() []jetstream.JetStreamOpt {
	opts := opts()

	res := []jetstream.JetStreamOpt{}

	if opts.Trace {
		ct := &jetstream.ClientTrace{
			RequestSent: func(subj string, payload []byte) {
				log.Printf(">>> %s\n%s\n\n", subj, string(payload))
			},
			ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
				log.Printf("<<< %s: %s", subj, string(payload))
			},
		}
		res = append(res, jetstream.WithClientTrace(ct))
	}

	return res
}

func jsOpts() []nats.JSOpt {
	opts := opts()
	jso := []nats.JSOpt{
		nats.Domain(opts.Config.JSDomain()),
		nats.APIPrefix(opts.Config.JSAPIPrefix()),
		nats.MaxWait(opts.Timeout),
	}

	if opts.Trace {
		ct := &nats.ClientTrace{
			RequestSent: func(subj string, payload []byte) {
				log.Printf(">>> %s\n%s\n\n", subj, string(payload))
			},
			ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
				log.Printf("<<< %s: %s", subj, string(payload))
			},
		}
		jso = append(jso, ct)
	}

	return jso
}

func addCheat(name string, cmd *fisk.CmdClause) {
	if opts().NoCheats {
		return
	}

	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}

func newNatsConnUnlocked(servers string, copts ...nats.Option) (*nats.Conn, error) {
	opts := options.DefaultOptions

	if opts.Conn != nil {
		return opts.Conn, nil
	}

	if opts.Config == nil {
		err := loadContext(false)
		if err != nil {
			return nil, err
		}
	}

	if servers == "" {
		servers = opts.Config.ServerURL()
	}

	var err error

	opts.Conn, err = nats.Connect(servers, copts...)

	return opts.Conn, err
}

func newNatsConn(servers string, copts ...nats.Option) (*nats.Conn, error) {
	mu.Lock()
	defer mu.Unlock()

	return newNatsConnUnlocked(servers, copts...)
}

func prepareJSHelper() (*nats.Conn, jetstream.JetStream, error) {
	mu.Lock()
	defer mu.Unlock()

	var err error
	opts := options.DefaultOptions

	jsOpts()
	if opts.Conn == nil {
		opts.Conn, _, err = prepareHelperUnlocked("", natsOpts()...)
		if err != nil {
			return nil, nil, err
		}
	}

	if opts.JSc != nil {
		return opts.Conn, opts.JSc, nil
	}

	switch {
	case opts.Config.JSDomain() != "":
		opts.JSc, err = jetstream.NewWithDomain(opts.Conn, opts.Config.JSDomain(), jetstreamOpts()...)
	case opts.Config.JSAPIPrefix() != "":
		opts.JSc, err = jetstream.NewWithAPIPrefix(opts.Conn, opts.Config.JSAPIPrefix(), jetstreamOpts()...)
	default:
		opts.JSc, err = jetstream.New(opts.Conn, jetstreamOpts()...)
	}

	if err != nil {
		return nil, nil, err
	}

	return opts.Conn, opts.JSc, nil
}

func prepareHelper(servers string, copts ...nats.Option) (*nats.Conn, *jsm.Manager, error) {
	mu.Lock()
	defer mu.Unlock()

	return prepareHelperUnlocked(servers, copts...)
}

func validator() *SchemaValidator {
	if os.Getenv("NOVALIDATE") == "" {
		return new(SchemaValidator)
	}

	if opts().Trace {
		log.Printf("!!! Disabling schema validation")
	}

	return nil
}

func jsmOpts() []jsm.Option {
	opts := opts()

	if opts.Config == nil {
		return []jsm.Option{}
	}

	jsopts, err := opts.Config.JSMOptions()
	if err != nil {
		return nil
	}

	if os.Getenv("NOVALIDATE") == "" {
		jsopts = append(jsopts, jsm.WithAPIValidation(validator()))
	}

	if opts.Timeout != 0 {
		jsopts = append(jsopts, jsm.WithTimeout(opts.Timeout))
	}

	if opts.Trace {
		jsopts = append(jsopts, jsm.WithTrace())
	}

	return jsopts
}

func prepareHelperUnlocked(servers string, copts ...nats.Option) (*nats.Conn, *jsm.Manager, error) {
	var err error

	opts := options.DefaultOptions

	if opts.Config == nil {
		err = loadContext(false)
		if err != nil {
			return nil, nil, err
		}
	}

	if opts.Conn == nil {
		opts.Conn, err = newNatsConnUnlocked(servers, copts...)
		if err != nil {
			return nil, nil, err
		}
	}

	if opts.Mgr != nil {
		return opts.Conn, opts.Mgr, nil
	}

	jsopts := jsmOpts()

	opts.Mgr, err = jsm.New(opts.Conn, jsopts...)
	if err != nil {
		return nil, nil, err
	}

	return opts.Conn, opts.Mgr, err
}

func loadContext(softFail bool) error {
	opts := options.DefaultOptions

	ctxOpts := []natscontext.Option{
		natscontext.WithServerURL(opts.Servers),
		natscontext.WithCreds(opts.Creds),
		natscontext.WithNKey(opts.Nkey),
		natscontext.WithCertificate(opts.TlsCert),
		natscontext.WithKey(opts.TlsKey),
		natscontext.WithCA(opts.TlsCA),
		natscontext.WithWindowsCertStore(opts.WinCertStoreType),
		natscontext.WithWindowsCertStoreMatch(opts.WinCertStoreMatch),
		natscontext.WithWindowsCertStoreMatchBy(opts.WinCertStoreMatchBy),
		natscontext.WithWindowsCaCertsMatch(opts.WinCertCaStoreMatch...),
		natscontext.WithSocksProxy(opts.SocksProxy),
		natscontext.WithJSEventPrefix(opts.JsEventPrefix),
		natscontext.WithJSAPIPrefix(opts.JsApiPrefix),
		natscontext.WithJSDomain(opts.JsDomain),
		natscontext.WithInboxPrefix(opts.InboxPrefix),
		natscontext.WithColorScheme(opts.ColorScheme),
	}

	if opts.TlsFirst {
		ctxOpts = append(ctxOpts, natscontext.WithTLSHandshakeFirst())
	}

	if opts.Token != "" {
		ctxOpts = append(ctxOpts, natscontext.WithToken(opts.Token))
	}

	if opts.Username != "" && opts.Password == "" {
		ctxOpts = append(ctxOpts, natscontext.WithToken(opts.Username))
	} else {
		ctxOpts = append(ctxOpts, natscontext.WithUser(opts.Username), natscontext.WithPassword(opts.Password))
	}

	var err error

	exist, _ := iu.IsFileAccessible(opts.CfgCtx)

	if exist && strings.HasSuffix(opts.CfgCtx, ".json") {
		opts.Config, err = natscontext.NewFromFile(opts.CfgCtx, ctxOpts...)
	} else {
		opts.Config, err = natscontext.New(opts.CfgCtx, !SkipContexts, ctxOpts...)
	}

	if err != nil && softFail {
		opts.Config, err = natscontext.New(opts.CfgCtx, false, ctxOpts...)
	}

	return err
}

func renderCluster(cluster *api.ClusterInfo) string {
	if cluster == nil {
		return ""
	}

	// first we figure out leader and downs based on the full names and build
	// peers array which is a list of all the full names
	leader := -1
	warn := []int{}
	var peers []string

	if cluster.Leader != "" {
		peers = append(peers, cluster.Leader)
		leader = 0
	}

	for i, r := range cluster.Replicas {
		name := r.Name
		if r.Offline || !r.Current {
			if leader == 0 {
				warn = append(warn, i+1)
			} else {
				warn = append(warn, i)
			}

		}
		peers = append(peers, name)
	}

	// now we compact that list of hostnames and apply styling * and ! to the leader and down ones
	compact := iu.CompactStrings(peers)
	if leader != -1 {
		compact[0] = compact[0] + "*"
	}
	for _, i := range warn {
		compact[i] = compact[i] + "!"
	}
	sort.Strings(compact)

	return f(compact)
}

// doReqAsync serializes and sends a request to the given subject and handles multiple responses.
// This function uses the value from `Timeout` CLI flag as upper limit for responses gathering.
// The value of the `waitFor` may shorten the interval during which responses are gathered:
//
//	waitFor < 0  : listen for responses for the full timeout interval
//	waitFor == 0 : (adaptive timeout), after each response, wait a short amount of time for more, then stop
//	waitFor > 0  : stops listening before the timeout if the given number of responses are received
func doReqAsync(req any, subj string, waitFor int, nc *nats.Conn, cb func([]byte)) error {
	jreq := []byte("{}")
	var err error

	if req != nil {
		switch val := req.(type) {
		case string:
			jreq = []byte(val)
		default:
			jreq, err = json.Marshal(req)
			if err != nil {
				return err
			}
		}
	}

	if opts().Trace {
		log.Printf(">>> %s: %s\n", subj, string(jreq))
	}

	var (
		mu       sync.Mutex
		ctr      = 0
		finisher *time.Timer
	)

	// Set deadline, max amount of time this function waits for responses
	ctx, cancel := context.WithTimeout(ctx, opts().Timeout)
	defer cancel()

	// Activate "adaptive timeout". Finisher may trigger early termination
	if waitFor == 0 {
		// First response can take up to Timeout to arrive
		finisher = time.NewTimer(opts().Timeout)
		go func() {
			select {
			case <-finisher.C:
				cancel()
			case <-ctx.Done():
				return
			}
		}()
	}

	errs := make(chan error)
	sub, err := nc.Subscribe(nc.NewRespInbox(), func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		data := m.Data
		compressed := false
		if m.Header.Get("Content-Encoding") == "snappy" {
			compressed = true
			ud, err := io.ReadAll(s2.NewReader(bytes.NewBuffer(data)))
			if err != nil {
				errs <- err
				return
			}
			data = ud
		}

		if opts().Trace {
			if compressed {
				log.Printf("<<< (%dB -> %dB) %s", len(m.Data), len(data), string(data))
			} else {
				log.Printf("<<< (%dB) %s", len(data), string(data))
			}

			if m.Header != nil {
				log.Printf("<<< Header: %+v", m.Header)
			}
		}

		// If adaptive timeout is active, set deadline for next response
		if finisher != nil {
			// Stop listening and return if no further responses arrive within this interval
			finisher.Reset(300 * time.Millisecond)
		}

		if m.Header.Get("Status") == "503" {
			errs <- nats.ErrNoResponders
			return
		}

		cb(data)
		ctr++

		// Stop listening if the requested number of responses have been received
		if waitFor > 0 && ctr == waitFor {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	if waitFor > 0 {
		sub.AutoUnsubscribe(waitFor)
	}

	msg := nats.NewMsg(subj)
	msg.Data = jreq
	if subj != "$SYS.REQ.SERVER.PING" && !strings.HasPrefix(subj, "$SYS.REQ.ACCOUNT") {
		msg.Header.Set("Accept-Encoding", "snappy")
	}
	msg.Reply = sub.Subject

	err = nc.PublishMsg(msg)
	if err != nil {
		return err
	}

	select {
	case err = <-errs:
		if err == nats.ErrNoResponders && strings.HasPrefix(subj, "$SYS") {
			return fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
		}

		return err
	case <-ctx.Done():
	}

	if opts().Trace {
		log.Printf("=== Received %d responses", ctr)
	}

	return nil
}

func doReq(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
	res := [][]byte{}
	mu := sync.Mutex{}

	err := doReqAsync(req, subj, waitFor, nc, func(r []byte) {
		mu.Lock()
		res = append(res, r)
		mu.Unlock()
	})

	return res, err
}

type raftLeader struct {
	name    string
	cluster string
	groups  int
}

func renderRaftLeaders(leaders map[string]*raftLeader, grpTitle string) {
	table := iu.NewTableWriter(opts(), "RAFT Leader Report")
	table.AddHeaders("Server", "Cluster", grpTitle, "Distribution")

	var llist []*raftLeader
	cstreams := map[string]int{}
	for _, v := range leaders {
		llist = append(llist, v)
		_, ok := cstreams[v.cluster]
		if !ok {
			cstreams[v.cluster] = 0
		}
		cstreams[v.cluster] += v.groups
	}
	sort.SliceStable(llist, func(i, j int) bool {
		if llist[i].cluster < llist[j].cluster {
			return true
		}
		if llist[i].cluster > llist[j].cluster {
			return false
		}
		return llist[i].groups < llist[j].groups
	})

	prev := ""
	for i, l := range llist {
		if i == 0 {
			prev = l.cluster
		}

		if prev != l.cluster {
			table.AddSeparator()
			prev = l.cluster
		}

		dots := int(math.Round((float64(l.groups) / float64(cstreams[l.cluster]) * 100) / 10))
		if dots <= 0 {
			dots = 1
		}
		table.AddRow(l.name, l.cluster, f(l.groups), strings.Repeat("*", dots))
	}
	fmt.Println(table.Render())
}

// io.Reader / io.Writer that updates progress bar
type progressRW struct {
	r io.Reader
	w io.Writer
	p progress.Writer
	t *progress.Tracker
}

func (pr *progressRW) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	pr.t.Increment(int64(n))

	return n, err
}

func (pr *progressRW) Write(p []byte) (n int, err error) {
	n, err = pr.w.Write(p)
	pr.t.Increment(int64(n))
	return n, err
}

func outPutMSGBodyCompact(data []byte, filter string, subject string, stream string) (string, error) {
	if len(data) == 0 && filter == "" {
		fmt.Println("nil body")
		return "", nil
	}

	data, err := filterDataThroughCmd(data, filter, subject, stream)
	if err != nil {
		// using q here so raw binary data will be escaped
		fmt.Printf("%q\nError while translating msg body: %s\n\n", data, err.Error())
		return "", err
	}
	output := string(data)
	if strings.HasSuffix(output, "\n") {
		fmt.Print(output)
	} else {
		fmt.Println(output)
	}

	return output, nil
}

func outPutMSGBody(data []byte, filter string, subject string, stream string) {
	output, err := outPutMSGBodyCompact(data, filter, subject, stream)
	if err != nil {
		return
	}

	fmt.Println()

	if !strings.HasSuffix(output, "\n") {
		fmt.Println()
	}
}

func filterDataThroughCmd(data []byte, filter, subject, stream string) ([]byte, error) {
	if filter == "" {
		return data, nil
	}
	funcMap := template.FuncMap{
		"Subject": func() string { return subject },
		"Stream":  func() string { return stream },
	}

	tmpl, err := template.New("translate").Funcs(funcMap).Parse(filter)
	if err != nil {
		return nil, err
	}
	var builder strings.Builder
	err = tmpl.Execute(&builder, nil)
	if err != nil {
		return nil, err
	}

	parts, err := shlex.Split(builder.String())
	if err != nil {
		return nil, fmt.Errorf("the filter command line could not be parsed: %w", err)
	}
	cmd := parts[0]
	args := parts[1:]

	runner := exec.Command(cmd, args...)
	// pass the message as string to stdin
	runner.Stdin = bytes.NewReader(data)
	// maybe we want to do something on error?
	return runner.CombinedOutput()
}

// currentActiveServers determines how many servers the connected server knows about
func currentActiveServers(nc *nats.Conn) (int, error) {
	var expect int

	err := doReqAsync(nil, "$SYS.REQ.SERVER.PING", 1, nc, func(msg []byte) {
		var res server.ServerStatsMsg

		err := json.Unmarshal(msg, &res)
		if err != nil {
			return
		}

		expect = res.Stats.ActiveServers
	})

	return expect, err
}

func calculateRate(new, last float64, since time.Duration) float64 {
	// If new == 0 we have missed a data point from nats.
	// Return the previous calculation so that it doesn't break graphs
	if new == 0 {
		return last
	}

	return (new - last) / since.Seconds()
}

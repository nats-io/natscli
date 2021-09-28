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
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/textproto"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/xlab/tablewriter"
	terminal "golang.org/x/term"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go"

	"github.com/nats-io/jsm.go/natscontext"
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

	consumers, err := mgr.ConsumerNames(stream)
	if err != nil {
		return "", nil, err
	}

	switch len(consumers) {
	case 0:
		return "", nil, fmt.Errorf("no Consumers are defined for Stream %s", stream)
	default:
		c := ""

		err = survey.AskOne(&survey.Select{
			Message:  "Select a Consumer",
			Options:  consumers,
			PageSize: selectPageSize(len(consumers)),
		}, &c)
		if err != nil {
			return "", nil, err
		}

		return c, nil, nil
	}
}

func selectStreamTemplate(mgr *jsm.Manager, template string, force bool) (string, error) {
	if template != "" {
		known, err := mgr.IsKnownStreamTemplate(template)
		if err != nil {
			return "", err
		}

		if known {
			return template, nil
		}
	}

	if force {
		return "", fmt.Errorf("unknown template %q", template)
	}

	templates, err := mgr.StreamTemplateNames()
	if err != nil {
		return "", err
	}

	switch len(templates) {
	case 0:
		return "", errors.New("no Streams Templates are defined")
	default:
		s := ""

		err = survey.AskOne(&survey.Select{
			Message:  "Select a Stream Template",
			Options:  templates,
			PageSize: selectPageSize(len(templates)),
		}, &s)
		if err != nil {
			return "", err
		}

		return s, nil
	}
}

func selectStream(mgr *jsm.Manager, stream string, force bool) (string, *jsm.Stream, error) {
	s, err := mgr.LoadStream(stream)
	if err == nil {
		return s.Name(), s, nil
	}

	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return "", nil, err
	}

	known := false
	for _, s := range streams {
		if s == stream {
			known = true
			break
		}
	}

	if known {
		return stream, nil, nil
	}

	if force {
		return "", nil, fmt.Errorf("unknown stream %q", stream)
	}

	switch len(streams) {
	case 0:
		return "", nil, errors.New("no Streams are defined")
	default:
		s := ""

		err = survey.AskOne(&survey.Select{
			Message:  "Select a Stream",
			Options:  streams,
			PageSize: selectPageSize(len(streams)),
		}, &s)
		if err != nil {
			return "", nil, err
		}

		return s, nil, nil
	}
}

func printJSON(d interface{}) error {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}
func parseDurationString(dstr string) (dur time.Duration, err error) {
	dstr = strings.TrimSpace(dstr)

	if len(dstr) <= 0 {
		return dur, nil
	}

	ls := len(dstr)
	di := ls - 1
	unit := dstr[di:]

	switch unit {
	case "w", "W":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*7*24) * time.Hour

	case "d", "D":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24) * time.Hour
	case "M":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*30) * time.Hour
	case "Y", "y":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*365) * time.Hour
	case "s", "S", "m", "h", "H":
		dur, err = time.ParseDuration(dstr)
		if err != nil {
			return dur, err
		}

	default:
		return dur, fmt.Errorf("invalid time unit %s", unit)
	}

	return dur, nil
}

// calculates progress bar width for uiprogress:
//
// if it cant figure out the width, assume 80
// if the width is too small, set it to minWidth and just live with the overflow
//
// this ensures a reasonable progress size, ideally we should switch over
// to a spinner for < minWidth rather than cause overflows, but thats for later.
func progressWidth() int {
	w, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80
	}

	minWidth := 10

	if w-30 <= minWidth {
		return minWidth
	} else {
		return w - 30
	}
}

func selectPageSize(count int) int {
	_, h, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		h = 40
	}

	ps := count
	if ps > h-4 {
		ps = h - 4
	}

	return ps
}

func isTerminal() bool {
	return terminal.IsTerminal(int(os.Stdin.Fd()))
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	if !isTerminal() {
		return false, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	ans := dflt

	err := survey.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}

func askOneBytes(prompt string, dflt string, help string) (int64, error) {
	if !isTerminal() {
		return 0, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	val := ""
	err := survey.AskOne(&survey.Input{
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

	i, err := humanize.ParseBytes(val)
	if err != nil {
		return 0, err
	}

	return int64(i), nil
}

func askOneInt(prompt string, dflt string, help string) (int64, error) {
	if !isTerminal() {
		return 0, fmt.Errorf("cannot ask for confirmation without a terminal")
	}

	val := ""
	err := survey.AskOne(&survey.Input{
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

func splitString(s string) []string {
	return strings.FieldsFunc(s, func(c rune) bool {
		if unicode.IsSpace(c) {
			return true
		}

		if c == ',' {
			return true
		}

		return false
	})
}

func natsOpts() []nats.Option {
	if config == nil {
		return []nats.Option{}
	}

	opts, err := config.NATSOptions()
	kingpin.FatalIfError(err, "configuration error")

	return append(opts, []nats.Option{
		nats.Name("NATS CLI Version " + version),
		nats.MaxReconnects(-1),
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
				log.Printf("Unexpected NATS error from server %s: %s", url, err)
			}
		}),
	}...)
}

func newNatsConn(servers string, opts ...nats.Option) (*nats.Conn, error) {
	if config == nil {
		if ctxError != nil {
			return nil, ctxError
		}

		err := loadContext()
		if err != nil {
			return nil, err
		}
	}

	if servers == "" {
		servers = config.ServerURL()
	}

	return nats.Connect(servers, opts...)
}

func prepareHelper(servers string, opts ...nats.Option) (*nats.Conn, *jsm.Manager, error) {
	if config == nil {
		if ctxError != nil {
			return nil, nil, ctxError
		}

		err := loadContext()
		if err != nil {
			return nil, nil, err
		}
	}

	nc, err := newNatsConn(servers, opts...)
	if err != nil {
		return nil, nil, err
	}

	jsopts := []jsm.Option{
		jsm.WithAPIPrefix(config.JSAPIPrefix()),
		jsm.WithEventPrefix(config.JSEventPrefix()),
		jsm.WithDomain(config.JSDomain()),
	}

	if os.Getenv("NOVALIDATE") == "" {
		jsopts = append(jsopts, jsm.WithAPIValidation(new(SchemaValidator)))
	}

	if timeout != 0 {
		jsopts = append(jsopts, jsm.WithTimeout(timeout))
	}

	if trace {
		jsopts = append(jsopts, jsm.WithTrace())
	}

	mgr, err := jsm.New(nc, jsopts...)
	if err != nil {
		return nil, nil, err
	}

	return nc, mgr, err
}

func humanizeDuration(d time.Duration) string {
	if d == math.MaxInt64 {
		return "never"
	}

	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}

	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}

	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}

	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}

	return fmt.Sprintf("%.2fs", d.Seconds())
}

func humanizeTime(t time.Time) string {
	return humanizeDuration(time.Since(t))
}

const (
	hdrLine   = "NATS/1.0\r\n"
	crlf      = "\r\n"
	hdrPreEnd = len(hdrLine) - len(crlf)
	statusLen = 3
	statusHdr = "Status"
	descrHdr  = "Description"
)

// copied from nats.go
func decodeHeadersMsg(data []byte) (nats.Header, error) {
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(data)))
	l, err := tp.ReadLine()
	if err != nil || len(l) < hdrPreEnd || l[:hdrPreEnd] != hdrLine[:hdrPreEnd] {
		return nil, nats.ErrBadHeaderMsg
	}

	mh, err := readMIMEHeader(tp)
	if err != nil {
		return nil, err
	}

	// Check if we have an inlined status.
	if len(l) > hdrPreEnd {
		var description string
		status := strings.TrimSpace(l[hdrPreEnd:])
		if len(status) != statusLen {
			description = strings.TrimSpace(status[statusLen:])
			status = status[:statusLen]
		}
		mh.Add(statusHdr, status)
		if len(description) > 0 {
			mh.Add(descrHdr, description)
		}
	}
	return nats.Header(mh), nil
}

// copied from nats.go
func readMIMEHeader(tp *textproto.Reader) (textproto.MIMEHeader, error) {
	m := make(textproto.MIMEHeader)
	for {
		kv, err := tp.ReadLine()
		if len(kv) == 0 {
			return m, err
		}

		// Process key fetching original case.
		i := bytes.IndexByte([]byte(kv), ':')
		if i < 0 {
			return nil, nats.ErrBadHeaderMsg
		}
		key := kv[:i]
		if key == "" {
			// Skip empty keys.
			continue
		}
		i++
		for i < len(kv) && (kv[i] == ' ' || kv[i] == '\t') {
			i++
		}
		value := string(kv[i:])
		m[key] = append(m[key], value)
		if err != nil {
			return m, err
		}
	}
}

type pubData struct {
	Cnt       int
	Count     int
	Unix      int64
	UnixNano  int64
	TimeStamp string
	Time      string
}

func (p *pubData) ID() string {
	return nuid.Next()
}

func pubReplyBodyTemplate(body string, ctr int) ([]byte, error) {
	now := time.Now()
	funcMap := template.FuncMap{
		"Random":    randomString,
		"Count":     func() int { return ctr },
		"Cnt":       func() int { return ctr },
		"Unix":      func() int64 { return now.Unix() },
		"UnixNano":  func() int64 { return now.UnixNano() },
		"TimeStamp": func() string { return now.Format(time.RFC3339) },
		"Time":      func() string { return now.Format(time.Kitchen) },
		"ID":        func() string { return nuid.Next() },
	}

	templ, err := template.New("body").Funcs(funcMap).Parse(body)
	if err != nil {
		return []byte(body), err
	}

	var b bytes.Buffer
	err = templ.Execute(&b, &pubData{
		Cnt:       ctr,
		Count:     ctr,
		Unix:      now.Unix(),
		UnixNano:  now.UnixNano(),
		TimeStamp: now.Format(time.RFC3339),
		Time:      now.Format(time.Kitchen),
	})
	if err != nil {
		return []byte(body), err
	}

	return b.Bytes(), nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var passwordRunes = append(letterRunes, []rune("@#_-%^&()")...)

func randomPassword(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = passwordRunes[rand.Intn(len(passwordRunes))]
	}

	return string(b)
}

func randomString(shortest uint, longest uint) string {
	if shortest > longest {
		shortest, longest = longest, shortest
	}

	var desired int

	switch {
	case int(longest)-int(shortest) < 0:
		desired = int(shortest) + rand.Intn(int(longest))
	case longest == shortest:
		desired = int(shortest)
	default:
		desired = int(shortest) + rand.Intn(int(longest-shortest))
	}

	b := make([]rune, desired)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return string(b)
}

func parseStringsToHeader(hdrs []string, seq int, msg *nats.Msg) error {
	for _, hdr := range hdrs {
		parts := strings.SplitN(hdr, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid header %q", hdr)
		}

		val, err := pubReplyBodyTemplate(strings.TrimSpace(parts[1]), seq)
		if err != nil {
			log.Printf("Failed to parse Header template for %s: %s", parts[0], err)
			continue
		}

		msg.Header.Add(strings.TrimSpace(parts[0]), string(val))
	}

	return nil
}

func loadContext() error {
	opts := []natscontext.Option{
		natscontext.WithServerURL(servers),
		natscontext.WithCreds(creds),
		natscontext.WithNKey(nkey),
		natscontext.WithCertificate(tlsCert),
		natscontext.WithKey(tlsKey),
		natscontext.WithCA(tlsCA),
		natscontext.WithJSEventPrefix(jsEventPrefix),
		natscontext.WithJSAPIPrefix(jsApiPrefix),
		natscontext.WithJSDomain(jsDomain),
	}

	if username != "" && password == "" {
		opts = append(opts, natscontext.WithToken(username))
	} else {
		opts = append(opts, natscontext.WithUser(username), natscontext.WithPassword(password))
	}

	config, ctxError = natscontext.New(cfgCtx, !skipContexts, opts...)

	return ctxError
}

func prepareConfig(_ *kingpin.ParseContext) (err error) {
	loadContext()

	rand.Seed(time.Now().UnixNano())

	return nil
}

func fileAccessible(f string) (bool, error) {
	stat, err := os.Stat(f)
	if err != nil {
		return false, err
	}

	if stat.IsDir() {
		return false, fmt.Errorf("is a directory")
	}

	file, err := os.Open(f)
	if err != nil {
		return false, err
	}
	file.Close()

	return true, nil
}

func isJsonString(s string) bool {
	trimmed := strings.TrimSpace(s)
	return strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")
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
	compact := compactStrings(peers)
	if leader != -1 {
		compact[0] = compact[0] + "*"
	}
	for _, i := range warn {
		compact[i] = compact[i] + "!"
	}
	sort.Strings(compact)

	return strings.Join(compact, ", ")
}

func doReqAsync(req interface{}, subj string, waitFor int, nc *nats.Conn, cb func([]byte)) error {
	jreq := []byte("{}")
	var err error

	if req != nil {
		jreq, err = json.MarshalIndent(req, "", "  ")
		if err != nil {
			return err
		}
	}

	if trace {
		log.Printf(">>> %s: %s\n", subj, string(jreq))
	}

	var (
		mu  sync.Mutex
		ctr = 0
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var finisher *time.Timer
	if waitFor == 0 {
		finisher = time.NewTimer(300 * time.Millisecond)
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
	sub, err := nc.Subscribe(nats.NewInbox(), func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		data := m.Data
		compressed := false
		if m.Header.Get("Content-Encoding") == "snappy" {
			compressed = true
			ud, err := ioutil.ReadAll(s2.NewReader(bytes.NewBuffer(data)))
			if err != nil {
				errs <- err
				return
			}
			data = ud
		}

		if trace {
			if compressed {
				log.Printf("<<< (%dB -> %dB) %s", len(m.Data), len(data), string(data))
			} else {
				log.Printf("<<< (%dB) %s", len(data), string(data))
			}

			if m.Header != nil {
				log.Printf("<<< Header: %+v", m.Header)
			}
		}

		if finisher != nil {
			finisher.Reset(300 * time.Millisecond)
		}

		if m.Header.Get("Status") == "503" {
			errs <- nats.ErrNoResponders
			return
		}

		cb(data)
		ctr++

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
	if subj != "$SYS.REQ.SERVER.PING" {
		msg.Header.Set("Accept-Encoding", "snappy")
	}
	msg.Reply = sub.Subject

	err = nc.PublishMsg(msg)
	if err != nil {
		return err
	}

	select {
	case err = <-errs:
		if err == nats.ErrNoResponders {
			return fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
		}
		return err
	case <-ctx.Done():
	}

	if trace {
		log.Printf(">>> Received %d responses", ctr)
	}

	return err
}
func doReq(req interface{}, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
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
	table := tablewriter.CreateTable()
	table.AddTitle("RAFT Leader Report")
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
		table.AddRow(l.name, l.cluster, humanize.Comma(int64(l.groups)), strings.Repeat("*", dots))
	}
	fmt.Println(table.Render())
}

func compactStrings(source []string) []string {
	if len(source) == 0 {
		return source
	}

	hnParts := make([][]string, len(source))
	shortest := math.MaxInt8

	for i, name := range source {
		hnParts[i] = strings.Split(name, ".")
		if len(hnParts[i]) < shortest {
			shortest = len(hnParts[i])
		}
	}

	toRemove := ""

	// we dont chop the 0 item off
	for i := shortest - 1; i > 0; i-- {
		s := hnParts[0][i]

		remove := true
		for _, name := range hnParts {
			if name[i] != s {
				remove = false
				break
			}
		}

		if remove {
			toRemove = "." + s + toRemove
		} else {
			break
		}
	}

	result := make([]string, len(source))
	for i, name := range source {
		result[i] = strings.TrimSuffix(name, toRemove)
	}

	return result
}

func newTableWriter(title string) *tablewriter.Table {
	table := tablewriter.CreateTable()
	table.UTF8Box()
	if title != "" {
		table.AddTitle(title)
	}

	return table
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func base64IfNotPrintable(val []byte) string {
	if isPrintable(string(val)) {
		return string(val)
	}

	return base64.StdEncoding.EncodeToString(val)
}

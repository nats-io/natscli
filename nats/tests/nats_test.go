// Copyright 2019-2025 The NATS Authors
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
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kballard/go-shellquote"

	"github.com/nats-io/natscli/cli"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var (
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func init() {
	cli.SkipContexts = true
}

func checkErr(t *testing.T, err error, format string, a ...any) {
	t.Helper()
	if err == nil {
		return
	}

	t.Fatalf(format, a...)
}

func runNatsCli(t *testing.T, args ...string) (output []byte) {
	t.Helper()
	return runNatsCliWithInput(t, "", args...)
}

func runNatsCliWithInput(t *testing.T, input string, args ...string) (output []byte) {
	t.Helper()

	var runArgs []string
	var cmd string
	var err error
	if os.Getenv("CI") == "true" {
		cmd = "../nats"
		runArgs, err = shellquote.Split(strings.Join(args, " "))
	} else {
		cmd = "go"
		runArgs, err = shellquote.Split(fmt.Sprintf("run ../main.go %s", strings.Join(args, " ")))
	}
	if err != nil {
		t.Fatalf("spliting command argument string failed: %v", err)
	}

	if _, err := exec.LookPath(cmd); err != nil {
		t.Fatalf("could not find %s in path", cmd)
		return
	}

	out, err := runCommand(cmd, input, runArgs...)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return out
}

func prepareHelper(servers string) (*nats.Conn, *jsm.Manager, error) {
	nc, err := nats.Connect(servers)
	if err != nil {
		return nil, nil, err
	}

	mgr, err := jsm.New(nc)
	if err != nil {
		return nil, nil, err
	}

	return nc, mgr, nil
}

func setupJStreamTest(t *testing.T) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
	t.Helper()

	dir, err := os.MkdirTemp("", "")
	checkErr(t, err, "could not create temporary js store: %v", err)
	sysAcc := server.NewAccount("SYS")

	srv, err = server.NewServer(&server.Options{
		Port:          -1,
		ServerName:    "TEST_SERVER",
		StoreDir:      dir,
		JetStream:     true,
		SystemAccount: "SYS",
		Accounts: []*server.Account{
			sysAcc,
		},
		Users: []*server.User{
			{
				Username: "sys",
				Password: "pass",
				Account:  sysAcc,
			},
		},
	})

	checkErr(t, err, "could not start js server: %v", err)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Errorf("nats server did not start")
	}

	nc, mgr, err = prepareHelper(srv.ClientURL())
	checkErr(t, err, "could not connect client to server @ %s: %v", srv.ClientURL(), err)

	streams, err := mgr.StreamNames(nil)
	checkErr(t, err, "could not load streams: %v", err)
	if len(streams) != 0 {
		t.Fatalf("found %v message streams but it should be empty", streams)
	}

	return srv, nc, mgr
}
func withJSCluster(t *testing.T, cb func(*testing.T, []*server.Server, *nats.Conn, *jsm.Manager) error) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows due to GitHub Actions resource constraints")
	}

	d, err := os.MkdirTemp("", "jstest")
	if err != nil {
		t.Fatalf("temp dir could not be made: %s", err)
	}
	defer os.RemoveAll(d)

	var (
		servers []*server.Server
	)

	sysAcc := server.NewAccount("SYS")

	for i := 1; i <= 3; i++ {
		opts := &server.Options{
			JetStream:  true,
			StoreDir:   filepath.Join(d, fmt.Sprintf("s%d", i)),
			Port:       -1,
			Host:       "localhost",
			ServerName: fmt.Sprintf("s%d", i),
			LogFile:    filepath.Join(d, fmt.Sprintf("s%d.log", i)),
			Cluster: server.ClusterOpts{
				Name: "TEST",
				Port: 12000 + i,
			},
			Routes: []*url.URL{
				{Host: "localhost:12001"},
				{Host: "localhost:12002"},
				{Host: "localhost:12003"},
			},
			SystemAccount: "SYS",
			Accounts: []*server.Account{
				sysAcc,
			},
			Users: []*server.User{
				{
					Username: "sys",
					Password: "pass",
					Account:  sysAcc,
				},
			},
		}

		s, err := server.NewServer(opts)
		if err != nil {
			t.Fatalf("server %d start failed: %v", i, err)
		}
		s.ConfigureLogger()
		go s.Start()
		if !s.ReadyForConnections(10 * time.Second) {
			t.Errorf("nats server %d did not start", i)
		}
		defer func() {
			s.Shutdown()
		}()

		servers = append(servers, s)
	}

	if len(servers) != 3 {
		t.Fatalf("servers did not start")
	}

	nc, err := nats.Connect(servers[0].ClientURL(), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("client start failed: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc, jsm.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("manager creation failed: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := mgr.JetStreamAccountInfo()
			if err != nil {
				continue
			}

			err = cb(t, servers, nc, mgr)

			if err != nil {
				t.Fatal(err)
			}

			return
		case <-ctx.Done():
			t.Fatalf("jetstream did not become available")
		}
	}
}

func setupConsTest(t *testing.T) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
	srv, nc, mgr = setupJStreamTest(t)

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	return srv, nc, mgr
}

func streamShouldExist(t *testing.T, mgr *jsm.Manager, stream string) {
	t.Helper()
	known, err := mgr.IsKnownStream(stream)
	checkErr(t, err, "stream lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", stream)
	}
}

func streamShouldNotExist(t *testing.T, mgr *jsm.Manager, stream string) {
	t.Helper()
	known, err := mgr.IsKnownStream(stream)
	checkErr(t, err, "stream lookup failed: %v", err)
	if known {
		t.Fatalf("unexpectedly found %s already existing", stream)
	}
}

func consumerShouldExist(t *testing.T, mgr *jsm.Manager, stream string, consumer string) {
	t.Helper()
	known, err := mgr.IsKnownConsumer(stream, consumer)
	checkErr(t, err, "consumer lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", consumer)
	}
}

func streamInfo(t *testing.T, mgr *jsm.Manager, stream string) *api.StreamInfo {
	t.Helper()
	str, err := mgr.LoadStream(stream)
	checkErr(t, err, "could not load stream %s", stream)
	info, err := str.Information()
	checkErr(t, err, "could not load stream %s", stream)

	return info
}

func mem1Stream() api.StreamConfig {
	return api.StreamConfig{
		Name:      "mem1",
		Subjects:  []string{"js.mem.>"},
		Storage:   api.MemoryStorage,
		Retention: api.LimitsPolicy,
		Replicas:  1,
	}
}

func file1Stream() api.StreamConfig {
	return api.StreamConfig{
		Name:      "file1",
		Subjects:  []string{"js.file.>"},
		Storage:   api.FileStorage,
		Retention: api.LimitsPolicy,
		Replicas:  1,
	}
}

func pull1Cons() api.ConsumerConfig {
	return api.ConsumerConfig{
		DeliverSubject: nats.NewInbox(),
		Durable:        "pull1",
		DeliverPolicy:  api.DeliverAll,
		AckPolicy:      api.AckExplicit,
		ReplayPolicy:   api.ReplayOriginal,
		MaxAckPending:  1000,
	}
}

func push1Cons() api.ConsumerConfig {
	return api.ConsumerConfig{
		Durable:       "push1",
		DeliverPolicy: api.DeliverAll,
		AckPolicy:     api.AckExplicit,
		ReplayPolicy:  api.ReplayOriginal,
		MaxAckPending: 1000,
	}
}

func TestCLIStreamCreate(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' str create mem1 --subjects 'js.mem.>,js.other' --storage m --max-msgs-per-subject=10 --max-msgs=-1 --max-age=-1 --max-bytes=-1 --ack --retention limits --max-msg-size=1024 --discard new --dupe-window 1h --replicas 1 --description 'test suite' --allow-rollup --deny-delete --no-deny-purge --allow-direct --allow-msg-ttl", srv.ClientURL()))
	streamShouldExist(t, mgr, "mem1")
	info := streamInfo(t, mgr, "mem1")

	if info.Config.Description != "test suite" {
		t.Fatalf("invalid descroption %q", info.Config.Description)
	}

	if len(info.Config.Subjects) != 2 {
		t.Fatalf("expected 2 subjects in the message stream, got %v", info.Config.Subjects)
	}

	if info.Config.Subjects[0] != "js.mem.>" && info.Config.Subjects[1] != "js.other" {
		t.Fatalf("expects [js.mem.>, js.other] got %v", info.Config.Subjects)
	}

	if info.Config.Retention != api.LimitsPolicy {
		t.Fatalf("incorrect retention policy, expected limits got %s", info.Config.Retention)
	}

	if info.Config.Storage != api.MemoryStorage {
		t.Fatalf("incorrect storage received, expected memory got %s", info.Config.Storage)
	}

	if info.Config.MaxMsgSize != 1024 {
		t.Fatalf("incorrect max message size stream, expected 1024 got %v", info.Config.MaxMsgSize)
	}

	if info.Config.Discard != api.DiscardNew {
		t.Fatalf("incorrect discard policy %q", info.Config.Discard)
	}

	if info.Config.Duplicates != time.Hour {
		t.Fatalf("expected duplicate window of 1 hour got %v", info.Config.Duplicates)
	}

	if info.Config.MaxMsgsPer != 10 {
		t.Fatalf("expected max messages per subject to be 10 got %d", info.Config.MaxMsgsPer)
	}

	if !info.Config.RollupAllowed {
		t.Fatalf("expected rollups to be allowed")
	}

	if info.Config.DenyPurge {
		t.Fatalf("expected purge to be allowed")
	}

	if !info.Config.DenyDelete {
		t.Fatalf("expected delete to be denied")
	}

	if !info.Config.AllowDirect {
		t.Fatalf("expected direct access to be enabled")
	}

	if !info.Config.AllowMsgTTL {
		t.Fatalf("expected msg-ttl to be allowed")
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' str create ORDERS --config testdata/ORDERS_config.json", srv.ClientURL()))
	streamShouldExist(t, mgr, "ORDERS")
	info = streamInfo(t, mgr, "ORDERS")

	if len(info.Config.Subjects) != 1 {
		t.Fatalf("expected 1 subject in the message stream, got %v", info.Config.Subjects)
	}

	if info.Config.Subjects[0] != "ORDERS.*" {
		t.Fatalf("expected [ORDERS.*], got %v", info.Config.Subjects)
	}

	if info.Config.Storage != api.FileStorage {
		t.Fatalf("expected file storage got %q", info.Config.Storage)
	}

	if info.Config.Duplicates != time.Hour {
		t.Fatalf("expected duplicate window of 1 hour got %v", info.Config.Duplicates)
	}

	if info.Config.RollupAllowed {
		t.Fatalf("expected rollups to be denied")
	}

	if info.Config.DenyPurge {
		t.Fatalf("expected purge to be allowed")
	}

	if info.Config.DenyDelete {
		t.Fatalf("expected delete to be allowed")
	}

	if !info.Config.AllowMsgTTL {
		t.Fatalf("expected msg-ttl to be allowed")
	}
}

func TestCLIStreamInfo(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' str info mem1 -j", srv.ClientURL()))

	var info server.StreamInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse cli output: %v", err)

	if info.Config.Name != "mem1" {
		t.Fatalf("expected info for mem1, got %s", info.Config.Name)

	}
}

func TestCLIStreamDelete(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create message stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str rm mem1 -f", srv.ClientURL()))
	streamShouldNotExist(t, mgr, "mem1")
}

func TestCLIStreamLs(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	for i := 1; i <= 2500; i++ {
		cfg := mem1Stream()
		cfg.Subjects = []string{}
		cfg.Name = fmt.Sprintf("mem_%d", i)
		_, err := mgr.NewStreamFromDefault(cfg.Name, cfg)
		checkErr(t, err, "could not create stream: %v", err)
		streamShouldExist(t, mgr, cfg.Name)
	}

	out := runNatsCli(t, fmt.Sprintf("--server='%s' str ls -j", srv.ClientURL()))

	list := []string{}
	err := json.Unmarshal(out, &list)
	checkErr(t, err, "could not parse cli output: %v", err)

	if len(list) != 2500 {
		t.Fatalf("expected 2500 ms got %d: %v", len(list), list)
	}

	if list[0] != "mem_1" && list[len(list)-1] != "mem_999" {
		t.Fatalf("invalid sorted list %v", list)
	}
}

func TestCLIStreamPurge(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	for i := 0; i < 10; i++ {
		_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
		checkErr(t, err, "could not publish message: %v", err)
	}

	checkMsgs := func(t *testing.T, c uint64) {
		t.Helper()
		i, err := stream.Information()
		checkErr(t, err, "could not get message stream info: %v", err)
		if i.State.Msgs != c {
			t.Fatalf("expected %d message(s) but got %d", c, i.State.Msgs)
		}
	}

	checkMsgs(t, 10)

	runNatsCli(t, fmt.Sprintf("--server='%s' str purge mem1 -f --subject js.mem.2", srv.ClientURL()))
	checkMsgs(t, 10)

	runNatsCli(t, fmt.Sprintf("--server='%s' str purge mem1 -f --subject js.mem.1 --seq 2", srv.ClientURL()))
	checkMsgs(t, 9)

	runNatsCli(t, fmt.Sprintf("--server='%s' str purge mem1 -f --subject js.mem.1 --keep 2", srv.ClientURL()))
	checkMsgs(t, 2)

	runNatsCli(t, fmt.Sprintf("--server='%s' str purge mem1 -f --subject js.mem.1", srv.ClientURL()))
	checkMsgs(t, 0)
}

func TestCLIStreamSeal(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	for i := 0; i < 10; i++ {
		_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
		checkErr(t, err, "could not publish message: %v", err)
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' str seal mem1 -f", srv.ClientURL()))
	checkErr(t, stream.Reset(), "reset failed")
	if !stream.Sealed() {
		t.Fatalf("stream was not sealed")
	}
}

func TestCLIStreamGet(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	item, err := stream.ReadMessage(1)
	checkErr(t, err, "could not get message: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}

	out := runNatsCli(t, fmt.Sprintf("--server='%s' str get mem1 1 -j", srv.ClientURL()))
	err = json.Unmarshal(out, &item)
	checkErr(t, err, "could not parse output: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}
}

func TestCLIStreamBackupAndRestore(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "file1")

	for i := 0; i < 1000; i++ {
		nc.Publish("js.file.1", []byte(RandomString(5480)))
	}

	td, err := os.MkdirTemp("", "")
	checkErr(t, err, "temp dir failed")
	os.RemoveAll(td)

	runNatsCli(t, fmt.Sprintf("--server='%s' str backup file1 '%s' --no-progress", srv.ClientURL(), td))

	preState, err := stream.State()
	checkErr(t, err, "state failed")
	stream.Delete()

	runNatsCli(t, fmt.Sprintf("--server='%s' str restore  '%s' --no-progress", srv.ClientURL(), td))
	stream, err = mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)

	postState, err := stream.State()
	checkErr(t, err, "state failed")
	if !reflect.DeepEqual(preState, postState) {
		t.Fatalf("restored state differed")
	}

	if postState.Msgs != 1000 {
		t.Fatalf("Expected 1000 messages got %d", postState.Msgs)
	}
}

func RandomString(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rng.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestCLIConsumerInfo(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "pull1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con info mem1 pull1 -j", srv.ClientURL()))
	var info server.ConsumerInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if info.Config.Durable != "pull1" {
		t.Fatalf("did not find into for pull1 in cli output: %v", string(out))
	}
}

func TestCLIConsumerEdit(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "pull1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con edit mem1 pull1 --description pull_test -f", srv.ClientURL()))

	c, err := mgr.LoadConsumer("mem1", "pull1")
	checkErr(t, err, "load failed")

	if c.Description() != "pull_test" {
		t.Fatalf("expected description to be pull_test got: %q", c.Description())
	}
}

func TestCLIConsumerLs(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "pull1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con ls mem1 -j", srv.ClientURL()))
	var info []string
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if len(info) != 1 {
		t.Fatalf("expected 1 item in output received %d", len(info))
	}

	if info[0] != "pull1" {
		t.Fatalf("did not find into for pull1 in cli output: %v", string(out))
	}
}

func TestCLIConsumerDelete(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "pull1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con rm mem1 pull1 -f", srv.ClientURL()))

	list, err := mgr.ConsumerNames("mem1")
	checkErr(t, err, "could not check consumer: %v", err)
	if len(list) != 0 {
		t.Fatalf("Expected no consumer, got %v", list)
	}
}

func TestCLIConsumerAdd(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 push1 --max-pending 10 --replay instant --deliver all --target out.push1 --ack explicit --filter '' --deliver-group '' --max-deliver 20 --bps 1024 --heartbeat=1s --flow-control --description 'test suite' --no-headers-only --backoff linear", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "push1")
	push1, err := mgr.LoadConsumer("mem1", "push1")
	checkErr(t, err, "push1 could not be loaded")

	if push1.Description() != "test suite" {
		t.Fatalf("invalid description %q", push1.Description())
	}

	if push1.RateLimit() != 1024 {
		t.Fatalf("Expected rate limit of 1024 but got %v", push1.RateLimit())
	}
	if push1.MaxDeliver() != 20 {
		t.Fatalf("Expected max delivery of 20 but got %v", push1.MaxDeliver())
	}
	if push1.DeliverySubject() != "out.push1" {
		t.Fatalf("Expected delivery target out.push1 but got %v", push1.DeliverySubject())
	}
	if push1.MaxAckPending() != 10 {
		t.Fatalf("Expected max ack pending 10 but got %v", push1.MaxAckPending())
	}
	if !push1.FlowControl() {
		t.Fatalf("Expected flow control, got false")
	}
	if push1.IsHeadersOnly() {
		t.Fatalf("Expected headers only to be off")
	}
	if len(push1.Backoff()) != 10 {
		t.Fatalf("Backoff policy was not set")
	}

	push1.Delete()

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 pull1 --config testdata/mem1_pull1_consumer.json", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "pull1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 push1 --filter 'js.mem.>' --max-pending 10 --replay instant --deliver subject --target out.push1 --ack explicit --deliver-group BOB --max-deliver=-1 --bps 1024 --heartbeat=1s --flow-control --description 'test suite' --no-headers-only --backoff none", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "push1")
	push1, err = mgr.LoadConsumer("mem1", "push1")
	checkErr(t, err, "push1 could not be loaded")
	if push1.DeliverPolicy() != api.DeliverLastPerSubject {
		t.Fatalf("expected subject delivery policy got %v", push1.DeliverPolicy())
	}
	if push1.DeliverGroup() != "BOB" {
		t.Fatalf("deliver group '%s' != 'BOB'", push1.DeliverGroup())
	}
	if push1.MaxDeliver() != -1 {
		t.Fatalf("max_deliver %d is not -1", push1.MaxDeliver())
	}
	if len(push1.Backoff()) != 0 {
		t.Fatalf("unexpected backoff policy: %v", push1.Backoff())
	}
}

func TestCLIConsumerNext(t *testing.T) {
	srv, nc, mgr := setupConsTest(t)
	defer srv.Shutdown()

	push1, err := mgr.NewConsumerFromDefault("mem1", push1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "push1")

	push1.Reset()
	if !push1.IsPullMode() {
		t.Fatalf("push1 is not push mode: %#v", push1.Configuration())
	}

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con next mem1 push1 --raw", nc.ConnectedUrl()))

	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("did not receive 'hello', got: '%s'", string(out))
	}
}

func TestCLIStreamAddDefaults(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' str add file1 --subjects other --defaults", srv.ClientURL()))
	streamShouldExist(t, mgr, "file1")

	stream, err := mgr.LoadStream("file1")
	checkErr(t, err, "could not get stream: %v", err)

	if stream.DiscardPolicy() != api.DiscardOld {
		t.Fatalf("expected old policy")
	}
	if stream.Retention() != api.LimitsPolicy {
		t.Fatalf("expected limits retention")
	}
}

func TestCliConsumerAddDefaults(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' c add mem1 PULL --pull --defaults", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "PULL")
}

func TestCLIStreamEdit(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	mem1, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str edit mem1 --subjects other -f --description 'test suite' --allow-direct", srv.ClientURL()))

	err = mem1.Reset()
	checkErr(t, err, "could not reset stream: %v", err)

	if len(mem1.Subjects()) != 1 {
		t.Fatalf("expected [other] got %v", mem1.Subjects())
	}

	if mem1.Description() != "test suite" {
		t.Fatalf("invalid description: %s", mem1.Description())
	}

	if mem1.Subjects()[0] != "other" {
		t.Fatalf("expected [other] got %v", mem1.Subjects())
	}

	if !mem1.DirectAllowed() {
		t.Fatalf("expected direct access to be enabled")
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' str edit mem1 -f --config testdata/mem1_config.json", srv.ClientURL()))

	err = mem1.Reset()
	checkErr(t, err, "could not reset stream: %v", err)

	if len(mem1.Subjects()) != 1 {
		t.Fatalf("expected [MEMORY.*] got %v", mem1.Subjects())
	}

	if mem1.Subjects()[0] != "MEMORY.*" {
		t.Fatalf("expected [MEMORY.*] got %v", mem1.Subjects())
	}

	if mem1.DirectAllowed() {
		t.Fatalf("expected direct access to be disabled")
	}
}

func TestCLIStreamCopy(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str cp mem1 file1 --storage file --subjects other", srv.ClientURL()))
	streamShouldExist(t, mgr, "file1")

	stream, err := mgr.LoadStream("file1")
	checkErr(t, err, "could not get stream: %v", err)
	info, err := stream.Information()
	checkErr(t, err, "could not get stream: %v", err)
	if info.Config.Storage != api.FileStorage {
		t.Fatalf("Expected file storage got %s", info.Config.Storage)
	}
}

func TestCLIConsumerCopy(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	_, err := mgr.NewConsumerFromDefault("mem1", push1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "push1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con cp mem1 push1 pull1 --pull --max-pending 0", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "pull1")

	pull1, err := mgr.LoadConsumer("mem1", "pull1")
	checkErr(t, err, "could not get consumer: %v", err)
	consumerShouldExist(t, mgr, "mem1", "pull1")

	ols, err := mgr.ConsumerNames("mem1")
	checkErr(t, err, "could not get consumer: %v", err)

	if len(ols) != 2 {
		t.Fatalf("expected 2 consumers, got %d", len(ols))
	}

	if !pull1.IsPullMode() {
		t.Fatalf("Expected pull1 to be pull-based, got %v", pull1.Configuration())
	}

	if pull1.MaxAckPending() != 1000 {
		t.Fatalf("Expected pull1 to have 1000 Ack outstanding, got %v", pull1.MaxAckPending())
	}
}

func TestCLIStreamBackupRestore(t *testing.T) {
	srv, nc, mgr := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := os.MkdirTemp("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)
	target := filepath.Join(dir, "backup.tgz")

	stream, err := mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "file1")

	for i := 0; i < 1024; i++ {
		_, err = nc.Request("js.file.1", []byte(fmt.Sprintf("message %d", i)), time.Second)
		checkErr(t, err, "publish failed")
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' stream backup file1 '%s'", srv.ClientURL(), target))

	err = stream.Delete()
	checkErr(t, err, "delete failed")
	streamShouldNotExist(t, mgr, "file1")

	runNatsCli(t, fmt.Sprintf("--server='%s' stream restore '%s'", srv.ClientURL(), target))
	streamShouldExist(t, mgr, "file1")

	stream, err = mgr.LoadStream("file1")
	checkErr(t, err, "load failed")
	state, err := stream.State()
	checkErr(t, err, "state failed")
	if state.LastSeq != 1024 {
		t.Fatalf("expected 1024 messages got %d", state.LastSeq)
	}
}

func TestCLIMessageRm(t *testing.T) {
	srv, nc, mgr := setupConsTest(t)
	defer srv.Shutdown()

	checkErr(t, nc.Publish("js.mem.1", []byte("msg1")), "publish failed")
	checkErr(t, nc.Publish("js.mem.1", []byte("msg2")), "publish failed")
	checkErr(t, nc.Publish("js.mem.1", []byte("msg3")), "publish failed")

	mem1, err := mgr.LoadStream("mem1")
	checkErr(t, err, "load failed")

	state, err := mem1.State()
	checkErr(t, err, "state failed")

	if state.Msgs != 3 {
		t.Fatalf("no message added to stream")
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' str rmm mem1 2 -f", srv.ClientURL()))
	state, err = mem1.State()
	checkErr(t, err, "state failed")

	if state.Msgs != 2 {
		t.Fatalf("message was not removed")
	}

	msg, err := mem1.ReadMessage(1)
	checkErr(t, err, "load failed")
	if cmp.Equal(msg.Data, []byte("msg1")) {
		checkErr(t, err, "load failed")
	}

	msg, err = mem1.ReadMessage(3)
	checkErr(t, err, "load failed")
	if cmp.Equal(msg.Data, []byte("msg3")) {
		checkErr(t, err, "load failed")
	}

	_, err = mem1.ReadMessage(2)
	if err == nil {
		t.Fatalf("loading delete message did not fail")
	}
}

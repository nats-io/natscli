// Copyright 2019 The NATS Authors
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
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jsm.go"
)

func init() {
	skipContexts = true
}

func runNatsCli(t *testing.T, args ...string) (output []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cmd string
	if os.Getenv("CI") == "true" {
		cmd = fmt.Sprintf("./nats %s", strings.Join(args, " "))
	} else {
		cmd = fmt.Sprintf("go run $(ls *.go | grep -v _test.go) %s", strings.Join(args, " "))
	}

	execution := exec.CommandContext(ctx, "bash", "-c", cmd)
	out, err := execution.CombinedOutput()
	if err != nil {
		t.Fatalf("nats utility failed: %v\n%v", err, string(out))
	}

	return out
}

func setupJStreamTest(t *testing.T) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
	t.Helper()

	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "could not create temporary js store: %v", err)

	srv, err = server.NewServer(&server.Options{
		Port:      -1,
		StoreDir:  dir,
		JetStream: true,
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

	runNatsCli(t, fmt.Sprintf("--server='%s' str create mem1 --subjects 'js.mem.>,js.other' --storage m --max-msgs-per-subject=10 --max-msgs=-1 --max-age=-1 --max-bytes=-1 --ack --retention limits --max-msg-size=1024 --discard new --dupe-window 1h --replicas 1 --description 'test suite'", srv.ClientURL()))
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

	_, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' str ls -j", srv.ClientURL()))

	list := []string{}
	err = json.Unmarshal(out, &list)
	checkErr(t, err, "could not parse cli output: %v", err)

	if len(list) != 1 {
		t.Fatalf("expected 1 ms got %v", list)
	}

	if list[0] != "mem1" {
		t.Fatalf("expected [mem1] got %v", list)
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

	td, err := ioutil.TempDir("", "")
	checkErr(t, err, "temp dir failed")
	os.RemoveAll(td)

	runNatsCli(t, fmt.Sprintf("--server='%s' str backup file1 %s --no-progress", srv.ClientURL(), td))

	preState, err := stream.State()
	checkErr(t, err, "state failed")
	stream.Delete()

	runNatsCli(t, fmt.Sprintf("--server='%s' str restore file1 %s --no-progress", srv.ClientURL(), td))
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
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
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

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 push1 --max-pending 10 --replay instant --deliver all --target out.push1 --ack explicit --filter '' --deliver-group '' --max-deliver 20 --bps 1024 --heartbeat=1s --flow-control --description 'test suite'", srv.ClientURL()))
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
	push1.Delete()

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 pull1 --config testdata/mem1_pull1_consumer.json", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "pull1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 push1 --filter 'js.mem.>' --max-pending 10 --replay instant --deliver subject --target out.push1 --ack explicit --deliver-group BOB --max-deliver 20 --bps 1024 --heartbeat=1s --flow-control --description 'test suite'", srv.ClientURL()))
	consumerShouldExist(t, mgr, "mem1", "push1")
	push1, err = mgr.LoadConsumer("mem1", "push1")
	checkErr(t, err, "push1 could not be loaded")
	if push1.DeliverPolicy() != api.DeliverLastPerSubject {
		t.Fatalf("expected subject delivery policy got %v", push1.DeliverPolicy())
	}
	if push1.DeliverGroup() != "BOB" {
		t.Fatalf("deliver group '%s' != 'BOB'", push1.DeliverGroup())
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

func TestCLIStreamEdit(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	mem1, err := mgr.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str edit mem1 --subjects other -f --description 'test suite'", srv.ClientURL()))

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

	runNatsCli(t, fmt.Sprintf("--server='%s' str edit mem1 -f --config testdata/mem1_config.json", srv.ClientURL()))

	err = mem1.Reset()
	checkErr(t, err, "could not reset stream: %v", err)

	if len(mem1.Subjects()) != 1 {
		t.Fatalf("expected [MEMORY.*] got %v", mem1.Subjects())
	}

	if mem1.Subjects()[0] != "MEMORY.*" {
		t.Fatalf("expected [MEMORY.*] got %v", mem1.Subjects())
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

	if pull1.MaxAckPending() != 20000 {
		t.Fatalf("Expected pull1 to have 20000 Ack outstanding, got %v", pull1.MaxAckPending())
	}
}

func TestCLIBackupRestore(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)

	target := filepath.Join(dir, "backup")

	mem1, err := mgr.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")
	origMem1Config := mem1.Configuration()

	c1, err := mem1.NewConsumerFromDefault(jsm.DefaultConsumer, jsm.DurableName("c1"))
	checkErr(t, err, "consumer c1 failed")
	origC1Config := c1.Configuration()

	t1, err := mgr.NewStreamTemplate("t1", 1, jsm.DefaultStream, jsm.Subjects("t1"), jsm.MemoryStorage())
	checkErr(t, err, "TEST template create failed: %s", err)
	origT1Config := t1.Configuration()

	runNatsCli(t, fmt.Sprintf("--server='%s' backup '%s'", srv.ClientURL(), target))

	checkErr(t, mem1.Delete(), "mem1 delete failed")
	checkErr(t, t1.Delete(), "t1 delete failed")

	runNatsCli(t, fmt.Sprintf("--server='%s' restore '%s'", srv.ClientURL(), target))

	mem1, err = mgr.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")
	if !cmp.Equal(mem1.Configuration(), origMem1Config) {
		t.Fatalf("mem1 recreate failed")
	}

	c1, err = mem1.LoadConsumer("c1")
	checkErr(t, err, "fetch c1 failed")
	if !cmp.Equal(c1.Configuration(), origC1Config) {
		t.Fatalf("mem1 recreate failed")
	}

	t1, err = mgr.LoadStreamTemplate("t1")
	checkErr(t, err, "template load failed")
	if !cmp.Equal(t1.Configuration(), origT1Config) {
		t.Fatalf("mem1 recreate failed")
	}
}

func TestCLIStreamBackupRestore(t *testing.T) {
	srv, nc, mgr := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := ioutil.TempDir("", "")
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

	runNatsCli(t, fmt.Sprintf("--server='%s' stream backup file1 %s", srv.ClientURL(), target))

	err = stream.Delete()
	checkErr(t, err, "delete failed")
	streamShouldNotExist(t, mgr, "file1")

	runNatsCli(t, fmt.Sprintf("--server='%s' stream restore file1 %s", srv.ClientURL(), target))
	streamShouldExist(t, mgr, "file1")

	stream, err = mgr.LoadStream("file1")
	checkErr(t, err, "load failed")
	state, err := stream.State()
	checkErr(t, err, "state failed")
	if state.LastSeq != 1024 {
		t.Fatalf("expected 1024 messages got %d", state.LastSeq)
	}
}

func TestCLIBackupRestore_UpdateStream(t *testing.T) {
	srv, _, mgr := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)

	target := filepath.Join(dir, "backup")

	mem1, err := mgr.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")

	runNatsCli(t, fmt.Sprintf("--server='%s' backup '%s'", srv.ClientURL(), target))

	runNatsCli(t, fmt.Sprintf("--server='%s' stream edit mem1 -f --subjects x", srv.ClientURL()))
	checkErr(t, mem1.Reset(), "reset failed")
	subs := mem1.Subjects()
	if len(subs) != 1 || subs[0] != "x" {
		t.Fatalf("expected [x] got %q", subs)
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' restore '%s' --update-streams", srv.ClientURL(), target))
	checkErr(t, mem1.Reset(), "reset failed")
	subs = mem1.Subjects()
	if len(subs) != 1 || subs[0] != "js.mem.>" {
		t.Fatalf("expected [js.mem.>] got %q", subs)
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

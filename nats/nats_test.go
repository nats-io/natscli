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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/internal/jsch"
)

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

func setupJStreamTest(t *testing.T) (srv *server.Server, nc *nats.Conn) {
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

	nc, err = prepareHelper(srv.ClientURL())
	checkErr(t, err, "could not connect client to server @ %s: %v", srv.ClientURL(), err)

	streams, err := jsch.StreamNames()
	checkErr(t, err, "could not load streams: %v", err)
	if len(streams) != 0 {
		t.Fatalf("found %v message streams but it should be empty", streams)
	}

	return srv, nc
}

func setupConsTest(t *testing.T) (srv *server.Server, nc *nats.Conn) {
	srv, nc = setupJStreamTest(t)

	_, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	return srv, nc
}

func streamShouldExist(t *testing.T, stream string) {
	t.Helper()
	known, err := jsch.IsKnownStream(stream)
	checkErr(t, err, "stream lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", stream)
	}
}

func streamShouldNotExist(t *testing.T, stream string) {
	t.Helper()
	known, err := jsch.IsKnownStream(stream)
	checkErr(t, err, "stream lookup failed: %v", err)
	if known {
		t.Fatalf("unexpectedly found %s already existing", stream)
	}
}

func consumerShouldExist(t *testing.T, stream string, consumer string) {
	t.Helper()
	known, err := jsch.IsKnownConsumer(stream, consumer)
	checkErr(t, err, "consumer lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", consumer)
	}
}

func streamInfo(t *testing.T, stream string) *server.StreamInfo {
	t.Helper()
	str, err := jsch.LoadStream(stream)
	checkErr(t, err, "could not load stream %s", stream)
	info, err := str.Information()
	checkErr(t, err, "could not load stream %s", stream)

	return info
}

func mem1Stream() server.StreamConfig {
	return server.StreamConfig{
		Name:     "mem1",
		Subjects: []string{"js.mem.>"},
		Storage:  server.MemoryStorage,
	}
}

func pull1Cons() server.ConsumerConfig {
	return server.ConsumerConfig{
		Durable:    "push1",
		DeliverAll: true,
		AckPolicy:  server.AckExplicit,
	}
}

func TestCLIStreamCreate(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' str create mem1 --subjects 'js.mem.>,js.other' --storage m  --max-msgs=-1 --max-age=-1 --max-bytes=-1 --ack --retention limits --max-msg-size=1024", srv.ClientURL()))
	streamShouldExist(t, "mem1")
	info := streamInfo(t, "mem1")

	if len(info.Config.Subjects) != 2 {
		t.Fatalf("expected 2 subjects in the message stream, got %v", info.Config.Subjects)
	}

	if info.Config.Subjects[0] != "js.mem.>" && info.Config.Subjects[1] != "js.other" {
		t.Fatalf("expects [js.mem.>, js.other] got %v", info.Config.Subjects)
	}

	if info.Config.Retention != server.LimitsPolicy {
		t.Fatalf("incorrect retention policy, expected limits got %s", info.Config.Retention.String())
	}

	if info.Config.Storage != server.MemoryStorage {
		t.Fatalf("incorrect storage received, expected memory got %s", info.Config.Storage.String())
	}

	if info.Config.MaxMsgSize != 1024 {
		t.Fatalf("incorrect max message size stream, expected 1024 got %v", info.Config.MaxMsgSize)
	}
}

func TestCLIStreamInfo(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' str info mem1 -j", srv.ClientURL()))

	var info server.StreamInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse cli output: %v", err)

	if info.Config.Name != "mem1" {
		t.Fatalf("expected info for mem1, got %s", info.Config.Name)

	}
}

func TestCLIStreamDelete(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create message stream: %v", err)
	streamShouldExist(t, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str rm mem1 -f", srv.ClientURL()))
	streamShouldNotExist(t, "mem1")
}

func TestCLIStreamLs(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

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
	srv, nc := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	i, err := stream.Information()
	checkErr(t, err, "could not get message stream info: %v", err)
	if i.State.Msgs != 1 {
		t.Fatalf("expected 1 message but got %d", i.State.Msgs)
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' str purge mem1 -f", srv.ClientURL()))
	i, err = stream.Information()
	checkErr(t, err, "could not get message stream info: %v", err)
	if i.State.Msgs != 0 {
		t.Fatalf("expected 0 messages but got %d", i.State.Msgs)
	}
}

func TestCLIStreamGet(t *testing.T) {
	srv, nc := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	item, err := stream.LoadMessage(1)
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

func TestCLIConsumerInfo(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, "mem1", "push1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con info mem1 push1 -j", srv.ClientURL()))
	var info server.ConsumerInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if info.Config.Durable != "push1" {
		t.Fatalf("did not find into for push1 in cli output: %v", string(out))
	}
}

func TestCLIConsumerLs(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, "mem1", "push1")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con ls mem1 -j", srv.ClientURL()))
	var info []string
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if len(info) != 1 {
		t.Fatalf("expected 1 item in output received %d", len(info))
	}

	if info[0] != "push1" {
		t.Fatalf("did not find into for push1 in cli output: %v", string(out))
	}
}

func TestCLIConsumerDelete(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, "mem1", "push1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con rm mem1 push1 -f", srv.ClientURL()))

	list, err := jsch.ConsumerNames("mem1")
	checkErr(t, err, "could not check cnsumer: %v", err)
	if len(list) != 0 {
		t.Fatalf("Expected no consumer, got %v", list)
	}
}

func TestCLIConsumerAdd(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' con add mem1 push1 --replay instant --deliver all --pull --filter '' --max-deliver 20", srv.ClientURL()))
	consumerShouldExist(t, "mem1", "push1")
}

func TestCLIConsumerNext(t *testing.T) {
	srv, nc := setupConsTest(t)
	defer srv.Shutdown()

	push1, err := jsch.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, "mem1", "push1")

	push1.Reset()
	if !push1.IsPullMode() {
		t.Fatalf("push1 is not push mode: %#v", push1.Configuration())
	}

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	out := runNatsCli(t, fmt.Sprintf("--server='%s' con next mem1 push1 --raw", srv.ClientURL()))

	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("did not receive 'hello', got: '%s'", string(out))
	}
}

func TestCLIStreamEdit(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	mem1, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str edit mem1 --subjects other", srv.ClientURL()))

	err = mem1.Reset()
	checkErr(t, err, "could not reset stream: %v", err)

	if len(mem1.Subjects()) != 1 {
		t.Fatalf("expected [other] got %v", mem1.Subjects())
	}

	if mem1.Subjects()[0] != "other" {
		t.Fatalf("expected [other] got %v", mem1.Subjects())
	}

}

func TestCLIStreamCopy(t *testing.T) {
	srv, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromDefault("mem1", mem1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, "mem1")

	runNatsCli(t, fmt.Sprintf("--server='%s' str cp mem1 file1 --storage file --subjects other", srv.ClientURL()))
	streamShouldExist(t, "file1")

	stream, err := jsch.LoadStream("file1")
	checkErr(t, err, "could not get stream: %v", err)
	info, err := stream.Information()
	checkErr(t, err, "could not get stream: %v", err)
	if info.Config.Storage != server.FileStorage {
		t.Fatalf("Expected file storage got %s", info.Config.Storage.String())
	}
}

func TestCLIConsumerCopy(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromDefault("mem1", pull1Cons())
	checkErr(t, err, "could not create consumer: %v", err)
	consumerShouldExist(t, "mem1", "push1")

	runNatsCli(t, fmt.Sprintf("--server='%s' con cp mem1 push1 pull1 --pull", srv.ClientURL()))
	consumerShouldExist(t, "mem1", "pull1")

	pull1, err := jsch.LoadConsumer("mem1", "pull1")
	checkErr(t, err, "could not get consumer: %v", err)
	consumerShouldExist(t, "mem1", "pull1")

	ols, err := jsch.ConsumerNames("mem1")
	checkErr(t, err, "could not get consumer: %v", err)

	if len(ols) != 2 {
		t.Fatalf("expected 2 consumers, got %d", len(ols))
	}

	if !pull1.IsPullMode() {
		t.Fatalf("Expected pull1 to be pull-based, got %v", pull1.Configuration())
	}
}

func TestCLIBackupRestore(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)

	target := filepath.Join(dir, "backup")

	mem1, err := jsch.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")
	origMem1Config := mem1.Configuration()

	c1, err := mem1.NewConsumerFromDefault(jsch.DefaultConsumer, jsch.DurableName("c1"))
	checkErr(t, err, "consumer c1 failed")
	origC1Config := c1.Configuration()

	t1, err := jsch.NewStreamTemplate("t1", 1, jsch.DefaultStream)
	checkErr(t, err, "TEST template create failed")
	origT1Config := t1.Configuration()

	runNatsCli(t, fmt.Sprintf("--server='%s' backup '%s'", srv.ClientURL(), target))

	checkErr(t, mem1.Delete(), "mem1 delete failed")
	checkErr(t, t1.Delete(), "t1 delete failed")

	runNatsCli(t, fmt.Sprintf("--server='%s' restore '%s'", srv.ClientURL(), target))

	mem1, err = jsch.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")
	if !cmp.Equal(mem1.Configuration(), origMem1Config) {
		t.Fatalf("mem1 recreate failed")
	}

	c1, err = mem1.LoadConsumer("c1")
	checkErr(t, err, "fetch c1 failed")
	if !cmp.Equal(c1.Configuration(), origC1Config) {
		t.Fatalf("mem1 recreate failed")
	}

	t1, err = jsch.LoadStreamTemplate("t1")
	checkErr(t, err, "template load failed")
	if !cmp.Equal(t1.Configuration(), origT1Config) {
		t.Fatalf("mem1 recreate failed")
	}
}

func TestCLIBackupRestore_UpdateStream(t *testing.T) {
	srv, _ := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)

	target := filepath.Join(dir, "backup")

	mem1, err := jsch.LoadStream("mem1")
	checkErr(t, err, "fetch mem1 failed")

	runNatsCli(t, fmt.Sprintf("--server='%s' backup '%s'", srv.ClientURL(), target))

	runNatsCli(t, fmt.Sprintf("--server='%s' stream edit mem1 --subjects x", srv.ClientURL()))
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
	srv, nc := setupConsTest(t)
	defer srv.Shutdown()

	checkErr(t, nc.Publish("js.mem.1", []byte("msg1")), "publish failed")
	checkErr(t, nc.Publish("js.mem.1", []byte("msg2")), "publish failed")
	checkErr(t, nc.Publish("js.mem.1", []byte("msg3")), "publish failed")

	mem1, err := jsch.LoadStream("mem1")
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

	msg, err := mem1.LoadMessage(1)
	checkErr(t, err, "load failed")
	if cmp.Equal(msg.Data, []byte("msg1")) {
		checkErr(t, err, "load failed")
	}

	msg, err = mem1.LoadMessage(3)
	checkErr(t, err, "load failed")
	if cmp.Equal(msg.Data, []byte("msg3")) {
		checkErr(t, err, "load failed")
	}

	msg, err = mem1.LoadMessage(2)
	if err == nil {
		t.Fatalf("loading delete message did not fail")
	}
}

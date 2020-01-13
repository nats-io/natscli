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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/jsch"
)

func runJsmCli(t *testing.T, args ...string) (output []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cmd string
	if os.Getenv("CI") == "true" {
		cmd = fmt.Sprintf("./jsm %s", strings.Join(args, " "))
	} else {
		cmd = fmt.Sprintf("go run $(ls *.go | grep -v _test.go) %s", strings.Join(args, " "))
	}
	execution := exec.CommandContext(ctx, "bash", "-c", cmd)
	out, err := execution.CombinedOutput()
	if err != nil {
		t.Fatalf("jsm utility failed: %v\n%v", err, string(out))
	}

	return out
}

func setupJSMTest(t *testing.T) (srv *server.Server, nc *nats.Conn) {
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

	sets, err := jsch.StreamNames()
	checkErr(t, err, "could not load sets: %v", err)
	if len(sets) != 0 {
		t.Fatalf("found %v message sets but it should be empty", sets)
	}

	return srv, nc
}

func setupObsTest(t *testing.T) (srv *server.Server, nc *nats.Conn) {
	srv, nc = setupJSMTest(t)

	_, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	return srv, nc
}

func msShouldExist(t *testing.T, set string) {
	t.Helper()
	known, err := jsch.IsKnownStream(set)
	checkErr(t, err, "ms lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", set)
	}
}

func msShouldNotExist(t *testing.T, set string) {
	t.Helper()
	known, err := jsch.IsKnownStream(set)
	checkErr(t, err, "ms lookup failed: %v", err)
	if known {
		t.Fatalf("unexpectedly found %s already existing", set)
	}
}

func obsShouldExist(t *testing.T, set string, obs string) {
	t.Helper()
	known, err := jsch.IsKnownConsumer(set, obs)
	checkErr(t, err, "obs lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", obs)
	}
}

func setInfo(t *testing.T, set string) *server.MsgSetInfo {
	t.Helper()
	stream, err := jsch.LoadStream(set)
	checkErr(t, err, "could not load stream %s", set)
	info, err := stream.Information()
	checkErr(t, err, "could not load stream %s", set)

	return info
}

func mem1MS() server.MsgSetConfig {
	return server.MsgSetConfig{
		Name:     "mem1",
		Subjects: []string{"js.mem.>"},
		Storage:  server.MemoryStorage,
	}
}

func pull1Obs() server.ObservableConfig {
	return server.ObservableConfig{
		Durable:    "push1",
		DeliverAll: true,
		AckPolicy:  server.AckExplicit,
	}
}

func TestCLIMSCreate(t *testing.T) {
	srv, _ := setupJSMTest(t)
	defer srv.Shutdown()

	runJsmCli(t, fmt.Sprintf("--server='%s' ms create mem1 --subjects 'js.mem.>,js.other' --storage m  --max-msgs=-1 --max-age=-1 --max-bytes=-1 --ack --retention stream --max-msg-size=1024", srv.ClientURL()))
	msShouldExist(t, "mem1")
	info := setInfo(t, "mem1")

	if len(info.Config.Subjects) != 2 {
		t.Fatalf("expected 2 subjects in the message set, got %v", info.Config.Subjects)
	}

	if info.Config.Subjects[0] != "js.mem.>" && info.Config.Subjects[1] != "js.other" {
		t.Fatalf("expects [js.mem.>, js.other] got %v", info.Config.Subjects)
	}

	if info.Config.Retention != server.StreamPolicy {
		t.Fatalf("incorrect retention policy set, expected stream got %s", info.Config.Retention.String())
	}

	if info.Config.Storage != server.MemoryStorage {
		t.Fatalf("incorrect storage received, expected memory got %s", info.Config.Storage.String())
	}

	if info.Config.MaxMsgSize != 1024 {
		t.Fatalf("incorrect max message size set, expected 1024 got %v", info.Config.MaxMsgSize)
	}
}

func TestCLIMSInfo(t *testing.T) {
	srv, _ := setupJSMTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms info mem1 -j", srv.ClientURL()))

	var info server.MsgSetInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse cli output: %v", err)

	if info.Config.Name != "mem1" {
		t.Fatalf("expected info for mem1, got %s", info.Config.Name)

	}
}

func TestCLIMSDelete(t *testing.T) {
	srv, _ := setupJSMTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	runJsmCli(t, fmt.Sprintf("--server='%s' ms rm mem1 -f", srv.ClientURL()))
	msShouldNotExist(t, "mem1")
}

func TestCLIMSLs(t *testing.T) {
	srv, _ := setupJSMTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms ls -j", srv.ClientURL()))

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

func TestCLIMSPurge(t *testing.T) {
	srv, nc := setupJSMTest(t)
	defer srv.Shutdown()

	stream, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	i, err := stream.Information()
	checkErr(t, err, "could not get message set info: %v", err)
	if i.Stats.Msgs != 1 {
		t.Fatalf("expected 1 message but got %d", i.Stats.Msgs)
	}

	runJsmCli(t, fmt.Sprintf("--server='%s' ms purge mem1 -f", srv.ClientURL()))
	i, err = stream.Information()
	checkErr(t, err, "could not get message set info: %v", err)
	if i.Stats.Msgs != 0 {
		t.Fatalf("expected 0 messages but got %d", i.Stats.Msgs)
	}
}

func TestCLIMSGet(t *testing.T) {
	srv, nc := setupJSMTest(t)
	defer srv.Shutdown()

	stream, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	item, err := stream.LoadMessage(1)
	checkErr(t, err, "could not get message: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms get mem1 1 -j", srv.ClientURL()))
	err = json.Unmarshal(out, &item)
	checkErr(t, err, "could not parse output: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}
}

func TestCLIObsInfo(t *testing.T) {
	srv, _ := setupObsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromTemplate("mem1", pull1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	obsShouldExist(t, "mem1", "push1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs info mem1 push1 -j", srv.ClientURL()))
	var info server.ObservableInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if info.Config.Durable != "push1" {
		t.Fatalf("did not find into for push1 in cli output: %v", string(out))
	}
}

func TestCLIObsLs(t *testing.T) {
	srv, _ := setupObsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromTemplate("mem1", pull1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	obsShouldExist(t, "mem1", "push1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs ls mem1 -j", srv.ClientURL()))
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

func TestCLIObsDelete(t *testing.T) {
	srv, _ := setupObsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromTemplate("mem1", pull1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	obsShouldExist(t, "mem1", "push1")

	runJsmCli(t, fmt.Sprintf("--server='%s' obs rm mem1 push1 -f", srv.ClientURL()))

	list, err := jsch.ConsumerNames("mem1")
	checkErr(t, err, "could not check observable: %v", err)
	if len(list) != 0 {
		t.Fatalf("Expected no observables, got %v", list)
	}
}

func TestCLIObsAdd(t *testing.T) {
	srv, _ := setupObsTest(t)
	defer srv.Shutdown()

	runJsmCli(t, fmt.Sprintf("--server='%s' obs add mem1 push1 --replay instant --deliver all --pull --filter '' --max-deliver 20", srv.ClientURL()))
	obsShouldExist(t, "mem1", "push1")
}

func TestCLIObsNext(t *testing.T) {
	srv, nc := setupObsTest(t)
	defer srv.Shutdown()

	push1, err := jsch.NewConsumerFromTemplate("mem1", pull1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	obsShouldExist(t, "mem1", "push1")

	push1.Reset()
	if !push1.IsPullMode() {
		t.Fatalf("push1 is not push mode: %#v", push1.Configuration())
	}

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs next mem1 push1 --raw", srv.ClientURL()))

	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("did not receive 'hello', got: '%s'", string(out))
	}
}

func TestCLIMSCopy(t *testing.T) {
	srv, _ := setupJSMTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewStreamFromTemplate("mem1", mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, "mem1")

	runJsmCli(t, fmt.Sprintf("--server='%s' ms cp mem1 file1 --storage file ", srv.ClientURL()))
	msShouldExist(t, "file1")

	stream, err := jsch.LoadStream("file1")
	checkErr(t, err, "could not get message set: %v", err)
	info, err := stream.Information()
	checkErr(t, err, "could not get message set: %v", err)
	if info.Config.Storage != server.FileStorage {
		t.Fatalf("Expected file storage got %s", info.Config.Storage.String())
	}
}

func TestCLIObsCopy(t *testing.T) {
	srv, _ := setupObsTest(t)
	defer srv.Shutdown()

	_, err := jsch.NewConsumerFromTemplate("mem1", pull1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	obsShouldExist(t, "mem1", "push1")

	runJsmCli(t, fmt.Sprintf("--server='%s' obs cp mem1 push1 pull1 --pull", srv.ClientURL()))
	obsShouldExist(t, "mem1", "pull1")

	pull1, err := jsch.LoadConsumer("mem1", "pull1")
	checkErr(t, err, "could not get observable: %v", err)
	obsShouldExist(t, "mem1", "pull1")

	ols, err := jsch.ConsumerNames("mem1")
	checkErr(t, err, "could not get observables: %v", err)

	if len(ols) != 2 {
		t.Fatalf("expected 2 observables, got %d", len(ols))
	}

	if !pull1.IsPullMode() {
		t.Fatalf("Expected pull1 to be pull-based, got %v", pull1.Configuration())
	}
}

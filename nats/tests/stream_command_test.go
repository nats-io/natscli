// Copyright 2025 The NATS Authors
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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func setupStreamTest(t *testing.T, args ...jsm.ConsumerOption) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager, name string) {
	srv, nc, mgr = setupJStreamTest(t)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	name = fmt.Sprintf("TEST_%d", rng.Intn(1000000))
	_, err := mgr.NewStream(name, jsm.Subjects("ORDERS.*"))
	if err != nil {
		t.Fatalf("unable to create stream: %s", err)
	}

	return srv, nc, mgr, name
}

func TestStreamAdd(t *testing.T) {
	srv, _, mgr, _ := setupStreamTest(t)
	defer srv.Shutdown()
	name := "name_for_add"

	runNatsCli(t, fmt.Sprintf("--server='%s' stream add %s --defaults --subjects=test", srv.ClientURL(), name))
	_, err := mgr.LoadStream(name)
	if err != nil {
		t.Errorf("failed to add stream %s: %s", name, err)
	}
}

func TestStreamLS(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream ls", srv.ClientURL())))

	if !expectMatchLine(t, output, name, "0", "0 B", "never") {
		t.Errorf("missing stream %s from output: %s", name, output)
	}
}

func TestStreamReport(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream report", srv.ClientURL())))
	if !expectMatchLine(t, output, name, "0", "0", "0 B", "0", "0") {
		t.Errorf("missing stream %s from output: %s", name, output)
	}
}

func TestStreamInfo(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream info %s --json", srv.ClientURL(), name)))
	err := expectMatchJSON(t, output, map[string]any{
		"config": map[string]any{
			"name":                 name,
			"subjects":             []any{"ORDERS.*"},
			"retention":            "limits",
			"max_consumers":        "-1",
			"max_msgs_per_subject": "-1",
			"max_msgs":             "-1",
			"max_bytes":            "-1",
			"max_age":              `\d+`,
			"max_msg_size":         "-1",
			"storage":              "file",
			"discard":              "old",
			"num_replicas":         "1",
			"duplicate_window":     `\d+`,
			"sealed":               "false",
			"deny_delete":          "false",
			"deny_purge":           "false",
			"allow_rollup_hdrs":    "false",
			"allow_direct":         "false",
			"mirror_direct":        "false",
			"metadata":             map[string]any{
				// Removed server version fields so we don't break on update
			},
			"consumer_limits": map[string]any{},
		},
		"created": `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`,
		"state": map[string]any{
			"messages":       "0",
			"bytes":          "0",
			"first_seq":      "0",
			"first_ts":       "0001-01-01T00:00:00Z",
			"last_seq":       "0",
			"last_ts":        "0001-01-01T00:00:00Z",
			"consumer_count": "0",
		},
		"cluster": map[string]any{
			"leader": `.+`,
		},
		"ts": `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`,
	})
	if err != nil {
		t.Error(err)
	}
}

func TestStreamState(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream state %s --json", srv.ClientURL(), name)))
	err := expectMatchJSON(t, output, map[string]any{
		"config": map[string]any{
			"name":                 name,
			"subjects":             []any{"ORDERS.*"},
			"retention":            "limits",
			"max_consumers":        "-1",
			"max_msgs_per_subject": "-1",
			"max_msgs":             "-1",
			"max_bytes":            "-1",
			"max_age":              `\d+`,
			"max_msg_size":         "-1",
			"storage":              "file",
			"discard":              "old",
			"num_replicas":         "1",
			"duplicate_window":     `\d+`,
			"sealed":               "false",
			"deny_delete":          "false",
			"deny_purge":           "false",
			"allow_rollup_hdrs":    "false",
			"allow_direct":         "false",
			"mirror_direct":        "false",
			"metadata":             map[string]any{
				// Removed server version fields so we don't break on update
			},
			"consumer_limits": map[string]any{},
		},
		"created": `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`,
		"state": map[string]any{
			"messages":       "0",
			"bytes":          "0",
			"first_seq":      "0",
			"first_ts":       "0001-01-01T00:00:00Z",
			"last_seq":       "0",
			"last_ts":        "0001-01-01T00:00:00Z",
			"consumer_count": "0",
		},
		"cluster": map[string]any{
			"leader": `.+`,
		},
		"ts": `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`,
	})

	if err != nil {
		t.Error(err)
	}
}

func TestStreamSubjects(t *testing.T) {
	srv, nc, _, name := setupStreamTest(t)
	defer srv.Shutdown()
	subject := "ORDERS.new"
	msg := "test"

	err := nc.Publish(subject, []byte(msg))
	if err != nil {
		t.Errorf("unable to publish message to stream %s: %s", name, err)
	}

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream subjects %s", srv.ClientURL(), name)))
	if !expectMatchLine(t, output, subject, "1") {
		t.Errorf("missing stream %s from output: %s", name, output)
	}
}

func TestStreamEdit(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream edit %s --description=TEST --force", srv.ClientURL(), name)))
	if !expectMatchLine(t, output, "Description", `""`) || !expectMatchLine(t, output, "Description", `"TEST"`) {
		t.Errorf("expected Description line not found: %s", output)
	}
}

func TestStreamRM(t *testing.T) {
	srv, _, mgr, name := setupStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' stream rm %s --force", srv.ClientURL(), name))
	stream, err := mgr.LoadStream(name)
	if err == nil {
		t.Errorf("failed to delete stream %+v", stream)
	}
}

func TestStreamPurge(t *testing.T) {
	srv, nc, mgr, name := setupStreamTest(t)
	defer srv.Shutdown()

	err := nc.Publish("ORDERS.new", []byte("TEST MESSAGE"))
	if err != nil {
		t.Errorf("failed to pushlish message to stream: %s %s", name, err)
	}

	info, err := getStreamInfo(name, mgr)
	if err != nil {
		t.Error(err)
	}

	const maxAttempts = 5

	for i := 0; i < maxAttempts; i++ {
		if info.Msgs != 1 {
			time.Sleep(50 * time.Millisecond)
		}
		if i == maxAttempts {
			t.Errorf("unexpected number of messages in stream: %d but expected 1", info.Msgs)
		}
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' stream purge %s --force", srv.ClientURL(), name))

	info, err = getStreamInfo(name, mgr)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < maxAttempts; i++ {
		if info.Msgs != 0 {
			time.Sleep(50 * time.Millisecond)
		}
		if i == maxAttempts {
			t.Errorf("unexpected number of messages in stream: %d but expected 1", info.Msgs)
		}
	}
}

func TestStreamCopy(t *testing.T) {
	srv, _, mgr, name := setupStreamTest(t)
	copiedStreamName := "TESTSTREAM"
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' stream copy %s %s --subjects=TEST.*", srv.ClientURL(), name, copiedStreamName))
	_, err := mgr.LoadStream(copiedStreamName)
	if err != nil {
		t.Errorf("failed to copy stream %s: %s", copiedStreamName, err)
	}
}

func TestStreamRMM(t *testing.T) {
	srv, nc, mgr, name := setupStreamTest(t)
	defer srv.Shutdown()

	err := nc.Publish("ORDERS.new", []byte("TEST MESSAGE"))
	if err != nil {
		t.Errorf("failed to pushlish message to stream: %s %s", name, err)
	}

	const maxAttempts = 5

	info, err := getStreamInfo(name, mgr)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < maxAttempts; i++ {
		if info.Msgs != 1 {
			time.Sleep(50 * time.Millisecond)
		}
		if i == maxAttempts {
			t.Errorf("unexpected number of messages in stream: %d but expected 1", info.Msgs)
		}
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' stream rmm %s 1 --force", srv.ClientURL(), name))

	info, err = getStreamInfo(name, mgr)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < maxAttempts; i++ {
		if info.Msgs != 0 {
			time.Sleep(50 * time.Millisecond)
		}
		if i == maxAttempts {
			t.Errorf("unexpected number of messages in stream: %d but expected 1", info.Msgs)
		}
	}
}

// View command has to be run with a terminal
//func TestStreamView(t *testing.T) {}

func TestStreamGet(t *testing.T) {
	srv, nc, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	err := nc.Publish("ORDERS.new", []byte("TEST MESSAGE"))
	if err != nil {
		t.Errorf("failed to pushlish message to stream: %s %s", name, err)
	}

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream get %s 1 --json", srv.ClientURL(), name)))
	err = expectMatchJSON(t, output, map[string]any{
		"subject": "ORDERS.new",
		"seq":     1,
		"data":    "VEVTVCBNRVNTQUdF",
		"time":    `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$`,
	})
	if err != nil {
		t.Error(err)
	}

}

func TestStreamBackup(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()
	tmpDir := t.TempDir()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream backup %s %s", srv.ClientURL(), name, tmpDir)))
	if !expectMatchLine(t, output, fmt.Sprintf("Starting backup of Stream \"%s\"", name)) ||
		!expectMatchLine(t, output, "done") {
		t.Errorf("Unexecpted output :%s", output)
	}
}

func TestStreamRestore(t *testing.T) {
	srv, _, mgr, name := setupStreamTest(t)
	defer srv.Shutdown()
	tmpDir := t.TempDir()

	runNatsCli(t, fmt.Sprintf("--server='%s' stream backup %s %s", srv.ClientURL(), name, tmpDir))
	mgr.DeleteStream(name)
	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream restore %s", srv.ClientURL(), tmpDir)))
	if !expectMatchLine(t, output, fmt.Sprintf("Starting restore of Stream \"%s\"", name)) ||
		!expectMatchLine(t, output, fmt.Sprintf("Restored stream \"%s\" in \\d+s", name)) {
		t.Errorf("Unexecpted output :%s", output)
	}
}

func TestStreamSeal(t *testing.T) {
	os.Setenv("TESTING", "true")
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream seal %s --force", srv.ClientURL(), name)))
	err := expectMatchJSON(t, output, map[string]any{
		"Sealed": "true",
	})
	if err != nil {
		t.Errorf("Failed to seal stream %s :%s", name, output)
	}
}

func TestStreamGaps(t *testing.T) {
	srv, _, _, name := setupStreamTest(t)
	defer srv.Shutdown()

	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream gaps %s --force", srv.ClientURL(), name)))
	if !expectMatchLine(t, output, `No deleted messages in `+name) {
		t.Errorf("Unexecpted output :%s", output)
	}
}

// Graph command has to be run with a terminal
// func TestStreamGraph(t *testing.T) {}

func TestStreamStepDown(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := "TESTSTEPDOWN"
		_, err := mgr.NewStream(name, jsm.Subjects("ORDERS.*"), jsm.Replicas(2))
		if err != nil {
			t.Errorf("unable to create stream: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream cluster step-down %s", servers[0].ClientURL(), name)))
		if !expectMatchLine(t, output, `New leader elected "s\d"`) {
			t.Errorf("Unexecpted output :%s", output)
		}

		return nil
	})
}

func TestStreamBalance(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := "TESTBALANCE"
		_, err := mgr.NewStream(name, jsm.Subjects("ORDERS.*"), jsm.Replicas(2))
		if err != nil {
			t.Errorf("unable to create stream: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream cluster balance", servers[0].ClientURL())))
		if !expectMatchLine(t, output, `Balanced \d streams`) {
			t.Errorf("Unexecpted output :%s", output)
		}

		return nil
	})
}

func TestStreamPeerRemove(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := "TESTPEER"
		_, err := mgr.NewStream(name, jsm.Subjects("ORDERS.*"), jsm.Replicas(2))
		if err != nil {
			t.Errorf("unable to create stream: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' stream cluster peer-remove %s %s", servers[0].ClientURL(), name, servers[0])))
		if !expectMatchLine(t, output, `Removing peer "s1"`) || !expectMatchLine(t, output, `Requested removal of peer "s1"`) {
			t.Errorf("Unexecpted output :%s", output)
		}

		return nil
	})
}

func getStreamInfo(name string, mgr *jsm.Manager) (*api.StreamState, error) {
	stream, err := mgr.LoadStream(name)
	if err != nil {
		return nil, fmt.Errorf("failed to load stream %s: %s", name, err)
	}
	info, err := stream.LatestInformation()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info %s: %s", name, err)
	}
	return &info.State, nil
}

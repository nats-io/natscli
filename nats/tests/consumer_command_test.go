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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Create a randomly named consumer. This function implies testing the 'add' subcommand
func createConsumer(mgr *jsm.Manager, t *testing.T, args ...jsm.ConsumerOption) (string, error) {
	os.Setenv("TESTING", "true")
	t.Helper()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	consumerName := fmt.Sprintf("TEST_%d", rng.Intn(1000000))
	opts := []jsm.ConsumerOption{
		jsm.DurableName(consumerName),
		jsm.AcknowledgeExplicit(),
		jsm.FilterStreamBySubject("ORDERS.new"),
	}
	opts = append(opts, args...)

	_, err := mgr.NewConsumer("ORDERS", opts...)

	if err != nil {
		return "", err
	}

	return consumerName, nil
}

func setupConsumerTest(t *testing.T, args ...jsm.ConsumerOption) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager, name string) {
	srv, nc, mgr = setupJStreamTest(t)
	_, err := mgr.NewStream("ORDERS", jsm.Subjects("ORDERS.*"))
	if err != nil {
		t.Fatalf("unable to create stream: %s", err)
	}
	name, err = createConsumer(mgr, t, args...)
	if err != nil {
		t.Errorf("unable to setup consumer tests: %s", err)
	}
	return srv, nc, mgr, name
}

func TestConsumerAdd(t *testing.T) {
	srv, _, _, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	name := "ADD_TEST_CONSUMER"

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer add ORDERS %s --defaults --pull", srv.ClientURL(), name))
	err := expectMatchJSON(t, string(output),
		map[string]any{
			"Configuration": map[string]any{
				"Ack Policy":        "Explicit",
				"Ack Wait":          "30.00s",
				"Deliver Policy":    "All",
				"Max Ack Pending":   "1,000",
				"Max Waiting Pulls": "512",
				"Name":              "ADD_TEST_CONSUMER",
				"Pull Mode":         "true",
				"Replay Policy":     "Instant",
			},
			"Header": `Information for Consumer ORDERS > ADD_TEST_CONSUMER created`,
			"State": map[string]any{
				"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
				"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
				"Outstanding Acks":       "0 out of maximum 1,000",
				"Redelivered Messages":   "0",
				"Required API Level":     "0 hosted at level 1",
				"Unprocessed Messages":   "0",
				"Waiting Pulls":          "0 of maximum 512",
			},
		},
	)
	if err != nil {
		t.Error(err)
	}
}

func TestConsumerRM(t *testing.T) {
	srv, _, mgr, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer rm ORDERS %s --force", srv.ClientURL(), name))

	_, err := mgr.LoadConsumer("ORDERS", name)
	if err == nil {
		t.Errorf("failed to delete consumer: %s", output)
	}

}

func TestConsumerEdit(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer edit ORDERS %s --max-pending=10 -f", srv.ClientURL(), name))
	if !strings.Contains(string(output), "MaxAckPending: 1000,") {
		t.Errorf("expected old MaxAckPending line not found")
	}
	if !strings.Contains(string(output), "MaxAckPending: 10,") {
		t.Errorf("expected new MaxAckPending line not found")
	}
}

func TestConsumerLS(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer ls ORDERS", srv.ClientURL()))

	if !expectMatchLine(t, string(output), name, `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, "0.*0", "never") {
		t.Errorf("consumer row not found in output:\n%s", output)
	}
}

func TestConsumerFind(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer find ORDERS --pull", srv.ClientURL()))
	if !expectMatchRegex(t, name, string(output)) {
		t.Errorf("failed to find consumer %s in %s", name, string(output))
	}
}

func TestConsumerInfo(t *testing.T) {
	os.Setenv("TESTING", "true")
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer info ORDERS %s", srv.ClientURL(), name))
	expected := map[string]any{
		"Header": `Information for Consumer ORDERS > TEST_\d+ created .*`,
		"Configuration": map[string]any{
			"Name":              `TEST_\d+`,
			"Pull Mode":         "true",
			"Filter Subject":    `ORDERS\.new`,
			"Ack Policy":        "Explicit",
			"Replay Policy":     "Instant",
			"Deliver Policy":    "All",
			"Max Ack Pending":   "1,000",
			"Max Waiting Pulls": "512",
			"Ack Wait":          `30\.00s`,
		},
		"State": map[string]any{
			"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
			"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
			"Outstanding Acks":       "0 out of maximum 1,000",
			"Redelivered Messages":   "0",
			"Required API Level":     "0 hosted at level 1",
			"Unprocessed Messages":   "0",
			"Waiting Pulls":          "0 of maximum 512",
		},
	}

	if err := expectMatchJSON(t, string(output), expected); err != nil {
		t.Errorf("columns mismatch: %v", err)
	}
}

func TestConsumerState(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer state ORDERS %s", srv.ClientURL(), name))
	expected := map[string]any{
		"Header": `State for Consumer ORDERS > TEST_\d+ created .*`,
		"State": map[string]any{
			"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
			"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
			"Outstanding Acks":       "0 out of maximum 1,000",
			"Redelivered Messages":   "0",
			"Required API Level":     "0 hosted at level 1",
			"Unprocessed Messages":   "0",
			"Waiting Pulls":          "0 of maximum 512",
		},
	}

	if err := expectMatchJSON(t, string(output), expected); err != nil {
		t.Errorf("columns mismatch: %v", err)
	}
}

func TestConsumerCopy(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer copy ORDERS %s COPY_1", srv.ClientURL(), name))
	expected := map[string]any{
		"Header": `Information for Consumer ORDERS > COPY_1 created`,
		"State": map[string]any{
			"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
			"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
			"Outstanding Acks":       "0 out of maximum 1,000",
			"Redelivered Messages":   "0",
			"Required API Level":     "0 hosted at level 1",
			"Unprocessed Messages":   "0",
			"Waiting Pulls":          "0 of maximum 512",
		},
	}

	if err := expectMatchJSON(t, string(output), expected); err != nil {
		t.Errorf("columns mismatch: %v", err)
	}
}

func TestConsumerNext(t *testing.T) {
	srv, nc, _, name := setupConsumerTest(t)
	defer srv.Shutdown()
	msgBody := "test message"

	err := nc.Publish("ORDERS.new", []byte(msgBody))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer next ORDERS %s", srv.ClientURL(), name))

	if !expectMatchRegex(t, msgBody, string(output)) {
		t.Errorf("failed to find message body %s in %s", msgBody, string(output))
	}
}

func TestConsumerSub(t *testing.T) {
	srv, nc, _, name := setupConsumerTest(t)
	defer srv.Shutdown()
	msgBody := "test message"

	err := nc.Publish("ORDERS.new", []byte(msgBody))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer sub ORDERS %s", srv.ClientURL(), name))

	if !expectMatchRegex(t, msgBody, string(output)) {
		t.Errorf("failed to find message body %s in %s", msgBody, string(output))
	}

}

// Graph command has to be run with a terminal
// func TestConsumerGraph(t *testing.T) {}

func TestConsumerPause(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer pause ORDERS %s \"2050-04-22 11:48:55\" --force", srv.ClientURL(), name))
	expected := fmt.Sprintf("Paused ORDERS > %s until 2050-04-22", name)
	if !expectMatchRegex(t, expected, string(output)) {
		t.Errorf("failed to pause consumer %s: %s", name, string(output))
	}
}
func TestConsumerUnpin(t *testing.T) {
	groupName := "PINNED_GROUP"
	srv, nc, _, consumerName := setupConsumerTest(t, jsm.PinnedClientPriorityGroups(time.Duration(1*time.Minute), groupName))

	err := nc.Publish("ORDERS.new", []byte("test"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	reqBody := map[string]any{
		"batch":   1,
		"expires": 1000000000,
		"group":   groupName,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("failed to marshal pull request: %v", err)
	}

	msg := &nats.Msg{
		Subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS." + consumerName,
		Data:    data,
	}

	_, err = nc.RequestMsg(msg, 2*time.Second)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer unpin ORDERS %s %s -f", srv.ClientURL(), consumerName, groupName))

	expected := `Unpinned client .+ from Priority Group ORDERS > TEST_\d+ > PINNED_GROUP`
	if !expectMatchRegex(t, expected, string(output)) {
		t.Errorf("failed to unpin consumer %s: %s", consumerName, string(output))
	}
}

func TestConsumerResume(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' consumer pause ORDERS %s \"2050-04-22 11:48:55\" --force", srv.ClientURL(), name))
	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer resume ORDERS %s --force", srv.ClientURL(), name))

	expected := fmt.Sprintf("Consumer ORDERS > %s was resumed while previously paused until 2050-04-22", name)
	if !expectMatchRegex(t, expected, string(output)) {
		t.Errorf("failed to pause consumer %s: %s", name, string(output))
	}
}

func TestConsumerReport(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer report ORDERS", srv.ClientURL()))

	if !expectMatchLine(t, string(output), name, "Pull", "Explicit") {
		t.Errorf("consumer row not found in output:\n%s", output)
	}
}

func TestConsumerClusterDown(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		_, err := mgr.NewStream("ORDERS", jsm.Subjects("ORDERS.*"), jsm.Replicas(3))
		if err != nil {
			t.Errorf("unable to create stream: %s", err)
		}
		_, err = mgr.NewConsumer("ORDERS", jsm.DurableName("CLUSTER_TEST"), jsm.AcknowledgeExplicit(), jsm.FilterStreamBySubject("ORDERS.new"))
		if err != nil {
			t.Errorf("unable to create consumer: %s", err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer cluster step-down ORDERS CLUSTER_TEST", servers[0].ClientURL()))
		success, field, expected := expectMatchMap(t,
			map[string]string{
				"stepdown": "Requesting leader step down of \"s\\d\" in a 3 peer RAFT group",
				"election": "New leader elected \"s\\d\"",
			},
			string(output),
		)

		if !success {
			t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
		}
		return nil
	})
}

func TestConsumerClusterBalance(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		_, err := mgr.NewStream("ORDERS", jsm.Subjects("ORDERS.*"), jsm.Replicas(3))
		if err != nil {
			t.Errorf("unable to create stream: %s", err)
		}
		_, err = mgr.NewConsumer("ORDERS", jsm.DurableName("CLUSTER_TEST"), jsm.AcknowledgeExplicit(), jsm.FilterStreamBySubject("ORDERS.new"))
		if err != nil {
			t.Errorf("unable to create consumer: %s", err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer cluster balance ORDERS", servers[0].ClientURL()))
		success, field, expected := expectMatchMap(t,
			map[string]string{
				"cluster_found": `Found cluster TEST with a balanced distribution of \d`,
				"balanced":      `Balanced \d consumers on ORDERS`,
			},
			string(output),
		)

		if !success {
			t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
		}
		return nil
	})
}

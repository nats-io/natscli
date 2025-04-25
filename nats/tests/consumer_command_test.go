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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Create a randomly named consumer. This function implies testing the 'add' subcommand
func createConsumer(mgr *jsm.Manager, t *testing.T, args ...jsm.ConsumerOption) (string, error) {
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

func expectMatchRegex(t *testing.T, found, expected string) bool {
	t.Helper()
	return !regexp.MustCompile(expected).MatchString(found)
}

func expectMatchMap(t *testing.T, fields map[string]string, output string) (bool, string, string) {
	t.Helper()
	for field, expected := range fields {
		re := regexp.MustCompile(expected)
		if !re.MatchString(string(output)) {
			return false, field, re.String()
		}
	}
	return true, "", ""
}

func TestConsumerAdd(t *testing.T) {
	srv, _, _, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	name := "ADD_TEST_CONSUMER"

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer add ORDERS %s --defaults --pull", srv.ClientURL(), name))
	success, field, expected := expectMatchMap(t,
		map[string]string{
			"header":             fmt.Sprintf("Information for Consumer ORDERS > %s created \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})", name),
			"name":               fmt.Sprintf("Name: %s", name),
			"pull_mode":          "Pull Mode: true",
			"deliver_policy":     "Deliver Policy: All",
			"ack_policy":         "Ack Policy: Explicit",
			"ack_wait":           "Ack Wait: 30.00s",
			"replay_policy":      "Replay Policy: Instant",
			"max_ack_pending":    "Max Ack Pending: 1,000",
			"max_waiting_pulls":  "Max Waiting Pulls: +512",
			"required_api_level": "Required API Level: 0 hosted at level \\d+",
			"last_delivered_msg": "Last Delivered Message: Consumer sequence: 0 Stream sequence: 0",
			"ack_floor":          "Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0",
			"outstanding_acks":   "Outstanding Acks: 0 out of maximum 1,000",
			"redelivered_msgs":   "Redelivered Messages: 0",
			"unprocessed_msgs":   "Unprocessed Messages: 0",
			"waiting_pulls":      "Waiting Pulls: 0 of maximum 512",
		},
		string(output),
	)

	if !success {
		t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
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
	rowRegex := regexp.MustCompile(`(?m)^│.*?│$`)
	found := false
	for _, line := range rowRegex.FindAllString(string(output), -1) {
		if regexp.MustCompile(name).MatchString(line) &&
			regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`).MatchString(line) &&
			regexp.MustCompile(`0.*0`).MatchString(line) &&
			regexp.MustCompile(`never`).MatchString(line) {
			found = true
			break
		}
	}
	if !found {
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
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer info ORDERS %s", srv.ClientURL(), name))
	success, field, expected := expectMatchMap(t,
		map[string]string{
			"header":             fmt.Sprintf("Information for Consumer ORDERS > %s created \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})", name),
			"name":               fmt.Sprintf("Name: %s", name),
			"pull_mode":          "Pull Mode: true",
			"deliver_policy":     "Deliver Policy: All",
			"ack_policy":         "Ack Policy: Explicit",
			"ack_wait":           "Ack Wait: 30.00s",
			"replay_policy":      "Replay Policy: Instant",
			"max_ack_pending":    "Max Ack Pending: 1,000",
			"max_waiting_pulls":  "Max Waiting Pulls: 512",
			"required_api_level": "Required API Level: 0 hosted at level \\d+",
			"last_delivered_msg": "Last Delivered Message: Consumer sequence: 0 Stream sequence: 0",
			"ack_floor":          "Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0",
			"outstanding_acks":   "Outstanding Acks: 0 out of maximum 1,000",
			"redelivered_msgs":   "Redelivered Messages: 0",
			"unprocessed_msgs":   "Unprocessed Messages: 0",
			"waiting_pulls":      "Waiting Pulls: 0 of maximum 512",
		},
		string(output),
	)

	if !success {
		t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
	}
}

func TestConsumerState(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer state ORDERS %s", srv.ClientURL(), name))
	success, field, expected := expectMatchMap(t,
		map[string]string{
			"header":             fmt.Sprintf("State for Consumer ORDERS > %s created \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})", name),
			"required_api_level": "Required API Level: 0 hosted at level \\d+",
			"last_delivered_msg": "Last Delivered Message: Consumer sequence: 0 Stream sequence: 0",
			"ack_floor":          "Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0",
			"outstanding_acks":   "Outstanding Acks: 0 out of maximum 1,000",
			"redelivered_msgs":   "Redelivered Messages: 0",
			"unprocessed_msgs":   "Unprocessed Messages: 0",
			"waiting_pulls":      "Waiting Pulls: 0 of maximum 512",
		},
		string(output),
	)

	if !success {
		t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
	}
}

func TestConsumerCopy(t *testing.T) {
	srv, _, _, name := setupConsumerTest(t)
	defer srv.Shutdown()

	output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer copy ORDERS %s COPY_1", srv.ClientURL(), name))
	success, field, expected := expectMatchMap(t,
		map[string]string{
			"header":             "Information for Consumer ORDERS > COPY_1 created \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})",
			"name":               "Name: COPY_1",
			"pull_mode":          "Pull Mode: true",
			"deliver_policy":     "Deliver Policy: All",
			"ack_policy":         "Ack Policy: Explicit",
			"ack_wait":           "Ack Wait: 30.00s",
			"replay_policy":      "Replay Policy: Instant",
			"max_ack_pending":    "Max Ack Pending: 1,000",
			"max_waiting_pulls":  "Max Waiting Pulls: 512",
			"required_api_level": "Required API Level: 0 hosted at level \\d+",
			"last_delivered_msg": "Last Delivered Message: Consumer sequence: 0 Stream sequence: 0",
			"ack_floor":          "Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0",
			"outstanding_acks":   "Outstanding Acks: 0 out of maximum 1,000",
			"redelivered_msgs":   "Redelivered Messages: 0",
			"unprocessed_msgs":   "Unprocessed Messages: 0",
			"waiting_pulls":      "Waiting Pulls: 0 of maximum 512",
		},
		string(output),
	)

	if !success {
		t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
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
	rowRegex := regexp.MustCompile(`(?m)^│.*?│$`)
	found := false

	for _, line := range rowRegex.FindAllString(string(output), -1) {
		if regexp.MustCompile(name).MatchString(line) &&
			regexp.MustCompile(`Pull`).MatchString(line) &&
			regexp.MustCompile(`Explicit`).MatchString(line) &&
			regexp.MustCompile(`30\.00s`).MatchString(line) &&
			regexp.MustCompile(`0`).FindAllString(line, -1) != nil &&
			len(regexp.MustCompile(`0`).FindAllString(line, -1)) >= 5 {
			found = true
			break
		}
	}

	if !found {
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
				"cluster_found": "Found cluster TEST with a balanced distribution of \\d",
				"balanced":      "Balanced \\d consumers on ORDERS",
			},
			string(output),
		)

		if !success {
			t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
		}
		return nil
	})
}

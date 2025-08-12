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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const defaultStreamName = "TEST_STREAM"
const defaultSubject = "TEST_STREAM.new"

// Create a randomly named consumer
func setupConsumerTest(t *testing.T, replicas int, mgr *jsm.Manager, args ...jsm.ConsumerOption) (string, error) {
	t.Helper()
	createDefaultTestStream(t, mgr, replicas)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	consumerName := fmt.Sprintf("TEST_%d", rng.Intn(1000000))
	opts := []jsm.ConsumerOption{
		jsm.DurableName(consumerName),
		jsm.AcknowledgeExplicit(),
		jsm.FilterStreamBySubject(defaultSubject),
	}
	opts = append(opts, args...)

	_, err := mgr.NewConsumer(defaultStreamName, opts...)

	if err != nil {
		return "", err
	}

	return consumerName, nil
}

func TestConsumerAdd(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		createDefaultTestStream(t, mgr, 1)
		name := "TEST_CONSUMER"

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer add %s %s --defaults --pull", srv.ClientURL(), defaultStreamName, name))
		err := expectMatchJSON(t, string(output),
			map[string]any{
				"Configuration": map[string]any{
					"Ack Policy":        "Explicit",
					"Ack Wait":          "30.00s",
					"Deliver Policy":    "All",
					"Max Ack Pending":   "1,000",
					"Max Waiting Pulls": "512",
					"Name":              name,
					"Pull Mode":         "true",
					"Replay Policy":     "Instant",
				},
				"Header": fmt.Sprintf(`Information for Consumer %s > %s created`, defaultStreamName, name),
				"State": map[string]any{
					"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
					"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
					"Outstanding Acks":       "0 out of maximum 1,000",
					"Redelivered Messages":   "0",
					"Required API Level":     "0 hosted at level \\d",
					"Unprocessed Messages":   "0",
					"Waiting Pulls":          "0 of maximum 512",
				},
			},
		)
		if err != nil {
			t.Errorf("Failed to add consumer: %s. %s", err, output)
		}
		return nil
	})
}

func TestConsumerRM(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer rm %s %s --force", srv.ClientURL(), defaultStreamName, name))

		_, err = mgr.LoadConsumer(defaultStreamName, name)
		if err == nil {
			t.Errorf("failed to delete consumer: %s", output)
		}
		return nil
	})
}

func TestConsumerEdit(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer edit %s %s --max-pending=10 -f", srv.ClientURL(), defaultStreamName, name))
		if !strings.Contains(string(output), "MaxAckPending: 1000,") {
			t.Errorf("expected old MaxAckPending line not found")
		}
		if !strings.Contains(string(output), "MaxAckPending: 10,") {
			t.Errorf("expected new MaxAckPending line not found")
		}
		return nil
	})
}

func TestConsumerLS(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer ls %s", srv.ClientURL(), defaultStreamName))

		if !expectMatchLine(t, string(output), name, `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, "0.*0", "never") {
			t.Errorf("consumer row not found in output:\n%s", output)
		}
		return nil
	})
}

func TestConsumerFind(t *testing.T) {
	t.Run("--pull", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			name, err := setupConsumerTest(t, 1, mgr)
			if err != nil {
				t.Fatal(err)
			}

			output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer find %s --pull", srv.ClientURL(), defaultStreamName))
			if !expectMatchRegex(t, name, string(output)) {
				t.Errorf("failed to find consumer %s in %s", name, string(output))
			}
			return nil
		})
	})

	t.Run("--api-level", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			_, err := mgr.NewStream("T1")
			if err != nil {
				t.Fatalf("unable to create stream: %s", err)
			}

			_, err = mgr.NewConsumer("T1", jsm.DurableName("C1"))
			if err != nil {
				t.Fatalf("unable to create consumer: %s", err)
			}
			_, err = mgr.NewConsumer("T1", jsm.DurableName("C2"), jsm.PauseUntil(time.Now()))
			if err != nil {
				t.Fatalf("unable to create consumer: %s", err)
			}

			t.Run("with 0", func(t *testing.T) {
				output := string(runNatsCli(t, fmt.Sprintf("--server='%s' consumer find T1 --api-level=0", srv.ClientURL())))
				if !(expectMatchLine(t, output, "C1") && expectMatchLine(t, output, "C2")) {
					t.Errorf("unexpected output. expected 2 consumers: %s", output)
				}
			})
			t.Run("with 1", func(t *testing.T) {
				output := string(runNatsCli(t, fmt.Sprintf("--server='%s' consumer find T1 --api-level=1", srv.ClientURL())))
				if !(expectMatchLine(t, output, "C2") && !expectMatchLine(t, output, "C1")) {
					t.Errorf("unexpected output. expected 1 consumer: %s", output)
				}
			})

			return nil
		})
	})
	t.Run("--offline", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			_, err := mgr.NewStream("T_OFFLINE")
			if err != nil {
				t.Fatalf("unable to create stream: %s", err)
			}

			_, err = mgr.NewConsumer("T_OFFLINE", jsm.DurableName("C1"), jsm.ConsumerMetadata(map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt - 1)}))
			if err != nil {
				t.Fatalf("unable to create consumer: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' consumer find T_OFFLINE --offline", srv.ClientURL())))
			if !expectMatchLine(t, output, "C1") {
				t.Errorf("unexpected output. expected 1 streams: %s", output)
			}
			return nil
		})
	})
}

func TestConsumerInfo(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer info %s %s", srv.ClientURL(), defaultStreamName, name))
		expected := map[string]any{
			"Header": fmt.Sprintf(`Information for Consumer %s > %s created .*`, defaultStreamName, name),
			"Configuration": map[string]any{
				"Name":              `TEST_\d+`,
				"Pull Mode":         "true",
				"Filter Subject":    defaultStreamName,
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
				"Required API Level":     "0 hosted at level \\d",
				"Unprocessed Messages":   "0",
				"Waiting Pulls":          "0 of maximum 512",
			},
		}

		if err := expectMatchJSON(t, string(output), expected); err != nil {
			t.Errorf("columns mismatch: %v", err)
		}
		return nil
	})
}

func TestConsumerState(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer state %s %s", srv.ClientURL(), defaultStreamName, name))
		expected := map[string]any{
			"Header": fmt.Sprintf(`State for Consumer %s > %s created .*`, defaultStreamName, name),
			"State": map[string]any{
				"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
				"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
				"Outstanding Acks":       "0 out of maximum 1,000",
				"Redelivered Messages":   "0",
				"Required API Level":     "0 hosted at level \\d",
				"Unprocessed Messages":   "0",
				"Waiting Pulls":          "0 of maximum 512",
			},
		}

		if err := expectMatchJSON(t, string(output), expected); err != nil {
			t.Errorf("columns mismatch: %v", err)
		}
		return nil
	})
}

func TestConsumerCopy(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer copy %s %s COPY_1", srv.ClientURL(), defaultStreamName, name))
		expected := map[string]any{
			"Header": fmt.Sprintf(`Information for Consumer %s > COPY_1 created`, defaultStreamName),
			"State": map[string]any{
				"Acknowledgment Floor":   "Consumer sequence: 0 Stream sequence: 0",
				"Last Delivered Message": "Consumer sequence: 0 Stream sequence: 0",
				"Outstanding Acks":       "0 out of maximum 1,000",
				"Redelivered Messages":   "0",
				"Required API Level":     "0 hosted at level \\d",
				"Unprocessed Messages":   "0",
				"Waiting Pulls":          "0 of maximum 512",
			},
		}

		if err := expectMatchJSON(t, string(output), expected); err != nil {
			t.Errorf("columns mismatch: %v", err)
		}
		return nil
	})
}

func TestConsumerNext(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}
		msgBody := "test message"

		err = nc.Publish(defaultSubject, []byte(msgBody))
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer next %s %s", srv.ClientURL(), defaultStreamName, name))

		if !expectMatchRegex(t, msgBody, string(output)) {
			t.Errorf("failed to find message body %s in %s", msgBody, string(output))
		}
		return nil
	})
}

func TestConsumerSub(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}
		msgBody := "test message"

		err = nc.Publish(defaultSubject, []byte(msgBody))
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer sub %s %s", srv.ClientURL(), defaultStreamName, name))

		if !expectMatchRegex(t, msgBody, string(output)) {
			t.Errorf("failed to find message body %s in %s", msgBody, string(output))
		}
		return nil
	})

}

// Graph command has to be run with a terminal
// func TestConsumerGraph(t *testing.T) {}

func TestConsumerPause(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer pause %s %s \"2050-04-22 11:48:55\" --force", srv.ClientURL(), defaultStreamName, name))
		expected := fmt.Sprintf("Paused %s > %s until 2050-04-22", defaultStreamName, name)
		if !expectMatchRegex(t, expected, string(output)) {
			t.Errorf("failed to pause consumer %s: %s", name, string(output))
		}
		return nil
	})
}

func TestConsumerUnpin(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		groupName := "PINNED_GROUP"
		consumerName, err := setupConsumerTest(t, 1, mgr, jsm.PinnedClientPriorityGroups(time.Duration(1*time.Minute), groupName))
		if err != nil {
			t.Fatal(err)
		}

		err = nc.Publish(defaultSubject, []byte("test"))
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
			Subject: fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.%s.%s", defaultStreamName, consumerName),
			Data:    data,
		}

		_, err = nc.RequestMsg(msg, 2*time.Second)
		if err != nil {
			t.Fatalf("request error: %v", err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer unpin %s %s %s -f", srv.ClientURL(), defaultStreamName, consumerName, groupName))

		expected := fmt.Sprintf(`Unpinned client .+ from Priority Group %s > %s > %s`, defaultStreamName, consumerName, groupName)
		if !expectMatchRegex(t, expected, string(output)) {
			t.Errorf("failed to unpin consumer %s: %s", consumerName, string(output))
		}
		return nil
	})
}

func TestConsumerResume(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		runNatsCli(t, fmt.Sprintf("--server='%s' consumer pause %s %s \"2050-04-22 11:48:55\" --force", srv.ClientURL(), defaultStreamName, name))
		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer resume %s %s --force", srv.ClientURL(), defaultStreamName, name))

		expected := fmt.Sprintf("Consumer %s > %s was resumed while previously paused until 2050-04-22", defaultStreamName, name)
		if !expectMatchRegex(t, expected, string(output)) {
			t.Errorf("failed to pause consumer %s: %s", name, string(output))
		}
		return nil
	})
}

func TestConsumerReport(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 1, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer report %s", srv.ClientURL(), defaultStreamName))

		if !expectMatchLine(t, string(output), name, "Pull", "Explicit") {
			t.Errorf("consumer row not found in output:\n%s", output)
		}
		return nil
	})
}

func TestConsumerClusterDown(t *testing.T) {
	withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name, err := setupConsumerTest(t, 3, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer cluster step-down %s %s", servers[0].ClientURL(), defaultStreamName, name))
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
		_, err := setupConsumerTest(t, 3, mgr)
		if err != nil {
			t.Fatal(err)
		}

		output := runNatsCli(t, fmt.Sprintf("--server='%s' consumer cluster balance %s", servers[0].ClientURL(), defaultStreamName))
		success, field, expected := expectMatchMap(t,
			map[string]string{
				"cluster_found": `Found cluster TEST with a balanced distribution of \d`,
				"balanced":      fmt.Sprintf(`Balanced \d consumers on %s`, defaultStreamName),
			},
			string(output),
		)

		if !success {
			t.Errorf("expected match for field %s with pattern %s in output:\n%s", field, expected, output)
		}
		return nil
	})
}

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
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/scaffold"
)

const sysUserCreds = "--user=sys --password=pass"

func setupServerTest(t *testing.T) (srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
	os.Setenv("TESTING", "true")
	srv, nc, mgr = setupJStreamTest(t)
	return srv, nc, mgr
}

func TestServerAccount(t *testing.T) {
	t.Run("info action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server account info SYS", srv.ClientURL(), sysUserCreds)))

		expected := map[string]any{
			"Details": map[string]any{
				"Complete":       "true",
				"Expired":        "false",
				"JetStream":      "false",
				"System Account": "true",
				"Tag":            "SYS",
				"Updated":        `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*`,
			},
			"Exports": map[string]any{
				"Response Type":   `Streamed|Singleton`,
				"Subject":         `\$JS.*|\$SYS.*`,
				"Tokens Required": "false",
				"Type":            "service",
			},
			"Header": `Account information for account SYS`,
			"Imports": map[string]any{
				"Sharing":  "true",
				"Subject":  `\$SYS\.REQ\.USER\.INFO from subject \$SYS\.REQ\.USER\.SYS\.INFO in account SYS`,
				"Tracking": "false",
				"Type":     "service",
			},
		}

		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("purge action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server account purge SYS --force", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Purge operation on account SYS initiated") {
			t.Errorf("Failed to start purge: %s", output)
		}
	})
}

func TestServerCheck(t *testing.T) {
	t.Run("connection action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server check connection --format=json", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "connections",
			"check_name":  "Connection",
			"ok": []any{
				`connected to nats://0\.0\.0\.0:\d+ in \d+\.\d+|rtt time \d+|round trip took \d+\.\d+s`,
			},
			"perf_data": []any{
				map[string]any{
					"name":     "connect_time",
					"value":    `0(\.\d+)?`,
					"warning":  `0(\.\d+)?`,
					"critical": `\d+`,
					"unit":     "s",
				},
				map[string]any{
					"name":     "rtt",
					"value":    `0(\.\d+)?`,
					"warning":  `0(\.\d+)?`,
					"critical": `\d+`,
					"unit":     "s",
				},
				map[string]any{
					"name":     "request_time",
					"value":    `0(\.\d+)?`,
					"warning":  `0(\.\d+)?`,
					"critical": `\d+`,
					"unit":     "s",
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("stream action", func(t *testing.T) {
		srv, _, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("TEST_STREAM", jsm.Subjects("ORDERS.*"))
		if err != nil {
			t.Fatalf("unable to create stream: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check stream --stream=TEST_STREAM --format=json", srv.ClientURL())))

		expected := map[string]any{
			"status":      "OK",
			"check_suite": "stream",
			"check_name":  "TEST_STREAM",
			"ok": []any{
				"0 sources",
			},
			"perf_data": []any{
				map[string]any{
					"name":     "sources",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
			},
		}
		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("consumer action", func(t *testing.T) {
		srv, _, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("TEST_STREAM", jsm.Subjects("TEST.*"))
		if err != nil {
			t.Fatalf("unable to create stream: %s", err)
		}

		opts := []jsm.ConsumerOption{
			jsm.DurableName("C"),
			jsm.AcknowledgeExplicit(),
			jsm.FilterStreamBySubject("TEST.new"),
		}

		_, err = mgr.NewConsumer("TEST_STREAM", opts...)
		if err != nil {
			t.Fatalf("unable to create consumser: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check consumer --stream=TEST_STREAM --consumer=C --format=json", srv.ClientURL())))

		expected := map[string]any{
			"status":      "OK",
			"check_suite": "consumer",
			"check_name":  "TEST_STREAM_C",
			"perf_data": []any{
				map[string]any{
					"name":     "ack_pending",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "pull_waiting",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "pending",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "redelivered",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
			},
		}
		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("message action", func(t *testing.T) {
		srv, nc, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("TEST_STREAM", jsm.Subjects("TEST.*"))
		if err != nil {
			t.Fatalf("unable to create stream: %s", err)
		}

		nc.Publish("TEST.in", []byte("test"))

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check message --stream=TEST_STREAM --subject=TEST.in --format=json", srv.ClientURL())))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "message",
			"check_name":  "Stream Message",
			"ok": []any{
				`Valid message on TEST_STREAM .*`,
			},
			"perf_data": []any{
				map[string]any{
					"name":     "age",
					"value":    `\d+(\.\d+)?`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "s",
				},
				map[string]any{
					"name":     "size",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "B",
				},
			},
		}

		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("meta action", func(t *testing.T) {
		withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server check meta --expect=3 --lag-critical=10 --seen-critical=10s --format=json", servers[0].ClientURL(), sysUserCreds)))
			expected := map[string]any{
				"status":      "OK",
				"check_suite": "meta",
				"check_name":  "JetStream Meta Cluster",
				"ok": []any{
					`\d+ peers led by \w+`,
				},
				"perf_data": []any{
					map[string]any{
						"name":     "peers",
						"value":    `\d+`,
						"warning":  `\d+`,
						"critical": `\d+`,
					},
					map[string]any{
						"name":     "peer_offline",
						"value":    `\d+`,
						"warning":  `\d+`,
						"critical": `\d+`,
					},
					map[string]any{
						"name":     "peer_not_current",
						"value":    `\d+`,
						"warning":  `\d+`,
						"critical": `\d+`,
					},
					map[string]any{
						"name":     "peer_inactive",
						"value":    `\d+`,
						"warning":  `\d+`,
						"critical": `\d+`,
					},
					map[string]any{
						"name":     "peer_lagged",
						"value":    `\d+`,
						"warning":  `\d+`,
						"critical": `\d+`,
					},
				},
			}
			err := expectMatchJSON(t, output, expected)
			if err != nil {
				t.Error(err)
			}

			return nil
		})
	})

	t.Run("request action", func(t *testing.T) {
		srv, _, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("TEST_STREAM", jsm.Subjects("TEST.*"))
		if err != nil {
			t.Fatalf("unable to create stream: %s", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check request --subject=TEST.in --format=json", srv.ClientURL())))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "request",
			"check_name":  "TEST.in",
			"ok": []any{
				"Valid response",
			},
			"perf_data": []any{
				map[string]any{
					"name":     "time",
					"value":    `0(\.\d+)?`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "s",
				},
			},
		}
		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("jetstream action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check jetstream --format=json", srv.ClientURL())))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "jetstream",
			"check_name":  "JetStream",
			"perf_data": []any{
				map[string]any{
					"name":     "memory",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "B",
				},
				map[string]any{
					"name":     "memory_pct",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "%",
				},
				map[string]any{
					"name":     "storage",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "B",
				},
				map[string]any{
					"name":     "storage_pct",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "%",
				},
				map[string]any{
					"name":     "streams",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "streams_pct",
					"value":    `\d+`,
					"warning":  `-?\d+`,
					"critical": `-?\d+`,
					"unit":     "%",
				},
				map[string]any{
					"name":     "consumers",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "consumers_pct",
					"value":    `\d+`,
					"warning":  `-?\d+`,
					"critical": `-?\d+`,
					"unit":     "%",
				},
				map[string]any{
					"name":     "replicas_ok",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "replicas_no_leader",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "replicas_missing_replicas",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "replicas_lagged",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "replicas_not_seen",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "s",
				},
				map[string]any{
					"name":     "replicas_fail",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("server action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server check server --name=%s --format=json", srv.ClientURL(), sysUserCreds, srv.Name())))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "server",
			"check_name":  `[A-Z0-9]`,
			"perf_data": []any{
				map[string]any{
					"name":     "uptime",
					"value":    `0(\.\d+)?`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "s",
				},
				map[string]any{
					"name":     "cpu",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "%",
				},
				map[string]any{
					"name":     "mem",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "connections",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
				map[string]any{
					"name":     "subscriptions",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("kv action", func(t *testing.T) {
		srv, nc, _ := setupServerTest(t)
		defer srv.Shutdown()

		cfg := nats.KeyValueConfig{
			Bucket: "T",
		}

		createTestBucket(t, nc, &cfg)

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check kv --bucket=T --format=json", srv.ClientURL())))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "kv",
			"check_name":  "T",
			"ok": []any{
				"bucket T",
			},
			"perf_data": []any{
				map[string]any{
					"name":     "values",
					"value":    `\d+`,
					"warning":  `-?\d+`,
					"critical": `-?\d+`,
				},
				map[string]any{
					"name":     "bytes",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "B",
				},
				map[string]any{
					"name":     "replicas",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("credential action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		creds := `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJleHAiOjE3NDY2MjIyODAsImp0aSI6IllVVUxIUVFTRDMySzJUWUVYSVM3QlpMSTZTUURMSFhLV1NEUVRER1RQVkw2VUhWR09aMlEiLCJpYXQiOjE3NDY1MzU4ODAsImlzcyI6IkFBNlZKN1FJN0tKRkdFREJGTFVaRVRYSTRCREpXNjVaVkpQRlZIMkZZRkJZUFAzSDRLTFBZN1VaIiwibmFtZSI6ImFsaWNlIiwic3ViIjoiVUNPT1RUNFpZT1VQRzRZWFVOWE01UkRPR0NXSUFUNU1ETERLRTNGQjZDS01aR0o0TkM0RUpEU0MiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19.3akG2bfQRUr4ttecc22kuLjhY6KDJ6YzBnSLEPUEqehsVE3AS_ksYzk08RNueiTc57w5tbJh7lt3dhqs9gp6Dw
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAAGQVZBGNLDSCASR67CSQRQFUCTTSWHKZESIMCUEQ2JDUDUGSZC326EE
------END USER NKEY SEED------

*************************************************************`

		tmpPath := filepath.Join(t.TempDir(), "test.creds")
		err := os.WriteFile(tmpPath, []byte(creds), 0644)
		if err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' server check credential --credential=\"%s\" --format=json", srv.ClientURL(), tmpPath)))
		expected := map[string]any{
			"status":      "OK",
			"check_suite": "credential",
			"check_name":  "Credential",
			"ok": []any{
				`expires in \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (Z|[+-]\d{4}) UTC`},
			"perf_data": []any{
				map[string]any{
					"name":     "expiry",
					"value":    `\d+`,
					"warning":  `\d+`,
					"critical": `\d+`,
					"unit":     "s",
				},
			},
		}
		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	// server check exporter blocks and can't be tested from here
	t.Run("exporter action", func(t *testing.T) {})
}

func TestServerCluster(t *testing.T) {
	t.Run("balance action", func(t *testing.T) {
		// Balance Action times out, but we have enough tests in the balancer to cover this
	})

	t.Run("step-down action", func(t *testing.T) {
		withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server cluster step-down --force", servers[0].ClientURL(), sysUserCreds)))
			if !expectMatchRegex(t, `Requesting leader step down of "s\d" in a 3 peer RAFT group`, output) ||
				!expectMatchRegex(t, `New leader elected "s\d"`, output) {
				t.Errorf("failed to step down leader: %s", output)
			}
			return nil
		})
	})

	t.Run("peer-remove action", func(t *testing.T) {
		withJSCluster(t, func(t *testing.T, servers []*server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			downId := 0
			if servers[0].JetStreamIsLeader() {
				// don't peer remove a leader
				downId = 1
			}

			// --force doesn't output anything, so we just check if the command doesn't fail
			runNatsCli(t, fmt.Sprintf("--server='%s' %s server cluster peer-remove %s --force", servers[0].ClientURL(), sysUserCreds, servers[downId].Name()))
			return nil
		})
	})
}

func TestServerConfig(t *testing.T) {
	t.Run("reload action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server config reload %s --force", srv.ClientURL(), sysUserCreds, srv.ID())))
		expected := map[string]any{
			"Connection Details": map[string]any{
				"Auth Required": `true|false`,
				"Host":          `[\d.:]+`,
				"TLS Required":  `true|false`,
			},
			"Header": `Server information for \w+ \([A-Z0-9]+\)`,
			"JetStream": map[string]any{
				"API Errors":                  `\d+`,
				"API Requests":                `\d+`,
				"API Support Level":           `\d+`,
				"Active Accounts":             `\d+`,
				"Always sync writes to disk":  `true|false`,
				"Cluster Message Compression": `true|false`,
				"Domain":                      `.*`,
				"File In Use":                 `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Ack Pending":         `unlimited|\d+`,
				"Maximum Duplicate Window":    `unlimited|\d+`,
				"Maximum File Storage":        `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum HA Assets":           `unlimited|\d+`,
				"Maximum Memory Storage":      `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Request Batch":       `unlimited|\d+`,
				"Memory In Use":               `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Storage Directory":           `.+`,
				"Strict API Parsing":          `true|false`,
				"Write sync Frequency":        `\d+[smh]+`,
			},
			"Limits": map[string]any{
				"Maximum Connections":   `[\d,]+`,
				"Maximum Payload":       `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Subscriptions": `\d+`,
				"TLS Timeout":           `[\d.]+s`,
				"Write Deadline":        `[\d.]+s`,
			},
			"Process Details": map[string]any{
				"Configuration Load Time": `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
				"Git Commit":              `.*`,
				"Go Version":              `go\d+\.\d+(\.\d+)?`,
				"Start Time":              `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
				"Uptime":                  `\d+s`,
				"Version":                 `\d+\.\d+\.\d+`,
			},
			"Statistics": map[string]any{
				"Bytes":          `\d+(\.\d+)?\s?[KMGTP]?i?B in \d+(\.\d+)?\s?[KMGTP]?i?B out`,
				"CPU Cores":      `\d+ \d+\.\d+%`,
				"Connections":    `\d+`,
				"GOMAXPROCS":     `\d+`,
				"Memory":         `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Messages":       `\d+ in \d+ out`,
				"Slow Consumers": `\d+`,
				"Subscriptions":  `\d+`,
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestServerGenerate(t *testing.T) {
	t.Run("generate action", func(t *testing.T) {
		bundle := scaffold.Bundle{
			Description:  "Test bundle for unit test",
			PreScaffold:  "",
			PostScaffold: "",
			Contact:      "test@example.com",
			Source:       "http://example.com",
			Version:      "1.0.0",
			Requires: scaffold.Requires{
				Operator: false,
			},
		}

		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()
		url := serveBundleZip(t, bundle)

		dest := filepath.Join(t.TempDir(), "test")
		runNatsCli(t, fmt.Sprintf("--server='%s' server generate \"%s\" --source \"%s\"", srv.ClientURL(), dest, url))

		configPath := filepath.Join(dest, "config.yaml")
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Fatalf("expected config.yaml to exist, but it was not found")
		}

		readmePath := filepath.Join(dest, "README.md")
		if _, err := os.Stat(readmePath); os.IsNotExist(err) {
			t.Fatalf("expected README.md to exist, but it was not found")
		}
	})
}

func TestServerGraph(t *testing.T) {
	// server action requires a terminal
	t.Run("server action", func(t *testing.T) {})

	// jetstream action requires a terminal
	t.Run("jetstream action", func(t *testing.T) {})
}

func TestServerInfo(t *testing.T) {
	t.Run("info action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server info %s ", srv.ClientURL(), sysUserCreds, srv.Name())))
		expected := map[string]any{
			"Connection Details": map[string]any{
				"Auth Required": `true|false`,
				"Host":          `[\d.:]+`,
				"TLS Required":  `true|false`,
			},
			"Header": `Server information for \w+ \([A-Z0-9]+\)`,
			"JetStream": map[string]any{
				"API Errors":                  `\d+`,
				"API Requests":                `\d+`,
				"API Support Level":           `\d+`,
				"Active Accounts":             `\d+`,
				"Always sync writes to disk":  `true|false`,
				"Cluster Message Compression": `true|false`,
				"Domain":                      `.*`,
				"File In Use":                 `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Ack Pending":         `unlimited|\d+`,
				"Maximum Duplicate Window":    `unlimited|\d+`,
				"Maximum File Storage":        `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum HA Assets":           `unlimited|\d+`,
				"Maximum Memory Storage":      `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Request Batch":       `unlimited|\d+`,
				"Memory In Use":               `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Storage Directory":           `.+`,
				"Strict API Parsing":          `true|false`,
				"Write sync Frequency":        `\d+[smh]+`,
			},
			"Limits": map[string]any{
				"Maximum Connections":   `[\d,]+`,
				"Maximum Payload":       `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Maximum Subscriptions": `\d+`,
				"TLS Timeout":           `[\d.]+s`,
				"Write Deadline":        `[\d.]+s`,
			},
			"Process Details": map[string]any{
				"Configuration Load Time": `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
				"Git Commit":              `.*`,
				"Go Version":              `go\d+\.\d+(\.\d+)?`,
				"Start Time":              `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
				"Uptime":                  `.*`,
				"Version":                 `\d+\.\d+\.\d+`,
			},
			"Statistics": map[string]any{
				"Bytes":          `\d+(\.\d+)?\s?[KMGTP]?i?B in \d+(\.\d+)?\s?[KMGTP]?i?B out`,
				"CPU Cores":      `\d+ \d+\.\d+%`,
				"Connections":    `\d+`,
				"GOMAXPROCS":     `\d+`,
				"Memory":         `\d+(\.\d+)?\s?[KMGTP]?i?B`,
				"Messages":       `\d+ in \d+ out`,
				"Slow Consumers": `\d+`,
				"Subscriptions":  `\d+`,
			},
		}

		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestServerList(t *testing.T) {
	t.Run("list action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server list 1 --json", srv.ClientURL(), sysUserCreds)))
		expected := []any{
			map[string]any{
				"server": map[string]any{
					"name":      `TEST_SERVER`,
					"host":      `0.0.0.0`,
					"id":        `[A-Z0-9]+`,
					"ver":       `\d+\.\d+\.\d+`,
					"jetstream": `true`,
					"flags":     `\d+`,
					"seq":       `\d+`,
					"time":      `\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z`,
				},
				"statsz": map[string]any{
					"start":             `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(Z|[+-]\d{2}:\d{2})$`,
					"mem":               `\d+`,
					"cores":             `\d+`,
					"cpu":               `\d+`,
					"connections":       `\d+`,
					"total_connections": `\d+`,
					"active_accounts":   `\d+`,
					"subscriptions":     `\d+`,
					"sent": map[string]any{
						"msgs":  `\d+`,
						"bytes": `\d+`,
					},
					"received": map[string]any{
						"msgs":  `\d+`,
						"bytes": `\d+`,
					},
					"slow_consumers": `\d+`,
					"active_servers": `\d+`,
					"jetstream": map[string]any{
						"config": map[string]any{
							"max_memory":    `\d+`,
							"max_storage":   `\d+`,
							"sync_interval": `\d+`,
						},
						"stats": map[string]any{
							"memory":           `\d+`,
							"storage":          `\d+`,
							"reserved_memory":  `\d+`,
							"reserved_storage": `\d+`,
							"accounts":         `\d+`,
							"ha_assets":        `\d+`,
							"api": map[string]any{
								"level":  `\d+`,
								"total":  `\d+`,
								"errors": `\d+`,
							},
						},
						"limits": map[string]any{},
					},
					"gomaxprocs": `\d+`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestServerMappings(t *testing.T) {
	t.Run("mappings action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server mappings TEST.* TEST.a.{{wildcard(1)}} TEST.local", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "TEST.a.local") {
			t.Errorf("failed to test mapping: %s", output)
		}
	})
}

func TestServerPasswd(t *testing.T) {
	t.Run("passwd action with ENV var", func(t *testing.T) {
		os.Setenv("PASSWORD", "abcdefghijklmnopqrstuvwxyz")
		defer os.Unsetenv("PASSWORD")
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server passwd", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `\$2[aby]?\$\d{2}\$[./A-Za-z0-9]{53}`) {
			t.Errorf("failed to generate password hash: %s", output)
		}
	})

	t.Run("passwd action with --generate", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server passwd --generate", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `^Generated password: [a-zA-Z0-9@#_\-%\^&()]+$`) ||
			!expectMatchLine(t, output, `^\s*bcrypt hash: \$2[aby]?\$\d{2}\$[./A-Za-z0-9]{53}$`) {
			t.Errorf("failed to generate passwd: %s", output)
		}
	})
}

func TestServerPing(t *testing.T) {
	t.Run("ping action", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server ping 1", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `^TEST_SERVER\s+rtt=\d+\.\d+.+$`) {
			t.Errorf("failed to ping: %s", output)
		}
	})
}

func TestServerReport(t *testing.T) {
	t.Run("accounts command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report accounts --json", srv.ClientURL(), sysUserCreds)))
		expected := []any{
			map[string]any{
				"account":     "SYS",
				"connections": `\d+`,
				"connection_info": []any{
					map[string]any{
						"cid":             `\d+`,
						"kind":            "Client",
						"type":            "nats",
						"ip":              `\d+\.\d+\.\d+\.\d+`,
						"port":            `\d+`,
						"start":           `.*`,
						"last_activity":   `.*`,
						"rtt":             `\d+`,
						"uptime":          `.*`,
						"idle":            `.*`,
						"pending_bytes":   `\d+`,
						"in_msgs":         `\d+`,
						"out_msgs":        `\d+`,
						"in_bytes":        `\d+`,
						"out_bytes":       `\d+`,
						"subscriptions":   `\d+`,
						"name":            `.*`,
						"lang":            "go",
						"version":         `.*`,
						"authorized_user": `.*`,
						"account":         "SYS",
						"subscriptions_list": []any{
							`.*`,
						},
						"name_tag": `.*`,
						"server": map[string]any{
							"name":      "TEST_SERVER",
							"host":      `0\.0\.0\.0`,
							"id":        `[A-Z0-9]{52}`,
							"ver":       `.*`,
							"jetstream": true,
							"flags":     `\d+`,
							"seq":       `\d+`,
							"time":      `.*`,
						},
					},
				},
				"in_msgs":       `\d+`,
				"out_msgs":      `\d+`,
				"in_bytes":      `\d+`,
				"out_bytes":     `\d+`,
				"subscriptions": `\d+`,
				"server": []any{
					map[string]any{
						"name":      "TEST_SERVER",
						"host":      `0\.0\.0\.0`,
						"id":        `[A-Z0-9]{52}`,
						"ver":       `.*`,
						"jetstream": true,
						"flags":     `\d+`,
						"seq":       `\d+`,
						"time":      `.*`,
					},
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}

	})
	t.Run("connections command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report connections --json", srv.ClientURL(), sysUserCreds)))
		expected := []any{
			map[string]any{
				"cid":             `\d+`,
				"kind":            `Client`,
				"type":            `nats`,
				"ip":              `127\.0\.0\.1`,
				"port":            `\d+`,
				"rtt":             `\d+`,
				"uptime":          `\d+s?`,
				"idle":            `\d+s?`,
				"in_msgs":         `\d+`,
				"out_msgs":        `\d+`,
				"in_bytes":        `\d+`,
				"out_bytes":       `\d+`,
				"subscriptions":   `\d+`,
				"lang":            `go`,
				"version":         `1\.42\.0`,
				"authorized_user": `.+`,
				"subscriptions_list": []any{
					`_INBOX\..+`,
				},
				"server": map[string]any{
					"name":      `TEST_SERVER`,
					"host":      `0\.0\.0\.0`,
					"id":        `[A-Z0-9]{52}`,
					"ver":       `\d\.\d+\.\d`,
					"jetstream": `true`,
					"flags":     `\d+`,
					"seq":       `\d+`,
					"time":      `.+`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("cpu command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report cpu --json", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"TEST_SERVER": `\d+`,
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("gateways command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report gateways", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Server", "Name", "Port", "Kind", "Connection", "ID", "Uptime", "RTT", "Bytes", "Accounts") ||
			!expectMatchLine(t, output, "TEST_SERVER") {
			t.Errorf("failed gateways: %s", output)
		}
	})
	t.Run("health command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report health", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Server", "Cluster", "Domain", "Status", "Type", "Error") ||
			!expectMatchLine(t, output, "TEST_SERVER", `ok \(200\)`) {
			t.Errorf("failed health: %s", output)
		}
	})
	t.Run("jetstream command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report jetstream", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Server", "Cluster", "Streams", "Consumers", "Messages", "Bytes", "Memory", "File", "API Req", "API Err", "Pending") ||
			!expectMatchLine(t, output, "TEST_SERVER") {
			t.Errorf("failed health: %s", output)
		}
	})
	t.Run("leafnodes command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report leafnodes", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Server", "Name", "Account", "Address", "RTT", "Msgs In", "Msgs Out", "Bytes Out", "Subs", "Compressed", "Spoke") {
			t.Errorf("failed health: %s", output)
		}
	})
	t.Run("mem command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report mem", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `^TEST_SERVER: .+ \(\d+(\.\d+)?\s?[KMGTP]?i?B\)$`) {
			t.Errorf("failed health: %s", output)
		}
	})
	t.Run("routes command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server report routes", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Server", "Cluster", "Name", "Account", "Address", "ID", "Uptime", "RTT", "Subs", "Bytes In", "Bytes Out") {
			t.Errorf("failed health: %s", output)
		}
	})

}

func TestServerRequest(t *testing.T) {
	t.Run("accounts command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request accounts", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      `TEST_SERVER`,
				"host":      `0\.0\.0\.0`,
				"id":        `[A-Z0-9]{52}`,
				"ver":       `2\.11\.3`,
				"jetstream": `true`,
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id":      `[A-Z0-9]{52}`,
				"now":            `.+`,
				"system_account": `SYS`,
				"accounts": []any{
					`SYS`,
					`\$G`,
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("connections command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request connections", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      `TEST_SERVER`,
				"host":      `0\.0\.0\.0`,
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": `true`,
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id":       `[A-Z0-9]{52}`,
				"now":             `.+`,
				"num_connections": `\d+`,
				"total":           `\d+`,
				"offset":          `\d+`,
				"limit":           `\d+`,
				"connections": []any{
					map[string]any{
						"cid":             `\d+`,
						"kind":            `Client`,
						"type":            `nats`,
						"ip":              `127\.0\.0\.1`,
						"port":            `\d+`,
						"start":           `.+`,
						"last_activity":   `.+`,
						"rtt":             `.+`,
						"uptime":          `.+`,
						"idle":            `.+`,
						"pending_bytes":   `\d+`,
						"in_msgs":         `\d+`,
						"out_msgs":        `\d+`,
						"in_bytes":        `\d+`,
						"out_bytes":       `\d+`,
						"subscriptions":   `\d+`,
						"name":            `NATS CLI .*`,
						"lang":            `go`,
						"version":         `\d+\.\d+\.\d+`,
						"authorized_user": `sys`,
						"account":         `SYS`,
						"name_tag":        `SYS`,
					},
				},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("gateways command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request gateways", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`, // variable version
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id":         `[A-Z0-9]{52}`,
				"now":               `.+`,
				"outbound_gateways": map[string]any{},
				"inbound_gateways":  map[string]any{},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("jetstream-health command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request jetstream-health", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"status":      "ok",
				"status_code": `\d+`,
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("jetstream command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request jetstream", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"memory":           `\d+`,
				"storage":          `\d+`,
				"reserved_memory":  `\d+`,
				"reserved_storage": `\d+`,
				"accounts":         `\d+`,
				"ha_assets":        `\d+`,
				"api": map[string]any{
					"level":  `\d+`,
					"total":  `\d+`,
					"errors": `\d+`,
				},
				"server_id": `[A-Z0-9]{52}`,
				"now":       `.+`,
				"config": map[string]any{
					"max_memory":    `\d+`,
					"max_storage":   `\d+`,
					"store_dir":     `.*/|\\jetstream`,
					"sync_interval": `\d+`,
				},
				"limits":    map[string]any{},
				"streams":   `\d+`,
				"consumers": `\d+`,
				"messages":  `\d+`,
				"bytes":     `\d+`,
				"total":     `\d+`,
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("kick command", func(t *testing.T) {
		srv, nc, _ := setupServerTest(t)
		defer srv.Shutdown()

		_, err := nats.Connect(srv.ClientURL(), nats.UserInfo("sys", "pass"))
		if err != nil {
			t.Fatalf("Error connecting to kNATS: %v", err)
		}

		resp := &server.ServerAPIConnzResponse{}
		data, err := nc.Request("$SYS.REQ.SERVER.PING.CONNZ", nil, time.Second)
		if err != nil {
			log.Fatal(err)
		}
		json.Unmarshal(data.Data, resp)

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request kick %d %s", srv.ClientURL(), sysUserCreds, resp.Data.Conns[0].Cid, srv.ID())))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
		}
		err = expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("leafnodes command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request leafnodes", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id": `[A-Z0-9]{52}`,
				"now":       `.+`,
				"leafnodes": `\d+`,
				"leafs":     []any{},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("profile command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()
		defer deleteProfileFile(t, ".")

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request profile mutex", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `Server "TEST_SERVER" profile written: mutex-\d{8}-\d{6}-TEST_SERVER`) {
			t.Errorf("failed to write profile: %s", output)
		}
	})

	t.Run("routes command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request routes", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id":   `[A-Z0-9]{52}`,
				"server_name": "TEST_SERVER",
				"now":         `.+`,
				"num_routes":  `\d+`,
				"routes":      []any{},
			},
		}
		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("subscriptions command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server request subscriptions", srv.ClientURL(), sysUserCreds)))
		expected := map[string]any{
			"server": map[string]any{
				"name":      "TEST_SERVER",
				"host":      "0.0.0.0",
				"id":        `[A-Z0-9]{52}`,
				"ver":       `\d+\.\d+\.\d+`,
				"jetstream": "true",
				"flags":     `\d+`,
				"seq":       `\d+`,
				"time":      `.+`,
			},
			"data": map[string]any{
				"server_id":         `[A-Z0-9]{52}`,
				"now":               `.+`,
				"num_subscriptions": `\d+`,
				"num_cache":         `\d+`,
				"num_inserts":       `\d+`,
				"num_removes":       `\d+`,
				"num_matches":       `\d+`,
				"cache_hit_rate":    `0(\.\d+)?|1(\.0+)?`,
				"max_fanout":        `\d+`,
				"avg_fanout":        `\d+(\.\d+)?`,
				"total":             `\d+`,
				"offset":            `\d+`,
				"limit":             `\d+`,
			},
		}

		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("variables command", func(t *testing.T) {
		srv, _, _ := setupServerTest(t)
		defer srv.Shutdown()

		runNatsCli(t, fmt.Sprintf("--server='%s' %s server request variables", srv.ClientURL(), sysUserCreds))
		// Don't check the output here and just check for a successful run. It's likely the output here will be
		// too variable to rely on.
	})
}

func TestServerRun(t *testing.T) {
	// This times out in tests.
}

func TestServerWatch(t *testing.T) {
	// Live view that requires a terminal
}

func TestServerStreamCheck(t *testing.T) {
	t.Run("stream-check command", func(t *testing.T) {
		srv, _, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("CHECK_STREAM", jsm.Subjects("TEST.*"))
		if err != nil {
			t.Error("Unable to creat new stream")
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server stream-check", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, "Servers: 1") ||
			!expectMatchLine(t, output, "Streams: 1") ||
			!expectMatchLine(t, output, "Stream Replica", "Raft", "Account", "Account ID", "Node", "Messages", "Bytes", "Subjects", "Deleted", "Consumers", "First", "Last", "Status", "Leader", "Peers") ||
			!expectMatchLine(t, output, "CHECK_STREAM") {
			t.Errorf("failed stream-check: %s", output)
		}
	})
}

func TestServerConsumerCheck(t *testing.T) {
	t.Run("consumer-check command", func(t *testing.T) {
		srv, _, mgr := setupServerTest(t)
		defer srv.Shutdown()

		_, err := mgr.NewStream("CONSUMER_CHECK_STREAM", jsm.Subjects("TEST.*"))
		if err != nil {
			t.Error("Unable to creat new stream")
		}

		opts := []jsm.ConsumerOption{
			jsm.DurableName("CONSUMER_CHECK_CONSUMER"),
			jsm.AcknowledgeExplicit(),
			jsm.FilterStreamBySubject("TEST.new"),
		}

		_, err = mgr.NewConsumer("CONSUMER_CHECK_STREAM", opts...)
		if err != nil {
			t.Error("Unable to creat new consumer")
		}

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s server consumer-check", srv.ClientURL(), sysUserCreds)))
		if !expectMatchLine(t, output, `^Servers: 1`) ||
			!expectMatchLine(t, output, `^Consumers: 1`) ||
			!expectMatchLine(t, output, `Consumer`, `Stream`, `Raft`, `Account`, `Account ID`, `Node`, `Delivered`, `ACK Floor`, `Counters`, `Status`, `Leader`, `Stream Cluster Leader`, `Peers`) ||
			!expectMatchLine(t, output, `CONSUMER_CHECK_CONSUMER`, `CONSUMER_CHECK_STREAM`) {
			t.Errorf("failed consumer-check: %s", output)
		}
	})
}

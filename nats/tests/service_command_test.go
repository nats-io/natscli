// Copyright 2026 The NATS Authors
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
	"maps"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func setupTestService(t *testing.T, nc *nats.Conn, name string, endpoints []string, handler micro.HandlerFunc) micro.Service {
	t.Helper()

	svc, err := micro.AddService(nc, micro.Config{
		Name:    name,
		Version: "1.0.0",
	})
	if err != nil {
		t.Fatalf("failed to add service: %v", err)
	}

	for _, ep := range endpoints {
		if err := svc.AddEndpoint(ep, handler); err != nil {
			t.Fatalf("failed to add service endpoint %q: %v", ep, err)
		}
	}

	return svc
}

func TestServiceInfo(t *testing.T) {
	srv, _, _ := setupJStreamTest(t)
	defer srv.Shutdown()
	nc, _, _ := prepareHelper(srv.ClientURL())

	svc := setupTestService(t, nc, "svc", []string{"ep1", "ep2"}, func(req micro.Request) {
		req.Respond(nil)
	})
	defer svc.Stop()

	t.Run("Info", func(t *testing.T) {
		output := runNatsCli(t, fmt.Sprintf("--server='%s' service info svc --json", srv.ClientURL()))

		var resp struct {
			Info  *micro.Info  `json:"info"`
			Stats *micro.Stats `json:"stats"`
		}
		err := json.Unmarshal(output, &resp)
		if err != nil {
			t.Errorf("failed to unmarshal response: %v", err)
		}

		if resp.Info.Name != "svc" {
			t.Errorf("expected service name to be svc, got %s", resp.Info.Name)
		}

		if resp.Info.Endpoints[0].Name != "ep1" {
			t.Errorf("expected endpoint name to be ep1, got %s", resp.Info.Endpoints[0].Name)
		}

		if resp.Info.Endpoints[1].Name != "ep2" {
			t.Errorf("expected endpoint name to be ep2, got %s", resp.Info.Endpoints[1].Name)
		}
	})

	t.Run("Info with endpoint filter", func(t *testing.T) {
		output := runNatsCli(t, fmt.Sprintf("--server='%s' service info svc --json --endpoint='.*2'", srv.ClientURL()))

		var resp struct {
			Info  *micro.Info  `json:"info"`
			Stats *micro.Stats `json:"stats"`
		}
		err := json.Unmarshal(output, &resp)
		if err != nil {
			t.Errorf("failed to unmarshal response: %v", err)
		}

		if resp.Info.Name != "svc" {
			t.Errorf("expected service name to be svc, got %s", resp.Info.Name)
		}

		if len(resp.Info.Endpoints) != 1 {
			t.Errorf("expected 1 endpoint, got %d", len(resp.Info.Endpoints))
		}

		if resp.Info.Endpoints[0].Name != "ep2" {
			t.Errorf("expected endpoint name to be ep2, got %s", resp.Info.Endpoints[0].Name)
		}
	})
}

func TestServiceRequest(t *testing.T) {
	t.Run("Request with body argument", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				req.Respond([]byte("echo: " + string(req.Data())))
			})
			defer svc.Stop()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'test request'", srv.ClientURL()))

			if !strings.Contains(string(output), "echo: test request") {
				t.Errorf("expected response with echoed data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request from stdin via --force-stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				req.Respond([]byte("received: " + string(req.Data())))
			})
			defer svc.Stop()
			nc.Flush()

			output, _ := runNatsCliWithInput(t, "stdin payload", fmt.Sprintf("--server='%s' service request --force-stdin svc echo", srv.ClientURL()))

			if !strings.Contains(string(output), "received: stdin payload") {
				t.Errorf("expected response with stdin data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request to unknown endpoint fails", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				req.Respond(nil)
			})
			defer svc.Stop()
			nc.Flush()

			_, err := runNatsCliWithInput(t, "", fmt.Sprintf("--server='%s' service request svc bogus 'x'", srv.ClientURL()))
			if err == nil {
				t.Fatal("expected an error, got none")
			}
			msg := err.Error()
			if !strings.Contains(msg, "no endpoint \"bogus\"") {
				t.Errorf("expected error to mention the unknown endpoint, got: %s", msg)
			}
			if !strings.Contains(msg, "available: echo") {
				t.Errorf("expected error to list the available endpoints, got: %s", msg)
			}
			return nil
		})
	})

	t.Run("Request to unknown service fails", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			_, err := runNatsCliWithInput(t, "", fmt.Sprintf("--server='%s' service request nosuchservice echo 'x'", srv.ClientURL()))
			if err == nil {
				t.Fatal("expected an error, got none")
			}
			if !strings.Contains(err.Error(), "no service") {
				t.Errorf("expected error to mention missing service, got: %s", err.Error())
			}
			return nil
		})
	})

	t.Run("Request with no payload sends an empty body", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedLen int

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedLen = len(req.Data())
				req.Respond([]byte("ok"))
			})
			defer svc.Stop()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo", srv.ClientURL()))

			if receivedLen != 0 {
				t.Errorf("expected empty request body, got %d bytes", receivedLen)
			}
			if !strings.Contains(string(output), "ok") {
				t.Errorf("expected response 'ok', got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request --send-on newline from stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedRequests []string
			expected := []string{"request1", "request2", "request3"}

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedRequests = append(receivedRequests, string(req.Data()))
				req.Respond([]byte("response: " + string(req.Data())))
			})
			defer svc.Stop()
			nc.Flush()

			input := strings.Join(expected, "\n") + "\n"
			output, _ := runNatsCliWithInput(t, input, fmt.Sprintf("--server='%s' service request --force-stdin --send-on newline svc echo", srv.ClientURL()))

			if len(receivedRequests) != 3 {
				t.Errorf("expected 3 requests but received %d", len(receivedRequests))
			}

			for i, req := range expected {
				if i < len(receivedRequests) && receivedRequests[i] != req {
					t.Errorf("expected request(%d) %q got %q", i, req, receivedRequests[i])
				}
			}

			expectedResponses := []string{"response: request1", "response: request2", "response: request3"}
			for _, expectedResp := range expectedResponses {
				if !strings.Contains(string(output), expectedResp) {
					t.Errorf("expected output to contain %q, got: %s", expectedResp, output)
				}
			}
			return nil
		})
	})

	t.Run("Request with --raw", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				req.Respond([]byte("raw response"))
			})
			defer svc.Stop()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'test' --raw", srv.ClientURL()))

			if !strings.Contains(string(output), "raw response") {
				t.Errorf("expected raw response, got: %s", output)
			}
			if strings.Contains(string(output), "Received with rtt") {
				t.Errorf("expected no metadata with --raw flag, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request with --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedRequests []string

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedRequests = append(receivedRequests, string(req.Data()))
				req.Respond([]byte("response"))
			})
			defer svc.Stop()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'test request' --count 3", srv.ClientURL()))

			if len(receivedRequests) != 3 {
				t.Errorf("expected 3 requests but received %d", len(receivedRequests))
			}
			return nil
		})
	})

	t.Run("Request with --header flags", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedHeaders nats.Header

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedHeaders = nats.Header{}
				maps.Copy(receivedHeaders, req.Headers())
				req.Respond([]byte("response"))
			})
			defer svc.Stop()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'test with headers' -H 'X-Test:value1' --header 'X-Custom:value2'", srv.ClientURL()))

			if receivedHeaders == nil {
				t.Fatal("no request received")
			}
			if receivedHeaders.Get("X-Test") != "value1" {
				t.Errorf("expected header X-Test:value1 got %q", receivedHeaders.Get("X-Test"))
			}
			if receivedHeaders.Get("X-Custom") != "value2" {
				t.Errorf("expected header X-Custom:value2 got %q", receivedHeaders.Get("X-Custom"))
			}
			return nil
		})
	})

	t.Run("Request with --translate", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				req.Respond([]byte("hello world"))
			})
			defer svc.Stop()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'test' --translate 'tr a-z A-Z'", srv.ClientURL()))

			if !strings.Contains(string(output), "HELLO WORLD") {
				t.Errorf("expected translated output 'HELLO WORLD', got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request with templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedMsg string

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedMsg = string(req.Data())
				req.Respond([]byte("response"))
			})
			defer svc.Stop()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'Count: {{ Count }}'", srv.ClientURL()))

			expected := "Count: 1"
			if receivedMsg != expected {
				t.Errorf("expected request body %q got %q", expected, receivedMsg)
			}
			return nil
		})
	})

	t.Run("Request with --no-templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedMsg string

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedMsg = string(req.Data())
				req.Respond([]byte("response"))
			})
			defer svc.Stop()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'Count: {{ Count }}' --no-templates", srv.ClientURL()))

			expected := "Count: {{ Count }}"
			if receivedMsg != expected {
				t.Errorf("expected request body %q got %q", expected, receivedMsg)
			}
			return nil
		})
	})

	t.Run("Request with templates and --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var receivedRequests []string

			svc := setupTestService(t, nc, "svc", []string{"echo"}, func(req micro.Request) {
				receivedRequests = append(receivedRequests, string(req.Data()))
				req.Respond([]byte("response"))
			})
			defer svc.Stop()
			nc.Flush()

			count := 3
			runNatsCli(t, fmt.Sprintf("--server='%s' service request svc echo 'Request {{ Count }}' --count %d", srv.ClientURL(), count))

			if len(receivedRequests) != count {
				t.Errorf("expected %d requests but received %d", count, len(receivedRequests))
			}

			for i := range count {
				expected := fmt.Sprintf("Request %d", i+1)
				if i < len(receivedRequests) && receivedRequests[i] != expected {
					t.Errorf("expected request[%d] %q got %q", i, expected, receivedRequests[i])
				}
			}
			return nil
		})
	})
}

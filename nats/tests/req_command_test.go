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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestCLIRequestSendOn(t *testing.T) {
	t.Run("Request with body argument", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-body"
			expected := "test request"

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("echo: " + string(m.Data)))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s '%s'", srv.ClientURL(), subject, expected))

			if !strings.Contains(string(output), "echo: test request") {
				t.Errorf("expected response with echoed data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request --send-on eof from stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-stdin"
			expected := "stdin payload"

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("received: " + string(m.Data)))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output, _ := runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' request --force-stdin %s", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "received: stdin payload") {
				t.Errorf("expected response with stdin data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request --send-on newline from stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-newline"
			var receivedRequests []string
			expected := []string{"request1", "request2", "request3"}

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedRequests = append(receivedRequests, string(m.Data))
				m.Respond([]byte("response: " + string(m.Data)))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			input := strings.Join(expected, "\n") + "\n"
			output, _ := runNatsCliWithInput(t, input, fmt.Sprintf("--server='%s' request --force-stdin --send-on newline %s", srv.ClientURL(), subject))

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
}

func TestCLIRequestRaw(t *testing.T) {
	t.Run("Request with --raw", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-raw"

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("raw response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --raw", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "raw response") {
				t.Errorf("expected raw response, got: %s", output)
			}

			if strings.Contains(string(output), "Received with rtt") {
				t.Errorf("expected no metadata with --raw flag, got: %s", output)
			}

			return nil
		})
	})
}

func TestCLIRequestReplies(t *testing.T) {
	t.Run("Request with --replies 2", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-multi"
			replyCount := 0

			sub1, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
			})
			defer sub1.Unsubscribe()

			sub2, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
			})
			defer sub2.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --replies 2", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "reply 1") || !strings.Contains(string(output), "reply 2") {
				t.Errorf("expected both replies, got: %s", output)
			}

			return nil
		})
	})

	t.Run("Request with --replies 0", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-unbounded"
			replyCount := 0

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				if replyCount <= 3 {
					m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
				}
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --replies 0", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "reply 1") {
				t.Errorf("expected at least reply 1, got: %s", output)
			}

			return nil
		})
	})
}

func TestCLIRequestCount(t *testing.T) {
	t.Run("Request with --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-count"
			var receivedRequests []string

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedRequests = append(receivedRequests, string(m.Data))
				m.Respond([]byte("response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			body := "test request"
			runNatsCli(t, fmt.Sprintf("--server='%s' request %s '%s' --count 3", srv.ClientURL(), subject, body))

			if len(receivedRequests) != 3 {
				t.Errorf("expected 3 requests but received %d", len(receivedRequests))
			}

			return nil
		})
	})
}

func TestCLIRequestReplyTimeout(t *testing.T) {
	t.Run("Request with --reply-timeout flag", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-timeout"
			replyCount := 0

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --replies 2 --reply-timeout 50ms", srv.ClientURL(), subject))

			if replyCount < 1 {
				t.Errorf("expected at least 1 reply, got %d", replyCount)
			}

			return nil
		})
	})
}

func TestCLIRequestHeaders(t *testing.T) {
	t.Run("Request with --header flags", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-headers"
			var receivedMsg *nats.Msg

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedMsg = m
				m.Respond([]byte("response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			body := "test with headers"
			runNatsCli(t, fmt.Sprintf("--server='%s' request %s '%s' -H 'X-Test:value1' --header 'X-Custom:value2'", srv.ClientURL(), subject, body))

			if receivedMsg == nil {
				t.Fatal("no request received")
			}
			if receivedMsg.Header.Get("X-Test") != "value1" {
				t.Errorf("expected header X-Test:value1 got %q", receivedMsg.Header.Get("X-Test"))
			}
			if receivedMsg.Header.Get("X-Custom") != "value2" {
				t.Errorf("expected header X-Custom:value2 got %q", receivedMsg.Header.Get("X-Custom"))
			}

			return nil
		})
	})
}

func TestCLIRequestTranslate(t *testing.T) {
	t.Run("Request with --translate flag", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-translate"

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("hello world"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --translate 'tr a-z A-Z'", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "HELLO WORLD") {
				t.Errorf("expected translated output 'HELLO WORLD', got: %s", output)
			}

			return nil
		})
	})
}

func TestCLIRequestTemplates(t *testing.T) {
	t.Run("Request with templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-templates"
			var receivedMsg string

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedMsg = string(m.Data)
				m.Respond([]byte("response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'Count: {{ Count }}'", srv.ClientURL(), subject))

			expected := "Count: 1"
			if receivedMsg != expected {
				t.Errorf("expected request body %q got %q", expected, receivedMsg)
			}

			return nil
		})
	})

	t.Run("Request with --no-templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-no-templates"
			var receivedMsg string

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedMsg = string(m.Data)
				m.Respond([]byte("response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'Count: {{ Count }}' --no-templates", srv.ClientURL(), subject))

			expected := "Count: {{ Count }}"
			if receivedMsg != expected {
				t.Errorf("expected request body %q got %q", expected, receivedMsg)
			}

			return nil
		})
	})

	t.Run("Request with templates and --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-templates-count"
			var receivedRequests []string

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedRequests = append(receivedRequests, string(m.Data))
				m.Respond([]byte("response"))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			count := 3
			runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'Request {{ Count }}' --count %d", srv.ClientURL(), subject, count))

			if len(receivedRequests) != count {
				t.Errorf("expected %d requests but received %d", count, len(receivedRequests))
			}

			for i := range count {
				expected := fmt.Sprintf("Request %d", i+1)
				if receivedRequests[i] != expected {
					t.Errorf("expected request[%d] %q got %q", i, expected, receivedRequests[i])
				}
			}

			return nil
		})
	})
}

func TestCLIRequestWaitForEmpty(t *testing.T) {
	t.Run("Request with --wait-for-empty terminates on empty reply", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-wait-empty"
			var reqCount atomic.Int32

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				n := reqCount.Add(1)
				if n <= 2 {
					m.Respond([]byte(fmt.Sprintf("reply %d", n)))
				} else {
					m.Respond(nil)
				}
			})
			defer sub.Unsubscribe()
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s 'test' --wait-for-empty --count 3", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "reply 1") {
				t.Errorf("expected output to contain 'reply 1', got: %s", output)
			}
			if !strings.Contains(string(output), "reply 2") {
				t.Errorf("expected output to contain 'reply 2', got: %s", output)
			}

			return nil
		})
	})
}

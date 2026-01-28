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
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Publish Command Tests

func TestCLIPubSendOnNewline(t *testing.T) {
	t.Run("Publish with body argument", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-body"
			var messages []string
			expected := "Test Message"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' pub --send-on newline %s '%s'", srv.ClientURL(), subject, expected))

			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if len(messages) > 0 && messages[0] != expected {
				t.Errorf("expected message %q got %q", expected, messages[0])
			}
			return nil
		})
	})

	t.Run("Publish with body argument and --quiet", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-quiet"
			var messages []string
			expected := "Test Message"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			out := runNatsCli(t, fmt.Sprintf("--server='%s' pub --send-on newline -q %s '%s'", srv.ClientURL(), subject, expected))

			if len(out) != 0 {
				t.Errorf("expected cli output to be 0 but got %d\n %s", len(out), out)
			}
			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if len(messages) > 0 && messages[0] != expected {
				t.Errorf("expected message %q got %q", expected, messages[0])
			}
			return nil
		})
	})

	t.Run("Publish --send-on newline from stdin", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-sendon"
			var messages []string
			expected := []string{"test", "pub", "input"}
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			// --force-stdin required for testing as a terminal is not present
			runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --send-on newline --force-stdin %s", srv.ClientURL(), subject))

			if len(messages) != len(expected) {
				t.Errorf("expected %d messages and received %d", len(expected), len(messages))
			}
			for i, msg := range messages {
				if messages[i] != expected[i] {
					t.Errorf("expected message(%d) %q got %q", i, expected[i], msg)
				}
			}
			return nil
		})
	})

	t.Run("Publish --send-on eof from stdin", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-eof"
			var messages []string
			expected := "test\npub\ninput"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			// --force-stdin required for testing as a terminal is not present
			runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' pub --force-stdin %s", srv.ClientURL(), subject))

			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if len(messages) > 0 && messages[0] != expected {
				t.Errorf("expected message %q got %q", expected, messages[0])
			}
			return nil
		})
	})
}

func TestCLIPubTemplates(t *testing.T) {
	t.Run("Publish --templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-templates"
			var messages []string
			expected := "Count: 1"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s \"Count: {{ Count }}\"", srv.ClientURL(), subject))

			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if len(messages) > 0 && messages[0] != expected {
				t.Errorf("expected message %q got %q", expected, messages[0])
			}
			return nil
		})
	})

	t.Run("Publish --no-templates", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-no-templates"
			var messages []string
			expected := "Count: {{ Count }}"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s \"Count: {{ Count }}\" --no-templates", srv.ClientURL(), subject))

			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if len(messages) > 0 && messages[0] != expected {
				t.Errorf("expected message %q got %q", expected, messages[0])
			}
			return nil
		})
	})

	t.Run("Publish with templates and --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-templates-count"
			var messages []string
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			count := 3
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s \"Message {{ Count }}\" --count %d", srv.ClientURL(), subject, count))

			if len(messages) != count {
				t.Errorf("expected %d messages and received %d", count, len(messages))
			}

			for i := range count {
				expected := fmt.Sprintf("Message %d", i+1)
				if messages[i] != expected {
					t.Errorf("expected message[%d] %q got %q", i, expected, messages[i])
				}
			}

			return nil
		})
	})
}

func TestCLIPubSTDIN(t *testing.T) {
	t.Run("Publish doesn't eat stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var messages []string
			sub, _ := nc.Subscribe("test.*", func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			tmpfile, err := os.CreateTemp(t.TempDir(), "repro.txt")
			if err != nil {
				t.Fatal(err)
			}
			defer tmpfile.Close()

			lines := []string{
				"test.1;one",
				"test.2;two",
				"test.3;three",
			}

			for _, line := range lines {
				fmt.Fprintln(tmpfile, line)
			}

			scriptPath := "testdata/publish_stdin_test.sh"
			cmd := exec.Command("sh", scriptPath, srv.ClientURL(), tmpfile.Name())

			msg, err := cmd.CombinedOutput()
			if err != nil {
				t.Errorf("failed to run test script: %s \n %s", msg, err)
			}

			if len(messages) != 3 {
				t.Errorf("expected 3 message and received %d", len(messages))
			}
			return nil
		})
	})
}

func TestCLIPubCount(t *testing.T) {
	t.Run("Publish with --count", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-count"
			var messages []string
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			count := 5
			body := "test message"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --count %d", srv.ClientURL(), subject, body, count))

			if len(messages) != count {
				t.Errorf("expected %d messages and received %d", count, len(messages))
			}
			for i, msg := range messages {
				if msg != body {
					t.Errorf("message[%d]: expected %q got %q", i, body, msg)
				}
			}
			return nil
		})
	})
	t.Run("Publish with --count > 20 to trigger progress bar", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-count-large"
			var messages []string
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			count := 25
			body := "test message"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --count %d", srv.ClientURL(), subject, body, count))

			if len(messages) != count {
				t.Errorf("expected %d messages and received %d", count, len(messages))
			}

			return nil
		})
	})
}

func TestCLIPubSleep(t *testing.T) {
	t.Run("Publish with --sleep", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-sleep"
			var messages []string
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			count := 3
			body := "sleep test"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --count %d --sleep 10ms", srv.ClientURL(), subject, body, count))

			if len(messages) != count {
				t.Errorf("expected %d messages and received %d", count, len(messages))
			}
			return nil
		})
	})
}

func TestCLIPubReply(t *testing.T) {
	t.Run("Publish with custom --reply subject", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-reply-subject"
			replySubject := "custom.reply.subject"
			var receivedMsg *nats.Msg
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedMsg = m
			})
			defer sub.Unsubscribe()
			nc.Flush()

			body := "test with reply"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --reply %s", srv.ClientURL(), subject, body, replySubject))

			if receivedMsg == nil {
				t.Fatal("no message received")
			}
			if receivedMsg.Reply != replySubject {
				t.Errorf("expected reply subject %q got %q", replySubject, receivedMsg.Reply)
			}
			if string(receivedMsg.Data) != body {
				t.Errorf("expected body %q got %q", body, string(receivedMsg.Data))
			}
			return nil
		})
	})
}

func TestCLIPubHeaders(t *testing.T) {
	t.Run("Publish with --header", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-headers"
			var receivedMsg *nats.Msg
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				receivedMsg = m
			})
			defer sub.Unsubscribe()
			nc.Flush()

			body := "test with headers"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' -H 'X-Test:value1' -H 'X-Test2:value2'", srv.ClientURL(), subject, body))

			if receivedMsg == nil {
				t.Fatal("no message received")
			}
			if receivedMsg.Header.Get("X-Test") != "value1" {
				t.Errorf("expected header X-Test:value1 got %q", receivedMsg.Header.Get("X-Test"))
			}
			if receivedMsg.Header.Get("X-Test2") != "value2" {
				t.Errorf("expected header X-Custom:value2 got %q", receivedMsg.Header.Get("X-Custom"))
			}
			if string(receivedMsg.Data) != body {
				t.Errorf("expected body %q got %q", body, string(receivedMsg.Data))
			}
			return nil
		})
	})
}

func TestCLIPubQuiet(t *testing.T) {
	t.Run("Publish with --quiet", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-quiet"
			var messages []string
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			body := "quiet test"
			output := runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --quiet", srv.ClientURL(), subject, body))

			if len(output) != 0 {
				t.Errorf("expected no output with --quiet, got: %s", output)
			}

			if len(messages) != 1 {
				t.Errorf("expected 1 message and received %d", len(messages))
			}
			if messages[0] != body {
				t.Errorf("expected message %q got %q", body, messages[0])
			}

			return nil
		})
	})
}

func TestCLIPubJetStream(t *testing.T) {
	t.Run("Publish with --jetstream", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "js.test"

			stream, err := mgr.NewStream("JSTEST", jsm.Subjects(subject))
			if err != nil {
				t.Fatalf("failed to create stream: %s", err)
			}

			body := "jetstream message"
			runNatsCli(t, fmt.Sprintf("--server='%s' pub %s '%s' --jetstream", srv.ClientURL(), subject, body))

			info, err := stream.State()
			if err != nil {
				t.Fatalf("failed to get stream state: %s", err)
			}

			if info.Msgs != 1 {
				t.Errorf("expected 1 message in stream, got %d", info.Msgs)
			}

			return nil
		})
	})

	t.Run("Publish --jetstream with --send-on newline", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "js.newline"

			stream, err := mgr.NewStream("JSNEWLINE", jsm.Subjects(subject))
			if err != nil {
				t.Fatalf("failed to create stream: %s", err)
			}

			expected := []string{"line1", "line2", "line3"}
			runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --send-on newline --force-stdin %s --jetstream", srv.ClientURL(), subject))

			info, err := stream.State()
			if err != nil {
				t.Fatalf("failed to get stream state: %s", err)
			}

			if info.Msgs != 3 {
				t.Errorf("expected 3 messages in stream, got %d", info.Msgs)
			}

			return nil
		})
	})
}

func TestCLIPubAtomic(t *testing.T) {
	t.Run("Atomic publish with --jetstream", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-sendon"
			var messages []string
			expected := []string{"test", "pub", "input"}

			_, err := mgr.NewStream("test-stream", jsm.Subjects(subject), jsm.AllowAtomicBatchPublish())
			if err != nil {
				t.Errorf("failed to create stream: %s", err)
			}

			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})
			defer sub.Unsubscribe()
			nc.Flush()

			_, err = runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --send-on newline --force-stdin %s --jetstream --atomic", srv.ClientURL(), subject))
			if err != nil {
				t.Fatalf("failed with error %s", err.Error())
			}

			if len(messages) != len(expected) {
				t.Errorf("expected %d messages and received %d", len(expected), len(messages))
			}
			for i, msg := range messages {
				if messages[i] != expected[i] {
					t.Errorf("expected message(%d) %q got %q", i, expected[i], msg)
				}
			}
			return nil
		})
	})

	t.Run("Atomic publish without --send-on newline", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			expected := []string{"test", "pub", "input"}
			subject := "test-sendon"

			_, err := runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --jetstream --force-stdin %s  --atomic", srv.ClientURL(), subject))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !strings.Contains(err.Error(), "error: atomic batch publishing requires Jetstream ") {
				t.Fatalf("expected atomic batch publishing error, got %s", err)
			}
			return nil
		})
	})

	t.Run("Atomic publish without --send-on newline or --jetstream", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			expected := []string{"test", "pub", "input"}
			subject := "test-sendon"

			_, err := runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --force-stdin %s  --atomic", srv.ClientURL(), subject))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !strings.Contains(err.Error(), "error: atomic batch publishing requires Jetstream ") {
				t.Fatalf("expected atomic batch publishing error, got %s", err)
			}
			return nil
		})
	})
}

// Request Command Tests

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

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

func TestCLIPubSendOnNewline(t *testing.T) {
	t.Run("Publish with body argument", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			subject := "test-body"
			var messages []string
			expected := "Test Message"
			sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})

			runNatsCli(t, fmt.Sprintf("--server='%s' pub --send-on newline %s '%s'", srv.ClientURL(), subject, expected))
			_ = sub.Unsubscribe()

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

			out := runNatsCli(t, fmt.Sprintf("--server='%s' pub --send-on newline -q %s '%s'", srv.ClientURL(), subject, expected))
			_ = sub.Unsubscribe()

			if len(out) != 0 {
				t.Errorf("expected cli to output to be 0 but got %d\n %s", len(out), out)
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

			// --force-stdin required for testing as a terminal is not present
			runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --send-on newline --force-stdin %s", srv.ClientURL(), subject))
			_ = sub.Unsubscribe()

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

			// --force-stdin required for testing as a terminal is not present
			runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' pub --force-stdin %s", srv.ClientURL(), subject))
			_ = sub.Unsubscribe()

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
	srv, _, _ := setupJStreamTest(t)
	defer srv.Shutdown()
	nc, _, _ := prepareHelper(srv.ClientURL())
	t.Run("Publish --templates (Default)", func(t *testing.T) {
		subject := "test-templates"
		var messages []string
		expected := "Count: 1"
		sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
			messages = append(messages, string(m.Data))
		})

		runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' pub --force-stdin %s \"Count: {{ Count }}\"", srv.ClientURL(), subject))
		_ = sub.Unsubscribe()

		if len(messages) != 1 {
			t.Errorf("expected 1 message and received %d", len(messages))
		}
		if len(messages) > 0 && messages[0] != expected {
			t.Errorf("expected message %q got %q", expected, messages[0])
		}
	})

	t.Run("Publish --no-templates", func(t *testing.T) {
		subject := "test-no-templates"
		var messages []string
		expected := "Count: {{ Count }}"
		sub, _ := nc.Subscribe(subject, func(m *nats.Msg) {
			messages = append(messages, string(m.Data))
		})

		runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' pub --force-stdin %s \"Count: {{ Count }}\" --no-templates", srv.ClientURL(), subject))
		_ = sub.Unsubscribe()

		if len(messages) != 1 {
			t.Errorf("expected 1 message and received %d", len(messages))
		}
		if len(messages) > 0 && messages[0] != expected {
			t.Errorf("expected message %q got %q", expected, messages[0])
		}
	})
}

func TestCLIPubSTDIN(t *testing.T) {
	t.Run("Publish doesn't eat stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			var messages []string
			sub, _ := nc.Subscribe("test.*", func(m *nats.Msg) {
				messages = append(messages, string(m.Data))
			})

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

			_ = sub.Unsubscribe()

			if len(messages) != 3 {
				t.Errorf("expected 3 message and received %d", len(messages))
			}
			return nil
		})
	})
}

func TestCLIPubAtomic(t *testing.T) {
	t.Run("Atomic publish with jetstream", func(t *testing.T) {
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

			_, err = runNatsCliWithInput(t, strings.Join(expected, "\n"), fmt.Sprintf("--server='%s' pub --send-on newline --force-stdin %s --jetstream --atomic", srv.ClientURL(), subject))
			if err != nil {
				t.Fatalf("failed with error %s", err.Error())
			}

			_ = sub.Unsubscribe()

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

func TestCLIRequestSTDIN(t *testing.T) {
	t.Run("Request with body argument", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-body"
			expected := "test request"

			nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("echo: " + string(m.Data)))
			})
			nc.Flush()

			output := runNatsCli(t, fmt.Sprintf("--server='%s' request %s '%s'", srv.ClientURL(), subject, expected))

			if !strings.Contains(string(output), "echo: test request") {
				t.Errorf("expected response with echoed data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request with stdin eof", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-stdin"
			expected := "stdin payload"

			nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("received: " + string(m.Data)))
			})
			nc.Flush()

			output, _ := runNatsCliWithInput(t, expected, fmt.Sprintf("--server='%s' request --force-stdin %s", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "received: stdin payload") {
				t.Errorf("expected response with stdin data, got: %s", output)
			}
			return nil
		})
	})

	t.Run("Request with large payload from stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-large"

			nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte(fmt.Sprintf("received %d bytes", len(m.Data))))
			})
			nc.Flush()

			largePayload := strings.Repeat("A", 150*1024)

			output, _ := runNatsCliWithInput(t, largePayload, fmt.Sprintf("--server='%s' request --force-stdin %s", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "received 153600 bytes") {
				t.Errorf("expected confirmation of large payload, got: %s", output)
			}

			return nil
		})
	})

	t.Run("Request --send-on newline from stdin", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-newline"
			var receivedRequests []string
			expected := []string{"request1", "request2", "request3"}

			nc.Subscribe(subject, func(m *nats.Msg) {
				receivedRequests = append(receivedRequests, string(m.Data))
				m.Respond([]byte("response: " + string(m.Data)))
			})
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

	t.Run("Request with raw flag", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-raw"

			nc.Subscribe(subject, func(m *nats.Msg) {
				m.Respond([]byte("raw response"))
			})
			nc.Flush()

			output, _ := runNatsCliWithInput(t, "test", fmt.Sprintf("--server='%s' request --force-stdin --raw %s", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "raw response") {
				t.Errorf("expected raw response, got: %s", output)
			}

			if strings.Contains(string(output), "Received with rtt") {
				t.Errorf("expected no metadata with --raw flag, got: %s", output)
			}

			return nil
		})
	})

	t.Run("Request with multiple replies", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "test-request-multi"
			replyCount := 0

			nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
			})

			nc.Subscribe(subject, func(m *nats.Msg) {
				replyCount++
				m.Respond([]byte(fmt.Sprintf("reply %d", replyCount)))
			})
			nc.Flush()

			output, _ := runNatsCliWithInput(t, "test", fmt.Sprintf("--server='%s' request --force-stdin --replies 2 %s", srv.ClientURL(), subject))

			if !strings.Contains(string(output), "reply 1") || !strings.Contains(string(output), "reply 2") {
				t.Errorf("expected both replies, got: %s", output)
			}

			return nil
		})
	})
}

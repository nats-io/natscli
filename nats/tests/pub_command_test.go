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
	"github.com/nats-io/nats.go/micro"
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

func TestCLIRequestRepliesAll(t *testing.T) {
	srv, _, _ := setupJStreamTest(t)
	defer srv.Shutdown()
	nc, _, _ := prepareHelper(srv.ClientURL())

	svc, err := micro.AddService(nc, micro.Config{
		Name:    "svc",
		Version: "1.0.0",
	})
	if err != nil {
		t.Errorf("failed to add service: %v", err)
	}
	defer svc.Stop()

	err = svc.AddEndpoint("test-replies-all", micro.HandlerFunc(func(req micro.Request) {
		// send three replies
		headers := nats.Header{"NATS-Reply-Count": {"3"}}
		req.Respond([]byte("Response 0"), micro.WithHeaders(micro.Headers(headers)))
		req.Respond([]byte("Response 1"))
		req.Respond([]byte("Response 2"))
	}))
	if err != nil {
		t.Errorf("failed to add service endpoint test-replies-all: %v", err)
	}

	err = svc.AddEndpoint("test-replies-no-header", micro.HandlerFunc(func(req micro.Request) {
		req.Respond([]byte("Response 0"))
		req.Respond([]byte("Response 1"))
		req.Respond([]byte("Response 2"))
	}))
	if err != nil {
		t.Errorf("failed to add service endpoint test-replies-no-header: %v", err)
	}

	t.Run("Request with --replies-all", func(t *testing.T) {
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' request --replies-all %s ''", srv.ClientURL(), "test-replies-all")))

		responseCount := 0
		for i := range 3 {
			if strings.Contains(output, fmt.Sprintf("Response %d", i)) {
				responseCount++
			}
		}

		if responseCount != 3 {
			t.Errorf("expected %d replies, got %d\nOutput: %s", 3, responseCount, output)
		}
	})

	t.Run("Request with --replies-all without reply count header", func(t *testing.T) {
		out := string(runNatsCli(t, fmt.Sprintf("--server='%s' request --replies-all %s 'test request'", srv.ClientURL(), "test-replies-no-header")))

		// Should still get the one reply even without the header
		if !strings.Contains(out, "Response 0") {
			t.Errorf("expected reply to be received\nOutput: %s", out)
		}

		// Should not get the other replies
		if strings.Contains(out, "Response 1") {
			t.Errorf("expected single reply to be received\nOutput: %s", out)
		}
		if strings.Contains(out, "Response 2") {
			t.Errorf("expected single reply to be received\nOutput: %s", out)
		}
	})
}

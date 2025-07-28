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

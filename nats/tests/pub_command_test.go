package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"strings"
	"testing"
)

func TestCLIPubSendOnNewline(t *testing.T) {
	srv, _, _ := setupJStreamTest(t)
	defer srv.Shutdown()
	nc, _, _ := prepareHelper(srv.ClientURL())

	t.Run("Publish with body argument", func(t *testing.T) {
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
	})

	t.Run("Publish with body argument and --quiet", func(t *testing.T) {
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
	})

	t.Run("Publish --send-on newline from stdin", func(t *testing.T) {
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
	})

	t.Run("Publish --send-on eof from stdin", func(t *testing.T) {
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
	})
}

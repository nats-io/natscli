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

//go:build !windows

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestPubPipedStdinWithTerminalStdout(t *testing.T) {
	withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
		subject := "pub-stdin-test"
		expected := "pub-stdin-test-output"
		var received string

		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			received = string(m.Data)
		})
		if err != nil {
			t.Fatalf("unable to subscribe to subject - %v", err)
		}

		defer sub.Unsubscribe()
		nc.Flush()

		ptmx, pts, err := pty.Open()
		if err != nil {
			t.Fatalf("unable to open PTY - %v", err)
		}
		defer ptmx.Close()
		defer pts.Close()

		var cmd *exec.Cmd
		if os.Getenv("CI") == "true" {
			cmd = exec.Command("../nats", "--server", srv.ClientURL(), "pub", subject)
		} else {
			cmd = exec.Command("go", "run", "../main.go", "--server", srv.ClientURL(), "pub", subject)
		}
		cmd.Stdin = strings.NewReader(expected)
		cmd.Stdout = pts
		cmd.Stderr = pts

		if err := cmd.Start(); err != nil {
			t.Fatalf("unable to run nats client command - %v", err)
		}
		pts.Close()

		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()

		go func() {
			buffer := make([]byte, 4096)
			for {
				if _, err := ptmx.Read(buffer); err != nil {
					return
				}
			}
		}()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("command failed - %v", err)
			}
		case <-time.After(30 * time.Second):
			cmd.Process.Kill()
			t.Fatal("command timed out")
		}

		nc.Flush()
		time.Sleep(250 * time.Millisecond)

		if received != expected {
			t.Errorf("expected %q, got %q", expected, received)
		}

		return nil
	})
}

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
	"testing"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestAccountInfo(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s account info", srv.ClientURL(), sysUserCreds)))

		expected := map[string]any{
			"Header":                   "Account Information",
			"User":                     "sys",
			"Account":                  `SYS \(SYS\)`,
			"Expires":                  "never",
			"Client ID":                `\d+`,
			"Client IP":                `127\.0\.0\.1`,
			"RTT":                      `.+`,
			"Headers Supported":        "true",
			"Maximum Payload":          "1.0 MiB",
			"Connected URL":            `nats://localhost:\d+`,
			"Connected Address":        `127\.0\.0\.1:\d+`,
			"Connected Server Version": `.+`,
			"Connected Server ID":      `.+`,
			"System Account":           "true",
			"Connected Server Name":    "s1",
			"TLS Connection":           "no",
		}

		err := expectMatchJSON(t, output, expected)
		if err != nil {
			t.Error(err)
		}
		return nil
	})
}

func TestAccountReport(t *testing.T) {
	t.Run("statistics command", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s account report statistics", srv.ClientURL(), sysUserCreds)))

			if !expectMatchLine(t, output, "Server", "Cluster", "Version", "Connections", "Subscriptions") {
				t.Errorf("expected header line in output: %s", output)
			}
			if !expectMatchLine(t, output, "localhost") {
				t.Errorf("expected data row with server info in output: %s", output)
			}
			return nil
		})
	})

	t.Run("connections command", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' %s account report connections", srv.ClientURL(), sysUserCreds)))

			if !expectMatchLine(t, output, "CID", "Name", "Server", "IP", "Account") {
				t.Errorf("expected header line in output: %s", output)
			}
			if !expectMatchLine(t, output, `\d+`, "s1", `127\.0\.0\.1`) {
				t.Errorf("expected connection data row in output: %s", output)
			}
			return nil
		})
	})
}

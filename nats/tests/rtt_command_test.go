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
	"net"
	"regexp"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestCLIRTTMultipleTargets(t *testing.T) {
	withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
		silent, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		defer silent.Close()

		go func() {
			for {
				conn, err := silent.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
			}
		}()

		live := fmt.Sprintf("nats://127.0.0.1:%d", srv.Addr().(*net.TCPAddr).Port)

		output, err := runNatsCliCore(t, "", nil, fmt.Sprintf("--server=%s,nats://%s rtt", live, silent.Addr()))
		if err != nil {
			t.Fatalf("rtt failed: %v: %s", err, output)
		}

		if !expectMatchLine(t, string(output), fmt.Sprintf(`^%s: \d`, regexp.QuoteMeta(live))) {
			t.Errorf("missing rtt for %s: %s", live, output)
		}

		if !expectMatchLine(t, string(output), fmt.Sprintf(`^nats://%s: failed$`, regexp.QuoteMeta(silent.Addr().String()))) {
			t.Errorf("missing failed result for %s: %s", silent.Addr(), output)
		}

		return nil
	})
}

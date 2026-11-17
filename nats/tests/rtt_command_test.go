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
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	ntfsvc "github.com/synadia-io/orbit.go/ntf"
	ntf "github.com/synadia-io/orbit.go/ntf-client"
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

func TestCLIRTTHostnamePlainServer(t *testing.T) {
	withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
		addr := srv.Addr().(*net.TCPAddr)

		output, err := runNatsCliCore(t, "", nil, fmt.Sprintf("--server=nats://localhost:%d rtt", addr.Port))
		if err != nil {
			t.Fatalf("rtt failed: %v: %s", err, output)
		}

		if !expectMatchLine(t, string(output), fmt.Sprintf(`^%s: \d`, regexp.QuoteMeta("nats://"+addr.String()))) {
			t.Errorf("missing rtt for %s: %s", addr, output)
		}

		return nil
	})
}

func TestCLIRTTHostnameTLSServer(t *testing.T) {
	svc, err := ntfsvc.New(context.Background(), ntfsvc.Options{})
	if err != nil {
		t.Fatalf("ntf service failed: %v", err)
	}
	defer svc.Close()

	inst := ntf.New(t, svc.ClientURL()).CreateServer(t, false, ntf.WithGeneratedTLS(ntf.TLSServerOnly(), ntf.TLSSANs("localhost")))
	defer inst.Destroy(t)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	err = os.WriteFile(caFile, []byte(inst.TLS.CAPEM), 0600)
	if err != nil {
		t.Fatalf("write ca failed: %v", err)
	}

	port := inst.Servers[0].Port

	output, err := runNatsCliCore(t, "", nil, fmt.Sprintf("--server=nats://localhost:%d --tlsca='%s' rtt", port, caFile))
	if err != nil {
		t.Fatalf("rtt failed: %v: %s", err, output)
	}

	if !expectMatchLine(t, string(output), fmt.Sprintf(`^nats://127\.0\.0\.1:%d: \d`, port)) {
		t.Errorf("missing rtt for 127.0.0.1:%d: %s", port, output)
	}
}

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
	"sync"
	"testing"
	"time"
)

// TestConnectTimeoutFlag connects to a listener that accepts the TCP connection
// but never sends the server INFO, so the connect attempt runs for the full
// connect timeout. The process startup cost is the same for both invocations
// and cancels out in the comparison, so only --connect-timeout can account for
// the difference between the two runs.
func TestConnectTimeoutFlag(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not listen: %v", err)
	}
	defer listener.Close()

	var mu sync.Mutex
	var accepted []net.Conn
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range accepted {
			conn.Close()
		}
	}()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			mu.Lock()
			accepted = append(accepted, conn)
			mu.Unlock()
		}
	}()

	url := fmt.Sprintf("nats://%s", listener.Addr().String())

	// first invocation may have to build the CLI, do not measure that one
	runNatsCliCore(t, "", nil, "--version")

	timed := func(timeout string) time.Duration {
		t.Helper()

		start := time.Now()
		err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' --connect-timeout=%s server ping", url, timeout))
		elapsed := time.Since(start)

		if err == nil {
			t.Fatalf("expected the connection to %s to fail with a %s connect timeout", url, timeout)
		}

		return elapsed
	}

	short := timed("250ms")
	long := timed("6s")

	if long-short < 4*time.Second {
		t.Fatalf("--connect-timeout did not extend the connect attempt: 250ms run took %v, 6s run took %v", short, long)
	}
}

// Copyright 2025 The NATS Authors
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
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/natscli/cli"
	"github.com/synadia-io/orbit.go/counters"
	"strings"
	"testing"
	"time"
)

func init() { cli.SkipContexts = true }

func createCounterStream(t *testing.T, nc *nats.Conn) counters.Counter {
	t.Helper()

	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	stream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "COUNTERS",
		Subjects:        []string{"counters.>"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("creating counter stream: %v", err)
	}

	ctr, err := counters.NewCounterFromStream(js, stream)
	if err != nil {
		t.Fatalf("creating counter from stream: %v", err)
	}

	return ctr
}

func TestCounterGet(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		ctr := createCounterStream(t, nc)
		_, err := ctr.AddInt(context.Background(), "counters.test", 10)
		if err != nil {
			t.Fatal(err)
		}

		out := runNatsCli(t, fmt.Sprintf("--server='%s' counter get counters.test", srv.ClientURL()))
		if strings.TrimSpace(string(out)) != "10" {
			t.Fatalf("get failed: %s != 10", string(out))
		}

		return nil
	})
}

func TestCounterView(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		ctr := createCounterStream(t, nc)
		_, err := ctr.AddInt(context.Background(), "counters.test", 10)
		if err != nil {
			t.Fatal(err)
		}

		out := runNatsCli(t, fmt.Sprintf("--server='%s' counter view counters.test", srv.ClientURL()))
		err = expectMatchJSON(t, string(out), map[string]any{
			"Value":     "10",
			"Increment": "10",
			"Subject":   "counters.test",
		})
		if err != nil {
			t.Fatal(err)
		}

		return nil
	})
}

func TestCounterIncrement(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		ctr := createCounterStream(t, nc)
		_, err := ctr.AddInt(context.Background(), "counters.test", 10)
		if err != nil {
			t.Fatal(err)
		}

		out := runNatsCli(t, fmt.Sprintf("--server='%s' counter incr counters.test 10", srv.ClientURL()))
		if strings.TrimSpace(string(out)) != "20" {
			t.Fatalf("get failed: %s != 20", string(out))
		}

		return nil
	})
}

func TestCounterList(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		ctr := createCounterStream(t, nc)
		_, err := ctr.AddInt(context.Background(), "counters.test", 10)
		if err != nil {
			t.Fatal(err)
		}

		out := runNatsCli(t, fmt.Sprintf("--server='%s' counter ls 'counters.>' --json", srv.ClientURL()))

		v := []string{}
		err = json.Unmarshal(out, &v)
		if err != nil {
			t.Fatal(err)
		}

		if !cmp.Equal(v, []string{"counters.test"}) {
			t.Fatalf("list failed: %s", cmp.Diff(v, []string{"counters.test"}))
		}

		return nil
	})
}

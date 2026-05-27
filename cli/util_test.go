// Copyright 2019-2025 The NATS Authors
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

package cli

import (
	"testing"

	"github.com/nats-io/jsm.go/api"
)

func TestExtractWSProxyPath(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		servers   string
		proxyPath string
	}{
		{"nats url unchanged", "nats://localhost:4222", "nats://localhost:4222", ""},
		{"ws without path", "ws://localhost:8080", "ws://localhost:8080", ""},
		{"wss without path", "wss://localhost:8080", "wss://localhost:8080", ""},
		{"ws with path", "ws://localhost:8080/nats", "ws://localhost:8080", "/nats"},
		{"wss with path", "wss://host:443/proxy/nats", "wss://host:443", "/proxy/nats"},
		{"ws root path only", "ws://localhost:8080/", "ws://localhost:8080/", ""},
		{"multiple ws same path", "ws://h1:80/path,ws://h2:80/path", "ws://h1:80,ws://h2:80", "/path"},
		{"mixed ws and nats", "ws://h1:80/path,nats://h2:4222", "ws://h1:80,nats://h2:4222", "/path"},
		{"ws with path and query", "ws://proxy.example/nats?tenant=a", "ws://proxy.example", "/nats?tenant=a"},
		{"wss with path and query", "wss://proxy.example/nats?tenant=a&token=b", "wss://proxy.example", "/nats?tenant=a&token=b"},
		{"empty string", "", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			servers, proxyPath, err := extractWSProxyPath(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if servers != tc.servers {
				t.Errorf("servers: got %q, want %q", servers, tc.servers)
			}
			if proxyPath != tc.proxyPath {
				t.Errorf("proxyPath: got %q, want %q", proxyPath, tc.proxyPath)
			}
		})
	}

	t.Run("different paths error", func(t *testing.T) {
		_, _, err := extractWSProxyPath("ws://h1:80/path-a,ws://h2:80/path-b")
		if err == nil {
			t.Fatal("expected error for differing proxy paths")
		}
		want := `websocket servers with different paths are not supported: "/path-a", "/path-b"`
		if err.Error() != want {
			t.Errorf("error: got %q, want %q", err.Error(), want)
		}
	})
}

func TestRenderCluster(t *testing.T) {
	cluster := &api.ClusterInfo{
		Name:   "test",
		Leader: "S2",
		Replicas: []*api.PeerInfo{
			{Name: "S3", Current: false, Active: 30199700, Lag: 882130},
			{Name: "S1", Current: false, Active: 30202300, Lag: 882354},
		},
	}

	if result := renderCluster(cluster); result != "S1!, S2*, S3!" {
		t.Fatalf("invalid result: %s", result)
	}

	if result := renderCluster(&api.ClusterInfo{Name: "test"}); result != "" {
		t.Fatalf("invalid result: %q", result)
	}
}

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
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nats-io/jsm.go/audit/archive"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/scaffold"
	"gopkg.in/yaml.v2"
)

func expectMatchRegex(t *testing.T, pattern, output string) bool {
	t.Helper()

	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Errorf("invalid regex %q: %v", pattern, err)
		return false
	}

	return re.MatchString(output)
}

func expectMatchMap(t *testing.T, fields map[string]string, output string) (bool, string, string) {
	t.Helper()
	for field, expected := range fields {
		re := regexp.MustCompile(expected)
		if !re.MatchString(string(output)) {
			return false, field, re.String()
		}
	}
	return true, "", ""
}

func expectMatchLine(t *testing.T, output string, fields ...string) bool {
	t.Helper()
	rowLines := strings.Split(output, "\n")
	found := false

	for _, line := range rowLines {
		line = strings.TrimSpace(line)

		matchesAll := true
		for _, pattern := range fields {
			re, err := regexp.Compile(pattern)
			if err != nil {
				t.Errorf("invalid regex pattern %q: %v", pattern, err)
				matchesAll = false
				break
			}
			if !re.MatchString(line) {
				matchesAll = false
				break
			}
		}

		if matchesAll {
			found = true
			break
		}
	}

	return found
}

func expectMatchJSON(t *testing.T, jsonStr string, expected any) error {
	t.Helper()

	var actual any
	if err := json.Unmarshal([]byte(jsonStr), &actual); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return matchRecursive(actual, expected)
}

// matchRecursive compares actual and expected structures recursively.
// Maps are matched by key. Arrays pass if all expected items match any actual item.
// Strings are interpreted as regex patterns and matched against stringified actual values.
// Returns an error if any expected structure is missing or mismatched.
func matchRecursive(actual any, expected any) error {
	switch expectedTyped := expected.(type) {
	case map[string]any:
		actualMap, ok := actual.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object, got %T", actual)
		}
		for key, expectedValue := range expectedTyped {
			actualValue, exists := actualMap[key]
			if !exists {
				return fmt.Errorf("missing expected key: %q", key)
			}
			if err := matchRecursive(actualValue, expectedValue); err != nil {
				return fmt.Errorf("at key %q: %w", key, err)
			}
		}

	// Recursively check if any of the items in the expected array is the one we're looking for,
	// not expected[x] == actual[x]
	case []any:
		actualSlice, ok := actual.([]any)
		if !ok {
			return fmt.Errorf("expected array, got %T", actual)
		}

		for i, expectedItem := range expectedTyped {
			matched := false
			var lastErr error
			for _, actualItem := range actualSlice {
				if err := matchRecursive(actualItem, expectedItem); err == nil {
					matched = true
					break
				} else {
					lastErr = err
				}
			}
			if !matched {
				return fmt.Errorf("no match found for expected item at index %d: %v", i, lastErr)
			}
		}

	default:
		actualStr := fmt.Sprint(actual)
		patternStr := fmt.Sprint(expected)

		re, err := regexp.Compile(patternStr)
		if err != nil {
			return fmt.Errorf("invalid regex: %v", err)
		}
		if !re.MatchString(actualStr) {
			return fmt.Errorf("regex mismatch: value %q does not match pattern %q", actualStr, patternStr)
		}
	}

	return nil
}

// serveBundleZip starts up a webserver that we can connect to during tests to serve up a zip filel
// for bundle testing
func serveBundleZip(t *testing.T, bundle scaffold.Bundle) string {
	t.Helper()

	yamlData, err := yaml.Marshal(bundle)
	if err != nil {
		t.Fatalf("failed to marshal bundle: %v", err)
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	add := func(name string, contents []byte) {
		header := &zip.FileHeader{
			Name:   name,
			Method: zip.Deflate,
		}
		header.SetMode(0644)

		f, err := zw.CreateHeader(header)
		if err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
		if _, err := f.Write(contents); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}
	addDir := func(name string) {
		header := &zip.FileHeader{
			Name: name + "/",
		}
		header.SetMode(0755)
		_, err := zw.CreateHeader(header)
		if err != nil {
			t.Fatalf("failed to create dir %s: %v", name, err)
		}
	}

	addDir("scaffold")
	add("bundle.yaml", yamlData)
	add("scaffold.json", []byte(`{"source_directory": "scaffold"}`))
	add("scaffold/README.md", []byte("Generated by {{ .Contact }}"))
	add("scaffold/config.yaml", []byte(`jetstream: true
server_name: scaffolded-nats
port: 4222
`))

	_ = zw.Close()

	srv := &http.Server{
		Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/zip")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(buf.Bytes())
		}),
	}

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	go srv.Serve(ln)
	t.Cleanup(func() { _ = srv.Close() })

	return "http://" + ln.Addr().String()
}

func deleteProfileFile(t *testing.T, dir string) {
	t.Helper()

	pattern := regexp.MustCompile(`^mutex-\d{8}-\d{6}-s1$`)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && pattern.MatchString(info.Name()) {
			return os.Remove(path)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("failed to delete profile file: %v", err)
	}
}

// createTestArchive builds an archive file containing data for a single server
// and returns the path. The archive includes all the endpoint data needed for
// server request and server report commands.
func createTestArchive(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.zip")

	w, err := archive.NewWriter(path)
	if err != nil {
		t.Fatalf("creating archive writer: %v", err)
	}

	srvInfo := &server.ServerInfo{
		Name:      "s1",
		Host:      "localhost",
		ID:        "NABC1234567890NABC1234567890NABC1234567890NABC12345678",
		Cluster:   "TEST",
		Version:   "2.11.0",
		JetStream: true,
		Tags:      []string{"test"},
	}

	clusterTag := archive.TagCluster("TEST")
	serverTag := archive.TagServer("s1")

	w.Add(&server.ServerAPIVarzResponse{
		Server: srvInfo,
		Data: &server.Varz{
			MaxConn:       1000,
			MaxSubs:       100000,
			MaxPayload:    1048576,
			CPU:           1.5,
			Mem:           128 * 1024 * 1024,
			Connections:   5,
			Subscriptions: 42,
			InMsgs:        100,
			OutMsgs:       200,
			InBytes:       5000,
			OutBytes:      10000,
			SlowConsumers: 0,
		},
	}, clusterTag, serverTag, archive.TagServerVars())

	w.Add(&server.ServerAPIConnzResponse{
		Server: srvInfo,
		Data: &server.Connz{
			NumConns: 2,
			Conns: []*server.ConnInfo{
				{Name: "client-1", Kind: "Client", Type: "nats", Account: "users", Cid: 1, IP: "127.0.0.1", Port: 5000, InMsgs: 10, OutMsgs: 20, InBytes: 500, OutBytes: 1000, NumSubs: 3},
				{Name: "client-2", Kind: "Client", Type: "nats", Account: "users", Cid: 2, IP: "127.0.0.1", Port: 5001, InMsgs: 5, OutMsgs: 10, InBytes: 250, OutBytes: 500, NumSubs: 1},
			},
		},
	}, archive.TagAccount("users"), clusterTag, serverTag, archive.TagAccountConnections())

	w.Add(&server.ServerAPIRoutezResponse{
		Server: srvInfo,
		Data: &server.Routez{
			Routes: []*server.RouteInfo{},
		},
	}, clusterTag, serverTag, archive.TagServerRoutes())

	w.Add(&server.ServerAPIGatewayzResponse{
		Server: srvInfo,
		Data: &server.Gatewayz{
			Name: "TEST",
		},
	}, clusterTag, serverTag, archive.TagServerGateways())

	w.Add(&server.ServerAPILeafzResponse{
		Server: srvInfo,
		Data: &server.Leafz{
			Leafs: []*server.LeafInfo{},
		},
	}, clusterTag, serverTag, archive.TagServerLeafs())

	w.Add(&server.ServerAPISubszResponse{
		Server: srvInfo,
		Data: &server.Subsz{
			SublistStats: &server.SublistStats{
				NumSubs:      42,
				NumCache:     10,
				NumInserts:   100,
				NumRemoves:   58,
				NumMatches:   500,
				CacheHitRate: 0.95,
				MaxFanout:    3,
				AvgFanout:    1.5,
			},
		},
	}, clusterTag, serverTag, archive.TagServerSubs())

	w.Add(&server.ServerAPIJszResponse{
		Server: srvInfo,
		Data: &server.JSInfo{
			JetStreamStats: server.JetStreamStats{
				Memory:   0,
				Store:    1024,
				Accounts: 1,
				HAAssets: 0,
				API: server.JetStreamAPIStats{
					Total:  5,
					Errors: 0,
					Level:  4,
				},
			},
			ID:        "NABC1234567890NABC1234567890NABC1234567890NABC12345678",
			Streams:   1,
			Consumers: 1,
			Messages:  10,
			Bytes:     512,
			Config: server.JetStreamConfig{
				MaxMemory:    1073741824,
				MaxStore:     10737418240,
				StoreDir:     "/data/jetstream",
				SyncInterval: 120000000000,
			},
		},
	}, clusterTag, serverTag, archive.TagServerJetStream())

	w.Add(&server.ServerAPIHealthzResponse{
		Server: srvInfo,
		Data: &server.HealthStatus{
			Status:     "ok",
			StatusCode: 200,
		},
	}, clusterTag, serverTag, archive.TagServerHealth())

	w.Add(&server.ServerAPIAccountzResponse{
		Server: srvInfo,
		Data: &server.Accountz{
			SystemAccount: "SYS",
			Accounts:      []string{"users", "SYS"},
		},
	}, clusterTag, serverTag, archive.TagServerAccounts())

	if err := w.Close(); err != nil {
		t.Fatalf("closing archive writer: %v", err)
	}

	return path
}

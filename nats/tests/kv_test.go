// Copyright 2024 The NATS Authors
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
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/natscli/cli"

	"github.com/nats-io/nats.go"
)

func init() {
	cli.SkipContexts = true
}

func createTestBucket(t *testing.T, nc *nats.Conn, cfg *nats.KeyValueConfig) nats.KeyValue {
	t.Helper()

	if cfg == nil {
		cfg = &nats.KeyValueConfig{Bucket: "T"}
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("js failed: %s", err)
	}

	store, err := js.CreateKeyValue(cfg)
	if err != nil {
		t.Fatalf("new failed: %s", err)
	}

	return store
}

func mustPut(t *testing.T, store nats.KeyValue, key string, value string) uint64 {
	t.Helper()

	seq, err := store.Put(key, []byte(value))
	if err != nil {
		t.Fatalf("put failed: %s", err)
	}

	return seq
}

func TestCLIKVGet(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	mustPut(t, store, "X.Y", "Y")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' kv get T X.Y --raw", srv.ClientURL()))
	if strings.TrimSpace(string(out)) != "Y" {
		t.Fatalf("get failed: %s != Y", string(out))
	}
}

func TestCLIKVCreate(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	kvCreateCmd := fmt.Sprintf("--server='%s' kv create %s", srv.ClientURL(), store.Bucket())

	for _, test := range []struct {
		name  string
		key   string
		value string
		stdin bool
	}{
		{"simple", "X", "VAL", false},
		{"empty", "Y", "", false},
		{"stdin", "Z", "VAL", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.stdin {
				out := runNatsCliWithInput(t, test.value, fmt.Sprintf("%s %s", kvCreateCmd, test.key))
				if strings.TrimSpace(string(out)) != "" {
					t.Fatalf("put failed: %s", string(out))
				}
			} else {
				out := runNatsCli(t, fmt.Sprintf("%s %s %s", kvCreateCmd, test.key, test.value))
				if strings.TrimSpace(string(out)) != test.value {
					t.Fatalf("put failed: %s", string(out))
				}
			}

			val, err := store.Get(test.key)
			if err != nil {
				t.Fatalf("get failed: %s", err)
			}
			if !bytes.Equal(val.Value(), []byte(test.value)) {
				t.Fatalf("invalid value saved: %s", val.Value())
			}
		})
	}
}

func TestCLIKVPut(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	kvPutCmd := fmt.Sprintf("--server='%s' kv put %s", srv.ClientURL(), store.Bucket())

	for _, test := range []struct {
		name  string
		key   string
		value string
		stdin bool
	}{
		{"simple", "X", "VAL", false},
		{"empty", "Y", "", false},
		{"stdin", "Z", "VAL", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.stdin {
				out := runNatsCliWithInput(t, test.value, fmt.Sprintf("%s %s", kvPutCmd, test.key))
				if strings.TrimSpace(string(out)) != "" {
					t.Fatalf("put failed: %s", string(out))
				}
			} else {
				out := runNatsCli(t, fmt.Sprintf("%s %s %s", kvPutCmd, test.key, test.value))
				if strings.TrimSpace(string(out)) != test.value {
					t.Fatalf("put failed: %s", string(out))
				}
			}

			val, err := store.Get(test.key)
			if err != nil {
				t.Fatalf("get failed: %s", err)
			}
			if !bytes.Equal(val.Value(), []byte(test.value)) {
				t.Fatalf("invalid value saved: %s", val.Value())
			}
		})
	}
}

func TestCLIKVUpdate(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	kvUpdateCmd := fmt.Sprintf("--server='%s' kv update %s", srv.ClientURL(), store.Bucket())

	for _, test := range []struct {
		name  string
		key   string
		value string
		stdin bool
	}{
		{"simple", "X", "VAL", false},
		{"empty", "Y", "", false},
		{"stdin", "Z", "VAL", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rev := mustPut(t, store, test.key, "OLD")

			if test.stdin {
				out := runNatsCliWithInput(t, test.value, fmt.Sprintf("%s %s '' %d", kvUpdateCmd, test.key, rev))
				if strings.TrimSpace(string(out)) != "" {
					t.Fatalf("put failed: %s", string(out))
				}
			} else {
				out := runNatsCli(t, fmt.Sprintf("%s %s '%s' %d", kvUpdateCmd, test.key, test.value, rev))
				if strings.TrimSpace(string(out)) != test.value {
					t.Fatalf("put failed: %s", string(out))
				}
			}

			val, err := store.Get(test.key)
			if err != nil {
				t.Fatalf("get failed: %s", err)
			}
			if rev == val.Revision() {
				t.Fatalf("invalid revision: %d", val.Revision())
			}
			if !bytes.Equal(val.Value(), []byte(test.value)) {
				t.Fatalf("invalid value saved: %s", val.Value())
			}
		})
	}
}

func TestCLIKVDel(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	mustPut(t, store, "X", "VAL")

	runNatsCli(t, fmt.Sprintf("--server='%s' kv del T X -f", srv.ClientURL()))

	_, err := store.Get("X")
	if err != nats.ErrKeyNotFound {
		t.Fatalf("get did not fail: %v", err)
	}
}

func TestCLIAdd(t *testing.T) {
	srv, _, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' kv add T --history 5 --ttl 2m", srv.ClientURL()))
	known, err := mgr.IsKnownStream("KV_T")
	if err != nil {
		t.Fatalf("known failed: %s", err)
	}
	if !known {
		t.Fatalf("stream was not created")
	}

	stream, _ := mgr.LoadStream("KV_T")

	// TODO: needs status api
	// js, err := nc.JetStream()
	// if err != nil {
	// 	t.Fatalf("js failed: %s", err)
	// }
	//
	// status, err := store.Status()
	// if err != nil {
	// 	t.Fatalf("status failed: %s", err)
	// }

	if stream.MaxMsgsPerSubject() != 5 {
		t.Fatalf("history is %d", stream.MaxMsgsPerSubject())
	}

	if stream.MaxAge() != 2*time.Minute {
		t.Fatalf("ttl is %v", stream.MaxAge())
	}
}

func TestCLIPurge(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)
	mustPut(t, store, "X", "VALX")
	mustPut(t, store, "Y", "VALY")

	runNatsCli(t, fmt.Sprintf("--server='%s' kv purge T X -f", srv.ClientURL()))

	_, err := store.Get("X")
	if err != nats.ErrKeyNotFound {
		t.Fatalf("expected unknown key got: %v", err)
	}
	v, err := store.Get("Y")
	if err != nil {
		t.Fatalf("Y failed to get: %s", err)
	}
	if !bytes.Equal(v.Value(), []byte("VALY")) {
		t.Fatalf("incorrect Y value: %q", v.Value())
	}
}

func TestCLIRM(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	createTestBucket(t, nc, nil)

	runNatsCli(t, fmt.Sprintf("--server='%s' kv rm T -f", srv.ClientURL()))

	known, err := mgr.IsKnownStream("KV_T")
	if err != nil {
		t.Fatalf("is known failed: %s", err)
	}
	if known {
		t.Fatalf("stream was not deleted")
	}
}

func TestCLIEdit(t *testing.T) {
	os.Setenv("TESTING", "true")
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	cfg := nats.KeyValueConfig{
		Bucket:       "T",
		Description:  "This is an old descrption",
		MaxValueSize: 44,
		History:      44,
		TTL:          time.Duration(44 * time.Second),
		MaxBytes:     44,
		Compression:  true,
	}

	createTestBucket(t, nc, &cfg)
	output := string(runNatsCli(t, fmt.Sprintf("--server='%s' kv edit T --description='This is a new description'", srv.ClientURL())))
	err := expectMatchJSON(t, output, map[string]any{
		"Header": "Information for Key-Value Store Bucket T",
		"Configuration": map[string]any{
			"Backing Store Kind":    "JetStream",
			"Bucket Name":           "T",
			"Bucket Size":           "0 B",
			"Compressed":            "true",
			"Description":           "This is a new description",
			"History Kept":          "44",
			"JetStream Stream":      "KV_T",
			"Maximum Age":           "44.00s",
			"Maximum Bucket Size":   "44 B",
			"Maximum Value Size":    "44 B",
			"Per-Key TTL Supported": "false",
			"Storage":               "File",
			"Values Stored":         "0",
		},
		"Cluster Information": map[string]any{
			"Leader": ".+",
			"Name":   "",
		},
	})

	if err != nil {
		t.Error(err)
	}
}

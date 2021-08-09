package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go/kv"
	"github.com/nats-io/nats.go"
)

func init() {
	skipContexts = true
}

func createTestBucket(t *testing.T, nc *nats.Conn, opts ...kv.Option) kv.KV {
	t.Helper()

	store, err := kv.NewBucket(nc, "T", opts...)
	if err != nil {
		t.Fatalf("new failed: %s", err)
	}

	return store
}

func mustPut(t *testing.T, store kv.KV, key string, value string) uint64 {
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

	store := createTestBucket(t, nc)
	mustPut(t, store, "X.Y", "Y")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' kv get T X.Y --raw", srv.ClientURL()))
	if strings.TrimSpace(string(out)) != "Y" {
		t.Fatalf("get failed: %s != Y", string(out))
	}
}

func TestCLIKVPut(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc)

	out := runNatsCli(t, fmt.Sprintf("--server='%s' kv put T X VAL", srv.ClientURL()))
	if strings.TrimSpace(string(out)) != "VAL" {
		t.Fatalf("put failed: %s", string(out))
	}

	val, err := store.Get("X")
	if err != nil {
		t.Fatalf("get failed: %s", err)
	}
	if !bytes.Equal(val.Value(), []byte("VAL")) {
		t.Fatalf("invalid value saved: %s", val.Value())
	}
}

func TestCLIKVDel(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc)
	mustPut(t, store, "X", "VAL")

	runNatsCli(t, fmt.Sprintf("--server='%s' kv del T X -f", srv.ClientURL()))

	_, err := store.Get("X")
	if err != kv.ErrUnknownKey {
		t.Fatalf("get did not fail: %v", err)
	}
}

func TestCLIAdd(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	runNatsCli(t, fmt.Sprintf("--server='%s' kv add T --history 5 --ttl 2m", srv.ClientURL()))
	known, err := mgr.IsKnownStream("KV_T")
	if err != nil {
		t.Fatalf("known failed: %s", err)
	}
	if !known {
		t.Fatalf("stream was not created")
	}

	store, err := kv.NewClient(nc, "T")
	if err != nil {
		t.Fatalf("client failed: %s", err)
	}

	status, err := store.Status()
	if err != nil {
		t.Fatalf("status failed: %s", err)
	}

	if status.History() != 5 {
		t.Fatalf("history is %d", status.History())
	}

	if status.TTL() != 2*time.Minute {
		t.Fatalf("ttl is %v", status.TTL())
	}
}

func TestCLIDump(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc)
	mustPut(t, store, "X", "VALX")
	mustPut(t, store, "Y", "VALY")

	out := runNatsCli(t, fmt.Sprintf("--server='%s' kv dump T", srv.ClientURL()))
	var dumped map[string]kv.GenericEntry
	err := json.Unmarshal(out, &dumped)
	if err != nil {
		t.Fatalf("json unmarshal failed: %s", err)
	}

	if len(dumped) != 2 {
		t.Fatalf("expected 2 entries got %d", len(dumped))
	}
	if dumped["X"].Key != "X" && !bytes.Equal(dumped["X"].Val, []byte("VALX")) {
		t.Fatalf("did not get right res: %+v", dumped["X"])
	}
	if dumped["Y"].Key != "Y" && !bytes.Equal(dumped["Y"].Val, []byte("VALY")) {
		t.Fatalf("did not get right res: %+v", dumped["Y"])
	}
}

func TestCLIPurge(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc)
	mustPut(t, store, "X", "VALX")
	mustPut(t, store, "Y", "VALY")

	runNatsCli(t, fmt.Sprintf("--server='%s' kv purge T -f", srv.ClientURL()))

	status, err := store.Status()
	if err != nil {
		t.Fatalf("status failed: %s", err)
	}
	if status.Values() != 0 {
		t.Fatalf("found %d messages after purge", status.Values())
	}
}

func TestCLIRM(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	createTestBucket(t, nc)

	runNatsCli(t, fmt.Sprintf("--server='%s' kv rm T -f", srv.ClientURL()))

	known, err := mgr.IsKnownStream("KV_T")
	if err != nil {
		t.Fatalf("is known failed: %s", err)
	}
	if known {
		t.Fatalf("stream was not deleted")
	}
}

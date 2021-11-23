package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/cli"
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

func TestCLIKVPut(t *testing.T) {
	srv, nc, _ := setupJStreamTest(t)
	defer srv.Shutdown()

	store := createTestBucket(t, nc, nil)

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

// Copyright 2020-2022 The NATS Authors
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
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/mprimi/natscli/monitor"
)

func assertHasPDItem(t *testing.T, check *monitor.Result, items ...string) {
	t.Helper()

	if len(items) == 0 {
		t.Fatalf("no items to assert")
	}

	pd := check.PerfData.String()
	for _, i := range items {
		if !strings.Contains(pd, i) {
			t.Fatalf("did not contain item: '%s': %s", i, pd)
		}
	}
}

func withJetStream(t *testing.T, cb func(srv *server.Server, nc *nats.Conn, mgr *jsm.Manager)) {
	t.Helper()

	dir, err := os.MkdirTemp("", "")
	checkErr(t, err, "could not create temporary js store: %v", err)
	defer os.RemoveAll(dir)

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		StoreDir:  dir,
		JetStream: true,
	})
	checkErr(t, err, "could not start js server: %v", err)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Errorf("nats server did not start")
	}
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	}()

	opts.Conn = nil
	nc, mgr, err := prepareHelper(srv.ClientURL())
	checkErr(t, err, "could not connect client to server @ %s: %v", srv.ClientURL(), err)
	defer nc.Close()

	cb(srv, nc, mgr)
}

func dfltCmd() *SrvCheckCmd {
	return &SrvCheckCmd{kvBucket: "TEST", kvValuesWarn: -1, kvValuesCrit: -1}
}

func TestCheckConsumer(t *testing.T) {
	t.Run("Ack Pending", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerAckOutstandingCritical: 100}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumAckPending: 10,
		})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumAckPending: 300,
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Ack Pending: 300")
	})

	t.Run("Waiting Pulls", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerWaitingCritical: 100}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumWaiting: 10,
		})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumWaiting: 300,
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Waiting Pulls: 300")
	})

	t.Run("Pending", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerUnprocessedCritical: 100}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumPending: 10,
		})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumPending: 300,
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Unprocessed Messages: 300")
	})

	t.Run("Redelivered", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerRedeliveryCritical: 100}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumRedelivered: 10,
		})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			NumRedelivered: 300,
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Redelivered Messages: 300")
	})

	t.Run("Last Delivery", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerLastDeliveryCritical: 10 * time.Second}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "No deliveries")

		check = &monitor.Result{}
		last := time.Now().Add(-1 * time.Second)
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			Delivered: api.SequenceInfo{
				Last: &last,
			},
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		last = time.Now().Add(-500 * time.Second)
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			Delivered: api.SequenceInfo{
				Last: &last,
			},
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Last delivery 8m20s ago")
	})

	t.Run("Last Ack", func(t *testing.T) {
		cmd := &SrvCheckCmd{sourcesStream: "TEST", consumerName: "CONS", consumerLastAckCritical: 10 * time.Second}
		check := &monitor.Result{}

		cmd.checkConsumerStatus(check, api.ConsumerInfo{})

		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "No acknowledgements")

		check = &monitor.Result{}
		last := time.Now().Add(-1 * time.Second)
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			AckFloor: api.SequenceInfo{
				Last: &last,
			},
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)

		check = &monitor.Result{}
		last = time.Now().Add(-500 * time.Second)
		cmd.checkConsumerStatus(check, api.ConsumerInfo{
			AckFloor: api.SequenceInfo{
				Last: &last,
			},
		})
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
		assertListEquals(t, check.Criticals, "Last ack 8m20s ago")
	})
}

func TestCheckMessage(t *testing.T) {
	t.Run("Body timestamp", func(t *testing.T) {
		withJetStream(t, func(_ *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
			cmd := dfltCmd()
			check := &monitor.Result{}

			opts.Conn = nc
			_, err := mgr.NewStream("TEST")
			checkErr(t, err, "stream create failed: %v", err)

			cmd.sourcesStream = "TEST"
			cmd.msgSubject = "TEST"
			cmd.msgAgeCrit = 5 * time.Second
			cmd.msgAgeWarn = time.Second
			cmd.msgBodyAsTs = true

			cmd.checkStreamMessage(mgr, check)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.OKs)
			assertListEquals(t, check.Criticals, "no message found")

			now := time.Now().Unix()
			_, err = nc.Request("TEST", []byte(strconv.Itoa(int(now))), time.Second)
			checkErr(t, err, "publish failed: %v", err)

			check = &monitor.Result{}
			cmd.checkStreamMessage(mgr, check)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "Valid message on TEST > TEST")

			now = time.Now().Add(-2 * time.Second).Unix()
			_, err = nc.Request("TEST", []byte(strconv.Itoa(int(now))), time.Second)
			checkErr(t, err, "publish failed: %v", err)

			check = &monitor.Result{}
			cmd.checkStreamMessage(mgr, check)
			assertListIsEmpty(t, check.Criticals)
			if len(check.Warnings) != 1 {
				t.Fatalf("expected 1 warning got: %v", check.Warnings)
			}

			now = time.Now().Add(-6 * time.Second).Unix()
			_, err = nc.Request("TEST", []byte(strconv.Itoa(int(now))), time.Second)
			checkErr(t, err, "publish failed: %v", err)

			check = &monitor.Result{}
			cmd.checkStreamMessage(mgr, check)
			assertListIsEmpty(t, check.Warnings)
			if len(check.Criticals) != 1 {
				t.Fatalf("expected 1 critical got: %v", check.Criticals)
			}

			cmd.msgBodyAsTs = false

			check = &monitor.Result{}
			cmd.checkStreamMessage(mgr, check)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "Valid message on TEST > TEST")
		})
	})
}

func TestCheckKV(t *testing.T) {
	t.Run("Bucket", func(t *testing.T) {
		withJetStream(t, func(_ *server.Server, nc *nats.Conn, _ *jsm.Manager) {
			cmd := dfltCmd()
			check := &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.OKs)
			assertListEquals(t, check.Criticals, "bucket TEST not found")

			js, err := nc.JetStream()
			checkErr(t, err, "js context failed")

			_, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
			checkErr(t, err, "kv create failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "bucket TEST")
		})
	})

	t.Run("Values", func(t *testing.T) {
		withJetStream(t, func(srv *server.Server, nc *nats.Conn, _ *jsm.Manager) {
			fmt.Printf("url %v (%s)\n", srv.ClientURL(), srv.String())
			fmt.Printf("connected: %v version: %v name: %v\n", nc.ConnectedUrl(), nc.ConnectedServerVersion(), nc.ConnectedServerName())
			js, err := nc.JetStream()
			checkErr(t, err, "js context failed")

			bucket, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
			checkErr(t, err, "kv create failed: %v", err)

			cmd := dfltCmd()
			cmd.kvValuesWarn = 1
			cmd.kvValuesCrit = 2

			check := &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "0 values", "bucket TEST")

			_, err = bucket.PutString("K", "V")
			checkErr(t, err, "pub failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Warnings, "1 values")
			assertListIsEmpty(t, check.Criticals)

			_, err = bucket.PutString("K1", "V")
			checkErr(t, err, "pub failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Criticals, "2 values")
			assertListIsEmpty(t, check.Warnings)

			// now test inverse logic

			cmd.kvValuesCrit = 3
			cmd.kvValuesWarn = 5

			_, err = bucket.PutString("K2", "V")
			checkErr(t, err, "pub failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Criticals, "3 values")

			_, err = bucket.PutString("K3", "V")
			checkErr(t, err, "pub failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Warnings, "4 values")

			_, err = bucket.PutString("K4", "V")
			checkErr(t, err, "pub failed")
			_, err = bucket.PutString("K5", "V")
			checkErr(t, err, "pub failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.OKs, "bucket TEST", "6 values")
		})
	})

	t.Run("Key", func(t *testing.T) {
		withJetStream(t, func(srv *server.Server, nc *nats.Conn, _ *jsm.Manager) {
			js, err := nc.JetStream()
			checkErr(t, err, "js context failed")

			bucket, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST"})
			checkErr(t, err, "kv create failed")

			cmd := dfltCmd()
			cmd.kvKey = "KEY"
			check := &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Criticals, "key KEY not found")

			_, err = bucket.Put("KEY", []byte("VAL"))
			checkErr(t, err, "put failed")

			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListEquals(t, check.OKs, "bucket TEST", "key KEY found")
			assertListIsEmpty(t, check.Criticals)

			bucket.Delete("KEY")
			check = &monitor.Result{}
			cmd.checkKVStatusAndBucket(check, nc)
			assertListIsEmpty(t, check.Warnings)
			assertListEquals(t, check.OKs, "bucket TEST")
			assertListEquals(t, check.Criticals, "key KEY not found")
		})
	})
}

func TestCheckAccountInfo(t *testing.T) {
	setDefaults := func() (*SrvCheckCmd, *api.JetStreamAccountStats) {
		// cli defaults
		cmd := &SrvCheckCmd{
			jsConsumersCritical: -1,
			jsConsumersWarn:     -1,
			jsStreamsCritical:   -1,
			jsStreamsWarn:       -1,
			jsStoreCritical:     90,
			jsStoreWarn:         75,
			jsMemCritical:       90,
			jsMemWarn:           75,
		}

		info := &api.JetStreamAccountStats{
			JetStreamTier: api.JetStreamTier{
				Memory:    128,
				Store:     1024,
				Streams:   10,
				Consumers: 100,
				Limits: api.JetStreamAccountLimits{
					MaxMemory:    1024,
					MaxStore:     20480,
					MaxStreams:   200,
					MaxConsumers: 1000,
				},
			},
		}

		return cmd, info
	}

	t.Run("No info", func(t *testing.T) {
		cmd, _ := setDefaults()
		check := &monitor.Result{}
		err := cmd.checkAccountInfo(check, nil)
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("No limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		info.Limits = api.JetStreamAccountLimits{}
		check := &monitor.Result{}
		assertNoError(t, cmd.checkAccountInfo(check, info))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "memory=128B memory_pct=0%;75;90 storage=1024B storage_pct=0%;75;90 streams=10 streams_pct=0% consumers=100 consumers_pct=0%")
	})

	t.Run("Limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		check := &monitor.Result{}
		assertNoError(t, cmd.checkAccountInfo(check, info))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%")
	})

	t.Run("Limits, Thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		t.Run("Usage exceeds max", func(t *testing.T) {
			info.Streams = 300
			check := &monitor.Result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertListEquals(t, check.Criticals, "streams: exceed server limits")
			assertListIsEmpty(t, check.Warnings)
			assertHasPDItem(t, check, "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=300 streams_pct=150% consumers=100 consumers_pct=10%")
		})

		t.Run("Invalid thresholds", func(t *testing.T) {
			cmd, info := setDefaults()

			cmd.jsMemWarn = 90
			cmd.jsMemCritical = 80
			check := &monitor.Result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertListEquals(t, check.Criticals, "memory: invalid thresholds")
		})

		t.Run("Exceeds warning threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 800
			check := &monitor.Result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertHasPDItem(t, check, "memory=800B memory_pct=78%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%")
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.Warnings, "78% memory")
		})

		t.Run("Exceeds critical threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 960
			check := &monitor.Result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertHasPDItem(t, check, "memory=960B memory_pct=93%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%")
			assertListEquals(t, check.Criticals, "93% memory")
			assertListIsEmpty(t, check.Warnings)
		})
	})
}

func TestCheckMirror(t *testing.T) {
	cmd := &SrvCheckCmd{}
	info := &api.StreamInfo{}
	cmd.sourcesSeenCritical = time.Second
	cmd.sourcesLagCritical = 50

	t.Run("no mirror", func(t *testing.T) {
		check := &monitor.Result{}
		assertNoError(t, cmd.checkMirror(check, info))
		assertListEquals(t, check.Criticals, "not mirrored")
		assertListIsEmpty(t, check.Warnings)
	})

	t.Run("failed mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{Name: "M", Lag: 100, Active: time.Hour}
		check := &monitor.Result{}
		err := cmd.checkMirror(check, info)
		checkErr(t, err, "unexpected error")
		assertHasPDItem(t, check, "lag=100;;50 active=3600.0000s;;1.0000")
		assertListEquals(t, check.Criticals, "100 messages behind", "last active 1h0m0s")
		assertListIsEmpty(t, check.Warnings)
	})

	t.Run("ok mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{Name: "M", Lag: 1, Active: 10 * time.Millisecond}
		check := &monitor.Result{}
		assertNoError(t, cmd.checkMirror(check, info))
		assertHasPDItem(t, check, "lag=1;;50 active=0.0100s;;1.0000")
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
	})
}

func TestCheckSources(t *testing.T) {
	cmd := &SrvCheckCmd{}
	info := &api.StreamInfo{}

	t.Run("no sources", func(t *testing.T) {
		check := &monitor.Result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "no sources defined")
	})

	cmd.sourcesLagCritical = 10
	cmd.sourcesSeenCritical = time.Second
	cmd.sourcesMaxSources = 10
	cmd.sourcesMinSources = 1

	t.Run("lagged source", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{Name: "s1", Lag: 1000, Active: time.Millisecond},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 lagged sources")
		assertHasPDItem(t, check, "sources=1;1;10", "sources_lagged=1", "sources_inactive=0")
	})

	t.Run("inactive source", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{Name: "s1", Active: time.Hour},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 inactive sources")
		assertHasPDItem(t, check, "sources=1;1;10", "sources_lagged=0", "sources_inactive=1")
	})

	t.Run("not enough sources", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{Name: "s1", Active: time.Millisecond},
			},
		}

		cmd.sourcesMinSources = 2

		check := &monitor.Result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 sources of min expected 2")
		assertHasPDItem(t, check, "sources=1;2;10", "sources_lagged=0", "sources_inactive=0")
	})

	t.Run("too many sources", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{Name: "s1", Active: time.Millisecond},
				{Name: "s2", Active: time.Millisecond},
			},
		}

		cmd.sourcesMinSources = 1
		cmd.sourcesMaxSources = 1

		check := &monitor.Result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "2 sources of max expected 1")
		assertHasPDItem(t, check, "sources=2;1;1", "sources_lagged=0", "sources_inactive=0")
	})
}

func TestCheckVarz(t *testing.T) {
	t.Run("nil data", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}

		err := cmd.checkVarz(&monitor.Result{}, nil)
		if err.Error() != "no data received" {
			t.Fatalf("expected no data error: %s", err)
		}
	})

	t.Run("wrong server", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "other"}

		err := cmd.checkVarz(&monitor.Result{}, vz)
		if err.Error() != "result from other" {
			t.Fatalf("expected error about host: %s", err)
		}
	})

	t.Run("jetstream", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvJSRequired = true
		vz := &server.Varz{Name: "testing"}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "JetStream not enabled")
		vz.JetStream.Config = &server.JetStreamConfig{}
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "JetStream enabled")
	})

	t.Run("tls", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvTLSRequired = true
		vz := &server.Varz{Name: "testing"}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "TLS not required")

		vz.TLSRequired = true
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "TLS required")
	})

	t.Run("authentication", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvAuthRequire = true
		vz := &server.Varz{Name: "testing"}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Authentication not required")

		vz.AuthRequired = true
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "Authentication required")
	})

	t.Run("uptime", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "testing"}

		// invalid thresholds
		check := &monitor.Result{}
		cmd.srvUptimeCrit = 20 * time.Minute
		cmd.srvUptimeWarn = 10 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up invalid thresholds")
		assertListIsEmpty(t, check.OKs)

		// critical uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-1 * time.Second)
		cmd.srvUptimeCrit = 10 * time.Minute
		cmd.srvUptimeWarn = 20 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up 1.00s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=1.0000s;1200.0000;600.000")

		// critical uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-599 * time.Second)
		cmd.srvUptimeCrit = 10 * time.Minute
		cmd.srvUptimeWarn = 20 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up 9m59s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=599.0000s;1200.0000;600.000")

		// critical uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-600 * time.Second)
		cmd.srvUptimeCrit = 10 * time.Minute
		cmd.srvUptimeWarn = 20 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up 10m0s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=600.0000s;1200.0000;600.000")

		// critical -> warning uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-601 * time.Second)
		cmd.srvUptimeCrit = 10 * time.Minute
		cmd.srvUptimeWarn = 20 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.Warnings, "Up 10m1s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=601.0000s;1200.0000;600.000")

		// warning uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-1199 * time.Second)
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.Warnings, "Up 19m59s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=1199.0000s;1200.0000;600.000")

		// warning uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-1200 * time.Second)
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.Warnings, "Up 20m0s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=1200.0000s;1200.0000;600.000")

		// ok uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-1201 * time.Second)
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertListEquals(t, check.OKs, "Up 20m1s")
		assertHasPDItem(t, check, "uptime=1201.0000s;1200.0000;600.0000")

		// ok uptime
		check = &monitor.Result{}
		vz.Now = time.Now()
		vz.Start = vz.Now.Add(-1260 * time.Second)
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertListEquals(t, check.OKs, "Up 21m0s")
		assertHasPDItem(t, check, "uptime=1260.0000s;1200.0000;600.0000")
	})

	t.Run("cpu", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "testing", CPU: 50}

		// invalid thresholds
		cmd.srvCPUCrit = 60
		cmd.srvCPUWarn = 70
		check := &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "CPU invalid thresholds")
		assertListIsEmpty(t, check.OKs)

		// critical cpu
		cmd.srvCPUCrit = 50
		cmd.srvCPUWarn = 30
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "CPU 50.00")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "cpu=50%;30;50")

		// warning cpu
		cmd.srvCPUCrit = 60
		cmd.srvCPUWarn = 50
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "CPU 50.00")
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "cpu=50%;50;60")

		// ok cpu
		cmd.srvCPUCrit = 80
		cmd.srvCPUWarn = 70
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.OKs, "CPU 50.00")
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "cpu=50%;70;80")
	})

	// memory not worth testing, its the same logic as CPU

	t.Run("connections", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "testing", Connections: 1024}

		// critical connections
		cmd.srvConnCrit = 1024
		cmd.srvConnWarn = 800
		check := &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "connections=1024;800;1024")

		// critical connections reverse
		cmd.srvConnCrit = 1200
		cmd.srvConnWarn = 1300
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "connections=1024;1300;1200")

		// warn connections
		cmd.srvConnCrit = 2000
		cmd.srvConnWarn = 1024
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1024;2000")

		// warn connections reverse
		cmd.srvConnCrit = 1000
		cmd.srvConnWarn = 1300
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1300;1000")

		// ok connections
		cmd.srvConnCrit = 2000
		cmd.srvConnWarn = 1300
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.OKs, "Connections 1024.00")
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1300;2000")

		// ok connections reverse
		cmd.srvConnCrit = 800
		cmd.srvConnWarn = 900
		check = &monitor.Result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "Connections 1024.00")
		assertHasPDItem(t, check, "connections=1024;900;800")
	})
}

func TestCheckCredential(t *testing.T) {
	noExpiry := `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJBSUdIM0I2TEFGQkMzNktaSFJCSFI1QVZaTVFHQkdDS0NRTlNXRFBMN0U1NE5SM0I1SkxRIiwiaWF0IjoxNjk1MzY5NjU1LCJpc3MiOiJBRFFCT1haQTZaWk5MMko0VFpZNTZMUVpUN1FCVk9DNDVLVlQ3UDVNWkZVWU1LSVpaTUdaSE02QSIsIm5hbWUiOiJib2IiLCJzdWIiOiJVQkhPVDczREVGN1dZWUZUS1ZVSDZNWDNFUUVZSlFWWUNBRUJXUFJaSDNYR0E2WDdLRDNGUkFYSCIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfX0.kGsxvI3NNNp60unItd-Eo1Yw6B9T3rBOeq7lvRY_klP5yTaBZwhCTKUNYdr_n2HNkCNB44fyW2_pmBhDki_CDQ
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAIQJDZJGYOJN4NBOLYRRENCNTPXZ7PPVQW7RWEXWJUNBAFDRPDO27JWA
------END USER NKEY SEED------

*************************************************************`

	expires2100 := `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJleHAiOjQxMDI0NDQ4MDAsImp0aSI6IlhRQkNTUUo3M0c3STRWR0JVUUNNQjdKRVlDWlVNUzdLUzJPU0Q1Skk3WjY0NEE0TU40SUEiLCJpYXQiOjE2OTUzNzA4OTcsImlzcyI6IkFERU5CTlBZSUwzTklXVkxCMjJVUU5FR0NMREhGSllNSUxEVEFQSlk1SFlQV05LQVZQNzJXREFSIiwibmFtZSI6ImJvYiIsInN1YiI6IlVCTTdYREtRUzRRQVBKUEFCSllWSU5RR1lETko2R043MjZNQ01DV0VZRDJTTU9GQVZOQ1E1M09IIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.3ytewtkFoRLKNeRJjPGOyNWeeQKqKdfHmyRL2ofaUiqj_OoN2LAmg_Ms2zpU-A_2xAiUH7VsMIRJxw1cx3bwAg
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAKYITMHPMSYUGPNQBLLPGOPFQN44XNCGXHNSHLJJVMD3IKYGBOLAI7TI
------END USER NKEY SEED------

*************************************************************`

	writeCred := func(t *testing.T, cred string) string {
		tf, err := os.CreateTemp("", "")
		assertNoError(t, err)

		tf.Write([]byte(cred))
		tf.Close()

		return tf.Name()
	}

	t.Run("no expiry", func(t *testing.T) {
		cmd := &SrvCheckCmd{}
		cmd.credential = writeCred(t, noExpiry)
		defer func(f string) { os.Remove(f) }(cmd.credential)

		cmd.credentialRequiresExpire = true

		check := &monitor.Result{}
		assertNoError(t, cmd.checkCredential(check))
		assertListEquals(t, check.Criticals, "never expires")
		assertListIsEmpty(t, check.Warnings)

		cmd.credential = writeCred(t, expires2100)
		defer func(f string) { os.Remove(f) }(cmd.credential)

		check = &monitor.Result{}
		assertNoError(t, cmd.checkCredential(check))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertListEquals(t, check.OKs, "expires in 2100-01-01 00:00:00 +0000 UTC")
	})

	t.Run("critical", func(t *testing.T) {
		cmd := &SrvCheckCmd{}
		cmd.credential = writeCred(t, expires2100)

		defer os.Remove(cmd.credential)

		check := &monitor.Result{}
		cmd.credentialValidityCrit = 100 * 24 * 365 * time.Hour

		assertNoError(t, cmd.checkCredential(check))
		assertListEquals(t, check.Criticals, "expires sooner than 100y0d0h0m0s")
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.OKs)
	})

	t.Run("warning", func(t *testing.T) {
		cmd := &SrvCheckCmd{}
		cmd.credential = writeCred(t, expires2100)
		defer os.Remove(cmd.credential)

		check := &monitor.Result{}
		cmd.credentialValidityWarn = 100 * 24 * 365 * time.Hour

		assertNoError(t, cmd.checkCredential(check))
		assertListEquals(t, check.Warnings, "expires sooner than 100y0d0h0m0s")
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.OKs)
	})

}
func TestCheckJSZ(t *testing.T) {
	cmd := &SrvCheckCmd{}

	t.Run("nil meta", func(t *testing.T) {
		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, nil))
		assertListEquals(t, check.Criticals, "no cluster information")
	})

	meta := &server.ClusterInfo{}
	t.Run("no meta leader", func(t *testing.T) {
		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "No leader")
		assertListIsEmpty(t, check.OKs)
	})

	t.Run("invalid peer count", func(t *testing.T) {
		meta = &server.ClusterInfo{Leader: "l1"}
		cmd.raftExpect = 2
		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 peers of expected 2")
	})

	cmd.raftExpect = 3
	cmd.raftSeenCritical = time.Second
	cmd.raftLagCritical = 10

	t.Run("good peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Current: true, Active: 10 * time.Millisecond, Lag: 1},
				{Name: "replica2", Current: true, Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("not current peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Active: 10 * time.Millisecond, Lag: 1},
				{Name: "replica2", Current: true, Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 not current")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=1", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("offline peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Current: true, Offline: true, Active: 10 * time.Millisecond, Lag: 1},
				{Name: "replica2", Current: true, Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 offline")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=1", "peer_not_current=0", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("inactive peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Current: true, Active: 10 * time.Hour, Lag: 1},
				{Name: "replica2", Current: true, Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 inactive more than 1s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=1", "peer_lagged=0")
	})

	t.Run("lagged peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Current: true, Active: 10 * time.Millisecond, Lag: 10000},
				{Name: "replica2", Current: true, Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 lagged more than 10 ops")
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=0", "peer_lagged=1")
	})

	t.Run("multiple errors", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{Name: "replica1", Current: true, Active: 10 * time.Millisecond, Lag: 10000},
				{Name: "replica2", Current: true, Active: 10 * time.Hour, Lag: 1},
				{Name: "replica3", Current: true, Offline: true, Active: 10 * time.Millisecond, Lag: 1},
				{Name: "replica4", Active: 10 * time.Millisecond, Lag: 1},
			},
		}

		check := &monitor.Result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertHasPDItem(t, check, "peers=5;3;3", "peer_offline=1", "peer_not_current=1", "peer_inactive=1", "peer_lagged=1")
		assertListEquals(t, check.Criticals, "5 peers of expected 3",
			"1 not current",
			"1 inactive more than 1s",
			"1 offline",
			"1 lagged more than 10 ops")
	})
}

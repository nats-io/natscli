// Copyright 2020-2024 The NATS Authors
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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/options"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/monitor"
	"github.com/nats-io/nats-server/v2/server"
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

	options.DefaultOptions = &options.Options{}

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

	opts().Conn = nil
	nc, mgr, err := prepareHelper(srv.ClientURL())
	checkErr(t, err, "could not connect client to server @ %s: %v", srv.ClientURL(), err)
	defer nc.Close()

	cb(srv, nc, mgr)
}

func dfltCmd() *SrvCheckCmd {
	return &SrvCheckCmd{kvBucket: "TEST", kvValuesWarn: -1, kvValuesCrit: -1}
}

func TestCheckMessage(t *testing.T) {
	t.Run("Body timestamp", func(t *testing.T) {
		withJetStream(t, func(_ *server.Server, nc *nats.Conn, mgr *jsm.Manager) {
			cmd := dfltCmd()
			check := &monitor.Result{}

			opts().Conn = nc
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

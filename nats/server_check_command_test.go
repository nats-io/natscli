package main

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
)

func assertHasPDItem(t *testing.T, check *result, items ...string) {
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
			Store:     1024,
			Memory:    128,
			Streams:   10,
			Consumers: 100,
			Limits: api.JetStreamAccountLimits{
				MaxMemory:    1024,
				MaxStore:     20480,
				MaxStreams:   200,
				MaxConsumers: 1000,
			},
		}

		return cmd, info
	}

	t.Run("No info", func(t *testing.T) {
		cmd, _ := setDefaults()
		check := &result{}
		err := cmd.checkAccountInfo(check, nil)
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("No limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		info.Limits = api.JetStreamAccountLimits{}
		check := &result{}
		assertNoError(t, cmd.checkAccountInfo(check, info))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "memory=128B memory_pct=0%;75;90 storage=1024B storage_pct=0%;75;90 streams=10 streams_pct=0% consumers=100 consumers_pct=0%")
	})

	t.Run("Limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		check := &result{}
		assertNoError(t, cmd.checkAccountInfo(check, info))
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%")
	})

	t.Run("Limits, Thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		t.Run("Usage exceeds max", func(t *testing.T) {
			info.Streams = 300
			check := &result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertListEquals(t, check.Criticals, "streams: exceed server limits")
			assertListIsEmpty(t, check.Warnings)
			assertHasPDItem(t, check, "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=300 streams_pct=150% consumers=100 consumers_pct=10%")
		})

		t.Run("Invalid thresholds", func(t *testing.T) {
			cmd, info := setDefaults()

			cmd.jsMemWarn = 90
			cmd.jsMemCritical = 80
			check := &result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertListEquals(t, check.Criticals, "memory: invalid thresholds")
		})

		t.Run("Exceeds warning threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 800
			check := &result{}
			assertNoError(t, cmd.checkAccountInfo(check, info))
			assertHasPDItem(t, check, "memory=800B memory_pct=78%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%")
			assertListIsEmpty(t, check.Criticals)
			assertListEquals(t, check.Warnings, "78% memory")
		})

		t.Run("Exceeds critical threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 960
			check := &result{}
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
		check := &result{}
		assertNoError(t, cmd.checkMirror(check, info))
		assertListEquals(t, check.Criticals, "not mirrored")
		assertListIsEmpty(t, check.Warnings)
	})

	t.Run("failed mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{"M", nil, 100, time.Hour, nil}
		check := &result{}
		err := cmd.checkMirror(check, info)
		checkErr(t, err, "unexpected error")
		assertHasPDItem(t, check, "lag=100;;50 active=3600.0000s;;1.0000")
		assertListEquals(t, check.Criticals, "100 messages behind", "last active 1h0m0s")
		assertListIsEmpty(t, check.Warnings)
	})

	t.Run("ok mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{"M", nil, 1, 10 * time.Millisecond, nil}
		check := &result{}
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
		check := &result{}
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
				{"s1", nil, 1000, time.Millisecond, nil},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 lagged sources")
		assertHasPDItem(t, check, "sources=1;1;10", "sources_lagged=1", "sources_inactive=0")
	})

	t.Run("inactive source", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{"s1", nil, 0, time.Hour, nil},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 inactive sources")
		assertHasPDItem(t, check, "sources=1;1;10", "sources_lagged=0", "sources_inactive=1")
	})

	t.Run("not enough sources", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{"s1", nil, 0, time.Millisecond, nil},
			},
		}

		cmd.sourcesMinSources = 2

		check := &result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "1 sources of min expected 2")
		assertHasPDItem(t, check, "sources=1;2;10", "sources_lagged=0", "sources_inactive=0")
	})

	t.Run("too many sources", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{"s1", nil, 0, time.Millisecond, nil},
				{"s2", nil, 0, time.Millisecond, nil},
			},
		}

		cmd.sourcesMinSources = 1
		cmd.sourcesMaxSources = 1

		check := &result{}
		assertNoError(t, cmd.checkSources(check, info))
		assertListEquals(t, check.Criticals, "2 sources of max expected 1")
		assertHasPDItem(t, check, "sources=2;1;1", "sources_lagged=0", "sources_inactive=0")
	})
}

func TestCheckVarz(t *testing.T) {
	t.Run("nil data", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}

		err := cmd.checkVarz(&result{}, nil)
		if err.Error() != "no data received" {
			t.Fatalf("expected no data error: %s", err)
		}
	})

	t.Run("wrong server", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "other"}

		err := cmd.checkVarz(&result{}, vz)
		if err.Error() != "result from other" {
			t.Fatalf("expected error about host: %s", err)
		}
	})

	t.Run("jetstream", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvJSRequired = true
		vz := &server.Varz{Name: "testing"}

		check := &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "JetStream not enabled")
		vz.JetStream.Config = &server.JetStreamConfig{}
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "JetStream enabled")
	})

	t.Run("tls", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvTLSRequired = true
		vz := &server.Varz{Name: "testing"}

		check := &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "TLS not required")

		vz.TLSRequired = true
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "TLS required")
	})

	t.Run("authentication", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		cmd.srvAuthRequire = true
		vz := &server.Varz{Name: "testing"}

		check := &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Authentication not required")

		vz.AuthRequired = true
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "Authentication required")
	})

	t.Run("uptime", func(t *testing.T) {
		cmd := &SrvCheckCmd{srvName: "testing"}
		vz := &server.Varz{Name: "testing", Start: time.Now().Add(-1 * time.Second)}

		// invalid thresholds
		check := &result{}
		cmd.srvUptimeCrit = 20 * time.Minute
		cmd.srvUptimeWarn = 10 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up invalid thresholds")
		assertListIsEmpty(t, check.OKs)

		// critical uptime
		check = &result{}
		cmd.srvUptimeCrit = 10 * time.Minute
		cmd.srvUptimeWarn = 20 * time.Minute
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Up 1.00s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=1.0000s;1200.0000;600.000")

		// warning uptime
		check = &result{}
		vz.Start = time.Now().Add(-11 * time.Minute)
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.Warnings, "Up 11m0s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "uptime=660.0000s;1200.0000;600.000")

		// ok uptime
		check = &result{}
		vz.Start = time.Now().Add(-21 * time.Minute)
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
		check := &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "CPU invalid thresholds")
		assertListIsEmpty(t, check.OKs)

		// critical cpu
		cmd.srvCPUCrit = 50
		cmd.srvCPUWarn = 30
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "CPU 50.00")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "cpu=50%;30;50")

		// warning cpu
		cmd.srvCPUCrit = 60
		cmd.srvCPUWarn = 50
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "CPU 50.00")
		assertListIsEmpty(t, check.Criticals)
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "cpu=50%;50;60")

		// ok cpu
		cmd.srvCPUCrit = 80
		cmd.srvCPUWarn = 70
		check = &result{}
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
		check := &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "connections=1024;800;1024")

		// critical connections reverse
		cmd.srvConnCrit = 1200
		cmd.srvConnWarn = 1300
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Criticals, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Warnings)
		assertHasPDItem(t, check, "connections=1024;1300;1200")

		// warn connections
		cmd.srvConnCrit = 2000
		cmd.srvConnWarn = 1024
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1024;2000")

		// warn connections reverse
		cmd.srvConnCrit = 1000
		cmd.srvConnWarn = 1300
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.Warnings, "Connections 1024.00")
		assertListIsEmpty(t, check.OKs)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1300;1000")

		// ok connections
		cmd.srvConnCrit = 2000
		cmd.srvConnWarn = 1300
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListEquals(t, check.OKs, "Connections 1024.00")
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "connections=1024;1300;2000")

		// ok connections reverse
		cmd.srvConnCrit = 800
		cmd.srvConnWarn = 900
		check = &result{}
		assertNoError(t, cmd.checkVarz(check, vz))
		assertListIsEmpty(t, check.Warnings)
		assertListIsEmpty(t, check.Criticals)
		assertListEquals(t, check.OKs, "Connections 1024.00")
		assertHasPDItem(t, check, "connections=1024;900;800")
	})
}

func TestCheckJSZ(t *testing.T) {
	cmd := &SrvCheckCmd{}

	t.Run("nil meta", func(t *testing.T) {
		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, nil))
		assertListEquals(t, check.Criticals, "no cluster information")
	})

	meta := &server.ClusterInfo{}
	t.Run("no meta leader", func(t *testing.T) {
		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "No leader")
		assertListIsEmpty(t, check.OKs)
	})

	t.Run("invalid peer count", func(t *testing.T) {
		meta = &server.ClusterInfo{Leader: "l1"}
		cmd.raftExpect = 2
		check := &result{}
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
				{"replica1", true, false, 10 * time.Millisecond, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListIsEmpty(t, check.Criticals)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("not current peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", false, false, 10 * time.Millisecond, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 not current")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=1", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("offline peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, true, 10 * time.Millisecond, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 offline")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=1", "peer_not_current=0", "peer_inactive=0", "peer_lagged=0")
	})

	t.Run("inactive peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, false, 10 * time.Hour, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 inactive more than 1s")
		assertListIsEmpty(t, check.OKs)
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=1", "peer_lagged=0")
	})

	t.Run("lagged peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, false, 10 * time.Millisecond, 10000},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertListEquals(t, check.Criticals, "1 lagged more than 10 ops")
		assertHasPDItem(t, check, "peers=3;3;3", "peer_offline=0", "peer_not_current=0", "peer_inactive=0", "peer_lagged=1")
	})

	t.Run("multiple errors", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, false, 10 * time.Millisecond, 10000},
				{"replica2", true, false, 10 * time.Hour, 1},
				{"replica3", true, true, 10 * time.Millisecond, 1},
				{"replica4", false, false, 10 * time.Millisecond, 1},
			},
		}

		check := &result{}
		assertNoError(t, cmd.checkClusterInfo(check, meta))
		assertHasPDItem(t, check, "peers=5;3;3", "peer_offline=1", "peer_not_current=1", "peer_inactive=1", "peer_lagged=1")
		assertListEquals(t, check.Criticals, "5 peers of expected 3",
			"1 not current",
			"1 inactive more than 1s",
			"1 offline",
			"1 lagged more than 10 ops")
	})
}

package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
)

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
		_, _, _, err := cmd.checkAccountInfo(nil)
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("No limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		info.Limits = api.JetStreamAccountLimits{}
		warns, crits, pd, err := cmd.checkAccountInfo(info)
		checkErr(t, err, "unexpected error")
		if len(crits) > 0 {
			t.Fatalf("unexpected crits: %v", crits)
		}
		if len(warns) > 0 {
			t.Fatalf("unexpected warns: %v", warns)
		}
		if pd != "memory=128B memory_pct=0%;75;90 storage=1024B storage_pct=0%;75;90 streams=10 streams_pct=0% consumers=100 consumers_pct=0%" {
			t.Fatalf("unexpected pd: %s", pd)
		}
	})

	t.Run("Limits, default thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		warns, crits, pd, err := cmd.checkAccountInfo(info)
		checkErr(t, err, "unexpected error")
		if len(crits) > 0 {
			t.Fatalf("unexpected crits: %v", crits)
		}
		if len(warns) > 0 {
			t.Fatalf("unexpected warns: %v", warns)
		}
		if pd != "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%" {
			t.Fatalf("unexpected pd: %s", pd)
		}
	})

	t.Run("Limits, Thresholds", func(t *testing.T) {
		cmd, info := setDefaults()

		t.Run("Usage exceeds max", func(t *testing.T) {
			info.Streams = 300
			warns, crits, pd, err := cmd.checkAccountInfo(info)
			checkErr(t, err, "unexpected error")
			if !cmp.Equal(crits, []string{"streams: exceed server limits"}) {
				t.Fatalf("unexpected crits: %v", crits)
			}
			if len(warns) > 0 {
				t.Fatalf("unexpected warns: %v", warns)
			}
			if pd != "memory=128B memory_pct=12%;75;90 storage=1024B storage_pct=5%;75;90 streams=300 streams_pct=150% consumers=100 consumers_pct=10%" {
				t.Fatalf("unexpected pd: %s", pd)
			}
		})

		t.Run("Invalid thresholds", func(t *testing.T) {
			cmd, info := setDefaults()

			cmd.jsMemWarn = 90
			cmd.jsMemCritical = 80
			_, crits, _, err := cmd.checkAccountInfo(info)
			checkErr(t, err, "unexpected error")
			if !cmp.Equal(crits, []string{"memory: invalid thresholds"}) {
				t.Fatalf("unexpected crits: %v", crits)
			}
		})

		t.Run("Exceeds warning threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 800
			warns, crits, pd, err := cmd.checkAccountInfo(info)
			checkErr(t, err, "unexpected error")
			if pd != "memory=800B memory_pct=78%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%" {
				t.Fatalf("unexpected pd: %s", pd)
			}
			if len(crits) > 0 {
				t.Fatalf("unexpected crits: %v", crits)
			}
			if !cmp.Equal(warns, []string{"78% memory"}) {
				t.Fatalf("unexpected warns: %v", warns)
			}
		})

		t.Run("Exceeds critical threshold", func(t *testing.T) {
			cmd, info := setDefaults()

			info.Memory = 960
			warns, crits, pd, err := cmd.checkAccountInfo(info)
			checkErr(t, err, "unexpected error")
			if pd != "memory=960B memory_pct=93%;75;90 storage=1024B storage_pct=5%;75;90 streams=10 streams_pct=5% consumers=100 consumers_pct=10%" {
				t.Fatalf("unexpected pd: %s", pd)
			}
			if !cmp.Equal(crits, []string{"93% memory"}) {
				t.Fatalf("unexpected crits: %v", crits)
			}
			if len(warns) > 0 {
				t.Fatalf("unexpected warns: %v", warns)
			}
		})
	})
}

func TestCheckMirror(t *testing.T) {
	cmd := &SrvCheckCmd{}
	info := &api.StreamInfo{}
	cmd.sourcesSeenCritical = time.Second
	cmd.sourcesLagCritical = 50

	t.Run("no mirror", func(t *testing.T) {
		crits, _, err := cmd.checkMirror(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"not mirrored"}) {
			t.Fatalf("expected no mirror error got %+v", crits)
		}
	})

	t.Run("failed mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{"M", nil, 100, time.Hour, nil}
		crits, pd, err := cmd.checkMirror(info)
		checkErr(t, err, "unexpected error")
		if pd != "lag=100;;50 active=3600.000000s;;1.00" {
			t.Fatalf("invalid pd: %s", pd)
		}
		if !cmp.Equal(crits, []string{"100 messages behind", "last active 1h0m0s"}) {
			t.Fatalf("unexpected crits got %+v", crits)
		}
	})

	t.Run("ok mirror", func(t *testing.T) {
		info.Mirror = &api.StreamSourceInfo{"M", nil, 1, 10 * time.Millisecond, nil}
		crits, pd, err := cmd.checkMirror(info)
		checkErr(t, err, "unexpected error")
		if pd != "lag=1;;50 active=0.010000s;;1.00" {
			t.Fatalf("invalid pd: %s", pd)
		}
		if len(crits) > 0 {
			t.Fatalf("expected no crits got %+v", crits)
		}
	})
}

func TestCheckSources(t *testing.T) {
	cmd := &SrvCheckCmd{}
	info := &api.StreamInfo{}

	t.Run("no sources", func(t *testing.T) {
		crits, _, err := cmd.checkSources(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"no sources defined"}) {
			t.Fatalf("expected no sources error got %+v", crits)
		}
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

		crits, pd, err := cmd.checkSources(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 lagged sources"}) {
			t.Fatalf("expected lagged error got %+v", crits)
		}
		if pd != "sources=1;1;10; lagged=1 inactive=0" {
			t.Fatalf("unexpected performance data: %s", pd)
		}
	})

	t.Run("inactive source", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{"s1", nil, 0, time.Hour, nil},
			},
		}

		crits, pd, err := cmd.checkSources(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 inactive sources"}) {
			t.Fatalf("expected inactive error got %+v", crits)
		}
		if pd != "sources=1;1;10; lagged=0 inactive=1" {
			t.Fatalf("unexpected performance data: %s", pd)
		}
	})

	t.Run("not enough sources", func(t *testing.T) {
		info = &api.StreamInfo{
			Sources: []*api.StreamSourceInfo{
				{"s1", nil, 0, time.Millisecond, nil},
			},
		}

		cmd.sourcesMinSources = 2

		crits, pd, err := cmd.checkSources(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 sources of min expected 2"}) {
			t.Fatalf("expected min error got %+v", crits)
		}
		if pd != "sources=1;2;10; lagged=0 inactive=0" {
			t.Fatalf("unexpected performance data: %s", pd)
		}
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

		crits, pd, err := cmd.checkSources(info)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"2 sources of max expected 1"}) {
			t.Fatalf("expected max error got %+v", crits)
		}
		if pd != "sources=2;1;1; lagged=0 inactive=0" {
			t.Fatalf("unexpected performance data: %s", pd)
		}
	})
}

func TestCheckJSZ(t *testing.T) {
	cmd := &SrvCheckCmd{}

	t.Run("nil meta", func(t *testing.T) {
		crits, _, err := cmd.checkClusterInfo(nil)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"no cluster information"}) {
			t.Fatalf("expected api error got %+v", crits)
		}
	})

	meta := &server.ClusterInfo{}
	t.Run("no meta leader", func(t *testing.T) {
		crits, _, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"No leader"}) {
			t.Fatalf("expected no meta leader got %+v", crits)
		}
	})

	t.Run("invalid peer count", func(t *testing.T) {
		meta = &server.ClusterInfo{Leader: "l1"}
		cmd.raftExpect = 2
		crits, _, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 peers of expected 2"}) {
			t.Fatalf("expected peer error got %+v", crits)
		}
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

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if len(crits) > 0 {
			t.Fatalf("unexpected criticals %+v", crits)
		}
		if pb != "peers=3;3;3  offline=0 not_current=0 inactive=0 lagged=0" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
	})

	t.Run("not current peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", false, false, 10 * time.Millisecond, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 not current"}) {
			t.Fatalf("expected not current error error got %+v", crits)
		}
		if pb != "peers=3;3;3  offline=0 not_current=1 inactive=0 lagged=0" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
	})

	t.Run("offline peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, true, 10 * time.Millisecond, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 offline"}) {
			t.Fatalf("expected offline error error got %+v", crits)
		}
		if pb != "peers=3;3;3  offline=1 not_current=0 inactive=0 lagged=0" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
	})

	t.Run("inactive peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, false, 10 * time.Hour, 1},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 inactive more than 1s"}) {
			t.Fatalf("expected inactive error error got %+v", crits)
		}
		if pb != "peers=3;3;3  offline=0 not_current=0 inactive=1 lagged=0" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
	})

	t.Run("lagged peer", func(t *testing.T) {
		meta = &server.ClusterInfo{
			Leader: "l1",
			Replicas: []*server.PeerInfo{
				{"replica1", true, false, 10 * time.Millisecond, 10000},
				{"replica2", true, false, 10 * time.Millisecond, 1},
			},
		}

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		if !cmp.Equal(crits, []string{"1 lagged more than 10 ops"}) {
			t.Fatalf("expected lagged error error got %+v", crits)
		}
		if pb != "peers=3;3;3  offline=0 not_current=0 inactive=0 lagged=1" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
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

		crits, pb, err := cmd.checkClusterInfo(meta)
		checkErr(t, err, "unexpected error")
		expected := []string{
			"5 peers of expected 3",
			"1 not current",
			"1 inactive more than 1s",
			"1 offline",
			"1 lagged more than 10 ops",
		}
		if !cmp.Equal(crits, expected) {
			t.Fatalf("expected errors got %+v", crits)
		}
		if pb != "peers=5;3;3  offline=1 not_current=1 inactive=1 lagged=1" {
			t.Fatalf("unexpected performance data: %s", pb)
		}
	})
}

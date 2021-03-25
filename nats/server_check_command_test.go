package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
)

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
		info.Mirror = &api.StreamSourceInfo{"M", 100, time.Hour, nil}
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
		info.Mirror = &api.StreamSourceInfo{"M", 1, 10 * time.Millisecond, nil}
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
				{"s1", 1000, time.Millisecond, nil},
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
				{"s1", 0, time.Hour, nil},
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
				{"s1", 0, time.Millisecond, nil},
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
				{"s1", 0, time.Millisecond, nil},
				{"s2", 0, time.Millisecond, nil},
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

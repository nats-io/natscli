// Copyright 2019-2022 The NATS Authors
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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go/api"
)

func TestParseStringAsBytes(t *testing.T) {
	cases := []struct {
		input  string
		expect int64
		error  bool
	}{
		{input: "1", expect: 1},
		{input: "1000", expect: 1000},
		{input: "1K", expect: 1024},
		{input: "1k", expect: 1024},
		{input: "1KB", expect: 1024},
		{input: "1KiB", expect: 1024},
		{input: "1kb", expect: 1024},
		{input: "1M", expect: 1024 * 1024},
		{input: "1MB", expect: 1024 * 1024},
		{input: "1MiB", expect: 1024 * 1024},
		{input: "1m", expect: 1024 * 1024},
		{input: "1G", expect: 1024 * 1024 * 1024},
		{input: "1GB", expect: 1024 * 1024 * 1024},
		{input: "1GiB", expect: 1024 * 1024 * 1024},
		{input: "1g", expect: 1024 * 1024 * 1024},
		{input: "1T", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1TB", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1TiB", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1t", expect: 1024 * 1024 * 1024 * 1024},
		{input: "-1", expect: -1},
		{input: "-10", expect: -1},
		{input: "-10GB", expect: -1},
		{input: "1B", error: true},
		{input: "1FOO", error: true},
		{input: "FOO", error: true},
	}

	for _, c := range cases {
		v, err := parseStringAsBytes(c.input)
		if c.error {
			if !errors.Is(err, errInvalidByteString) {
				t.Fatalf("expected an invalid bytes error got: %v", err)
			}
		} else {
			if err != nil {
				t.Fatalf("did not expect an error parsing %v: %v", c.input, err)
			}
			if v != c.expect {
				t.Fatalf("expected %v to parse as %d got %d", c.input, c.expect, v)
			}
		}
	}
}

func TestSplitString(t *testing.T) {
	for _, s := range []string{"x y", "x	y", "x  y", "x,y", "x, y"} {
		parts := splitString(s)
		if parts[0] != "x" && parts[1] != "y" {
			t.Fatalf("Expected x and y from %s, got %v", s, parts)
		}
	}

	parts := splitString("x foo.*")
	if parts[0] != "x" && parts[1] != "y" {
		t.Fatalf("Expected x and foo.* from 'x foo.*', got %v", parts)
	}
}

func TestRandomString(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if len(randomString(1024, 1024)) != 1024 {
			t.Fatalf("got a !1024 length string")
		}
	}

	for i := 0; i < 1000; i++ {
		n := randomString(2024, 1024)
		if len(n) > 2024 {
			t.Fatalf("got a > 2024 length string")
		}

		if len(n) < 1024 {
			t.Fatalf("got a < 1024 length string (%d)", len(n))
		}
	}

	for i := 0; i < 1000; i++ {
		n := randomString(1024, 2024)
		if len(n) > 2024 {
			t.Fatalf("got a > 2024 length string")
		}

		if len(n) < 1024 {
			t.Fatalf("got a < 1024 length string (%d)", len(n))
		}
	}
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

func TestHostnameCompactor(t *testing.T) {
	names := []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-1.broker-broker-ss.choria.svc.cluster.local",
	}

	result := compactStrings(names)
	if !cmp.Equal(result, []string{"broker-broker-2", "broker-broker-0", "broker-broker-1"}) {
		t.Fatalf("Recevied %#v", result)
	}

	names = []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local1",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local2",
		"broker-broker-1.broker-broker-ss.choria.svc.cluster.local3",
	}
	result = compactStrings(names)
	if !cmp.Equal(result, names) {
		t.Fatalf("Recevied %#v", result)
	}

	names = []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-1.broker-broker-ss.other.svc.cluster.local",
	}
	result = compactStrings(names)
	if !cmp.Equal(result, []string{"broker-broker-2.broker-broker-ss.choria", "broker-broker-0.broker-broker-ss.choria", "broker-broker-1.broker-broker-ss.other"}) {
		t.Fatalf("Recevied %#v", result)
	}
}

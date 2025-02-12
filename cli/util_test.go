// Copyright 2019-2025 The NATS Authors
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

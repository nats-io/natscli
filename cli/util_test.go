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
	"testing"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/options"
)

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

func TestNatsOptsConnectTimeout(t *testing.T) {
	// natsOpts reads the global options, so swap in our own for the test
	origOpts := options.DefaultOptions
	t.Cleanup(func() { options.DefaultOptions = origOpts })

	resolve := func(t *testing.T) nats.Options {
		t.Helper()

		res := nats.GetDefaultOptions()
		for _, o := range natsOpts() {
			if err := o(&res); err != nil {
				t.Fatalf("applying connection option: %v", err)
			}
		}

		return res
	}

	options.DefaultOptions = &options.Options{ConnectTimeout: 30 * time.Second}
	if to := resolve(t).Timeout; to != 30*time.Second {
		t.Fatalf("expected a 30s connect timeout, got %v", to)
	}

	// unset should leave the nats.go default in place
	options.DefaultOptions = &options.Options{}
	if to := resolve(t).Timeout; to != nats.GetDefaultOptions().Timeout {
		t.Fatalf("expected the default connect timeout of %v, got %v", nats.GetDefaultOptions().Timeout, to)
	}
}

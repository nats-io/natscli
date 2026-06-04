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

	"github.com/nats-io/jsm.go/api"
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

// Copyright 2024 The NATS Authors
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

package audit

import (
	"strings"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/archive"
	"golang.org/x/exp/maps"
)

func init() {
	MustRegisterCheck(
		Check{
			Code:        "LEAF_001",
			Name:        "Whitespace in leafnode server names",
			Description: "No Leafnode contains whitespace in its name",
			Handler:     checkLeafnodeServerNamesForWhitespace,
		},
	)
}

func checkLeafnodeServerNamesForWhitespace(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		leafnodesWithWhitespace := map[string]struct{}{}

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverLeafz server.Leafz
			err := r.Load(&serverLeafz, clusterTag, serverTag, archive.TagServerLeafs())
			if err != nil {
				logWarning("Artifact 'LEAFZ' is missing for server %s", serverName)
				continue
			}

			for _, leaf := range serverLeafz.Leafs {
				// check if leafnode name contains whitespace
				if strings.ContainsAny(leaf.Name, " \n") {
					leafnodesWithWhitespace[leaf.Name] = struct{}{}
				}
			}
		}

		if len(leafnodesWithWhitespace) > 0 {
			examples.add("Cluster %s: %v", clusterName, maps.Keys(leafnodesWithWhitespace))
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d clusters with leafnode names containing whitespace", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

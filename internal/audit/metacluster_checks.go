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
	"errors"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/archive"
)

func init() {
	MustRegisterCheck(
		Check{
			Code:        "META_001",
			Name:        "Meta cluster offline replicas",
			Description: "All nodes part of the meta group are online",
			Handler:     checkMetaClusterOfflineReplicas,
		},
		Check{
			Code:        "META_002",
			Name:        "Meta cluster leader",
			Description: "All nodes part of the meta group agree on the meta cluster leader",
			Handler:     checkMetaClusterLeader,
		},
	)
}

// checkMetaClusterLeader verify that all server agree on the same meta group leader in each known cluster
func checkMetaClusterLeader(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	serverVarsTag := archive.TagServerVars()
	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		leaderFollowers := make(map[string][]string)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var jetStreamInfo server.JSInfo
			err := r.Load(&jetStreamInfo, clusterTag, serverTag, jsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if jetStreamInfo.Disabled {
				continue
			}

			var serverVarz server.Varz
			err = r.Load(&serverVarz, clusterTag, serverTag, serverVarsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'VARZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			if serverVarz.JetStream.Meta == nil {
				logWarning("%s / %s does not have meta group info", clusterName, serverName)
				continue
			}

			leader := serverVarz.JetStream.Meta.Leader
			if leader == "" {
				leader = "NO_LEADER"
			}

			_, present := leaderFollowers[leader]
			if !present {
				leaderFollowers[leader] = make([]string, 0)
			}
			leaderFollowers[leader] = append(leaderFollowers[leader], serverName)
		}

		if len(leaderFollowers) > 1 {
			examples.add("Members of %s disagree on meta leader (%v)", clusterName, leaderFollowers)
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d instance of replicas disagreeing on meta-cluster leader", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

// checkMetaClusterOfflineReplicas verify that all meta-cluster replicas are online for each known cluster
func checkMetaClusterOfflineReplicas(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	serverVarsTag := archive.TagServerVars()
	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var jetStreamInfo server.JSInfo
			err := r.Load(&jetStreamInfo, clusterTag, serverTag, jsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if jetStreamInfo.Disabled {
				continue
			}

			var serverVarz server.Varz
			err = r.Load(&serverVarz, clusterTag, serverTag, serverVarsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'VARZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			if serverVarz.JetStream.Meta == nil {
				logWarning("%s / %s does not have meta group info", clusterName, serverName)
				continue
			}

			for _, peerInfo := range serverVarz.JetStream.Meta.Replicas {
				if peerInfo.Offline {
					examples.add(
						"%s - %s reports peer %s as offline",
						clusterName,
						serverName,
						peerInfo.Name,
					)
				}
			}

		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d instance of replicas marked offline by peers", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

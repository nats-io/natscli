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
	"reflect"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/archive"
	"golang.org/x/exp/maps"
)

// checkClusterMemoryUsageOutliers creates a parametrized check to verify the memory usage of any given node in a
// cluster is not significantly higher than its peers
func createCheckClusterMemoryUsageOutliers(outlierThreshold float64) checkFunc {
	return func(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
		typeTag := archive.TagServerVars()
		clusterNames := r.GetClusterNames()

		clustersWithIssuesMap := make(map[string]any, len(clusterNames))

		for _, clusterName := range clusterNames {
			clusterTag := archive.TagCluster(clusterName)

			serverNames := r.GetClusterServerNames(clusterName)
			clusterMemoryUsageMap := make(map[string]float64, len(serverNames))
			clusterMemoryUsageTotal := float64(0)
			numServers := 0 // cannot use len(serverNames) as some artifacts may be missing

			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var serverVarz server.Varz
				err := r.Load(&serverVarz, clusterTag, serverTag, typeTag)
				if errors.Is(err, archive.ErrNoMatches) {
					logWarning("Artifact 'VARZ' is missing for server %s in cluster %s", serverName, clusterName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load VARZ for server %s in cluster %s: %w", serverName, clusterName, err)
				}

				numServers += 1
				clusterMemoryUsageMap[serverTag.Value] = float64(serverVarz.Mem)
				clusterMemoryUsageTotal += float64(serverVarz.Mem)
			}

			clusterMemoryUsageMean := clusterMemoryUsageTotal / float64(numServers)
			threshold := clusterMemoryUsageMean * outlierThreshold

			for serverName, serverMemoryUsage := range clusterMemoryUsageMap {
				if serverMemoryUsage > threshold {
					examples.add(
						"Cluster %s avg: %s, server %s: %s",
						clusterName,
						humanize.IBytes(uint64(clusterMemoryUsageMean)),
						serverName,
						humanize.IBytes(uint64(serverMemoryUsage)),
					)
					clustersWithIssuesMap[clusterName] = nil
				}
			}
		}

		if len(clustersWithIssuesMap) > 0 {
			logCritical(
				"Servers with memory usage above %.1fX the cluster average: %d in %d clusters",
				outlierThreshold,
				examples.Count(),
				len(clustersWithIssuesMap),
			)
			return PassWithIssues, nil
		}
		return Pass, nil
	}
}

// checkClusterUniformGatewayConfig verify that gateways configuration matches for all nodes in each cluster
func checkClusterUniformGatewayConfig(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		// For each cluster, build a map where they key is a server name in the cluster
		// And the value is a list of configured remote target clusters
		configuredOutboundGateways := make(map[string][]string)
		configuredInboundGateways := make(map[string][]string)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var gateways server.Gatewayz
			if err := r.Load(&gateways, clusterTag, serverTag, archive.TagServerGateways()); errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'GATEWAYZ' is missing for server %s cluster %s", serverName, clusterName)
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load GATEWAYZ for server %s: %w", serverName, err)
			}

			// Create list of configured outbound gateways for this server
			serverConfiguredOutboundGateways := make([]string, 0, len(gateways.OutboundGateways))
			for targetClusterName, outboundGateway := range gateways.OutboundGateways {
				if outboundGateway.IsConfigured {
					serverConfiguredOutboundGateways = append(serverConfiguredOutboundGateways, targetClusterName)
				}
			}

			// Create list of configured inbound gateways for this server
			serverConfiguredInboundGateways := make([]string, 0, len(gateways.OutboundGateways))
			for sourceClusterName, inboundGateways := range gateways.InboundGateways {
				for _, inboundGateway := range inboundGateways {
					if inboundGateway.IsConfigured {
						serverConfiguredInboundGateways = append(serverConfiguredInboundGateways, sourceClusterName)
						break
					}
				}
			}

			// Sort the lists for easier comparison later
			sort.Strings(serverConfiguredOutboundGateways)
			sort.Strings(serverConfiguredInboundGateways)
			// Store for later comparison against other servers in the cluster
			configuredOutboundGateways[serverName] = serverConfiguredOutboundGateways
			configuredInboundGateways[serverName] = serverConfiguredInboundGateways
		}

		gatewayTypes := []struct {
			gatewayType        string
			configuredGateways map[string][]string
		}{
			{"inbound", configuredInboundGateways},
			{"outbound", configuredOutboundGateways},
		}

		for _, t := range gatewayTypes {
			// Check each server configured gateways against another server in the same cluster
			var previousServerName string
			var previousTargetClusterNames []string
			for serverName, targetClusterNames := range t.configuredGateways {
				if previousTargetClusterNames != nil {
					logDebug(
						"Cluster %s - Comparing configured %s gateways of %s (%d) to %s (%d)",
						clusterName,
						t.gatewayType,
						serverName,
						len(targetClusterNames),
						previousServerName,
						len(previousTargetClusterNames),
					)
					if !reflect.DeepEqual(targetClusterNames, previousTargetClusterNames) {
						examples.add(
							"Cluster %s, %s gateways server %s: %v != server %s: %v",
							clusterName,
							t.gatewayType,
							serverName,
							targetClusterNames,
							previousServerName,
							previousTargetClusterNames,
						)
					}
				}
				previousServerName = serverName
				previousTargetClusterNames = targetClusterNames
			}
		}
	}
	if examples.Count() > 0 {
		logCritical("Found %d instance of gateways configurations mismatch", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

// makeCheckClusterHighHAAssets create a parametrized check to verify the number of HA assets is below some the given
// number for each known server in each known cluster
func makeCheckClusterHighHAAssets(haAssetsThreshold int) checkFunc {
	return func(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
		jsTag := archive.TagServerJetStream()

		for _, clusterName := range r.GetClusterNames() {
			clusterTag := archive.TagCluster(clusterName)
			for _, serverName := range r.GetClusterServerNames(clusterName) {
				serverTag := archive.TagServer(serverName)

				var serverJSInfo server.JSInfo
				if err := r.Load(&serverJSInfo, clusterTag, serverTag, jsTag); errors.Is(err, archive.ErrNoMatches) {
					logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
				}

				if serverJSInfo.HAAssets > haAssetsThreshold {
					examples.add("%s: %d HA assets", serverName, serverJSInfo.HAAssets)
				}
			}
		}

		if examples.Count() > 0 {
			logCritical("Found %d servers with >%d HA assets", examples.Count(), haAssetsThreshold)
			return PassWithIssues, nil
		}

		return Pass, nil
	}
}

func checkClusterNamesForWhitespace(reader *archive.Reader, examples *ExamplesCollection) (Outcome, error) {

	for _, clusterName := range reader.GetClusterNames() {
		if strings.Contains(clusterName, " ") {
			examples.add("Cluster: %s", clusterName)
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d clusters with names containing whitespace", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

func checkLeafnodeServerNamesForWhitespace(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {

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
				if strings.Contains(leaf.Name, " ") {
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

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
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/archive"
)

func init() {
	MustRegisterCheck(
		Check{
			Code:        "SERVER_001",
			Name:        "Server Health",
			Description: "All known nodes are healthy",
			Handler:     checkServerHealth,
		},
		Check{
			Code:        "SERVER_002",
			Name:        "Server Version",
			Description: "All known nodes are running the same software version",
			Handler:     checkServerVersion,
		},
		Check{
			Code:        "SERVER_003",
			Name:        "Server CPU Usage",
			Description: "CPU usage for all known nodes is below a given threshold",
			Configuration: map[string]*CheckConfiguration{
				"cpu": {
					Key:         "cpu",
					Description: "CPU Limit Threshold",
					Default:     0.9,
				},
			},
			Handler: checkServerCPUUsage,
		},
		Check{
			Code:        "SERVER_004",
			Name:        "Server Slow Consumers",
			Description: "No node is reporting slow consumers",
			Handler:     checkSlowConsumers,
		},
		Check{
			Code:        "SERVER_005",
			Name:        "Server Resources Limits ",
			Description: "Resource are below a given threshold compared to the configured limit",
			Configuration: map[string]*CheckConfiguration{
				"memory": {
					Key:         "memory",
					Description: "Threshold for memory usage",
					Default:     0.9,
				},
				"store": {
					Key:         "store",
					Description: "Threshold for store usage",
					Default:     0.9,
				},
			},
			Handler: checkServerResourceLimits,
		},
		Check{
			Code:        "SERVER_006",
			Name:        "Whitespace in JetStream domains",
			Description: "No JetStream server is configured with whitespace in its domain",
			Handler:     checkJetStreamDomainsForWhitespace,
		},
	)
}

// checkServerHealth verify all known servers are reporting healthy
func checkServerHealth(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	notHealthy, healthy := 0, 0

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var health server.HealthStatus

			err := r.Load(&health, clusterTag, serverTag, archive.TagServerHealth())
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'HEALTHZ' is missing for server %s", serverName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load health for server %s: %w", serverName, err)
			}

			if health.Status != "ok" {
				examples.add("%s: %d - %s", serverName, health.StatusCode, health.Status)
				notHealthy += 1
			} else {
				healthy += 1
			}
		}
	}

	if notHealthy > 0 {
		logCritical("%d/%d servers are not healthy", notHealthy, healthy+notHealthy)
		return PassWithIssues, nil
	}

	logInfo("%d/%d servers are healthy", healthy, healthy)
	return Pass, nil
}

// checkServerVersions verify all known servers are running the same version
func checkServerVersion(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	versionsToServersMap := make(map[string][]string)

	var lastVersionSeen string
	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverVarz server.Varz

			err := r.Load(&serverVarz, clusterTag, serverTag, archive.TagServerVars())
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'VARZ' is missing for server %s", serverName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load variables for server %s: %w", serverTag.Value, err)
			}

			version := serverVarz.Version

			_, exists := versionsToServersMap[version]
			if !exists {
				// First time encountering this version, create map entry
				versionsToServersMap[version] = []string{}
				// Add one example server for each version
				examples.add("%s - %s", serverName, version)
			}
			// Add this server to the list running this version
			versionsToServersMap[version] = append(versionsToServersMap[version], serverName)
			lastVersionSeen = version
		}
	}

	if len(versionsToServersMap) == 0 {
		logWarning("No servers version information found")
		return Skipped, nil
	} else if len(versionsToServersMap) > 1 {
		logCritical("Servers are running %d different versions", len(versionsToServersMap))
		return Fail, nil
	}

	// Map contains exactly one element (i.e. one version)
	examples.clear()

	logInfo("All servers are running version %s", lastVersionSeen)
	return Pass, nil
}

// checkServerCPUUsage verify CPU usage is below the given threshold for each server
func checkServerCPUUsage(check Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	severVarsTag := archive.TagServerVars()
	cpuThreshold := check.Configuration["cpu"].Value()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)
			var serverVarz server.Varz
			err := r.Load(&serverVarz, serverTag, clusterTag, severVarsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'VARZ' is missing for server %s", serverName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			// Example: 350% usage with 4 cores => 87.5% averaged
			averageCpuUtilization := serverVarz.CPU / float64(serverVarz.Cores)

			if averageCpuUtilization > cpuThreshold {
				examples.add("%s - %s: %.1f%%", clusterName, serverName, averageCpuUtilization)
			}
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d servers with >%.0f%% CPU usage", examples.Count(), cpuThreshold)
		return Fail, nil
	}

	return Pass, nil
}

// checkSlowConsumers verify that no server is reporting slow consumers
func checkSlowConsumers(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	totalSlowConsumers := int64(0)

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverVarz server.Varz
			err := r.Load(&serverVarz, clusterTag, serverTag, archive.TagServerVars())
			if err != nil {
				return Skipped, fmt.Errorf("failed to load Varz for server %s: %w", serverName, err)
			}

			if slowConsumers := serverVarz.SlowConsumers; slowConsumers > 0 {
				examples.add("%s/%s: %d slow consumers", clusterName, serverName, slowConsumers)
				totalSlowConsumers += slowConsumers
			}
		}
	}

	if totalSlowConsumers > 0 {
		logCritical("Total slow consumers: %d", totalSlowConsumers)
		return PassWithIssues, nil
	}

	logInfo("No slow consumers detected")
	return Pass, nil
}

// checkServerResourceLimits verifies that the resource usage of memory and store is not approaching the reserved amount for each known server
func checkServerResourceLimits(check Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	jsTag := archive.TagServerJetStream()

	memoryUsageThreshold := check.Configuration["memory"].Value()
	storeUsageThreshold := check.Configuration["store"].Value()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverJSInfo server.JSInfo
			err := r.Load(&serverJSInfo, clusterTag, serverTag, jsTag)
			if errors.Is(err, archive.ErrNoMatches) {
				logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.ReservedMemory > 0 {
				threshold := uint64(float64(serverJSInfo.ReservedMemory) * memoryUsageThreshold)
				if serverJSInfo.Memory > threshold {
					examples.add(
						"%s memory usage: %s of %s",
						serverName,
						humanize.IBytes(serverJSInfo.Memory),
						humanize.IBytes(serverJSInfo.ReservedMemory),
					)
				}
			}

			if serverJSInfo.ReservedStore > 0 {
				threshold := uint64(float64(serverJSInfo.ReservedStore) * storeUsageThreshold)
				if serverJSInfo.Store > threshold {
					examples.add(
						"%s store usage: %s of %s",
						serverName,
						humanize.IBytes(serverJSInfo.Store),
						humanize.IBytes(serverJSInfo.ReservedStore),
					)
				}
			}
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d instances of servers approaching reserved usage limit", examples.Count())
		return PassWithIssues, nil
	}

	return Pass, nil
}

func checkJetStreamDomainsForWhitespace(_ Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverJsz server.JSInfo
			err := r.Load(&serverJsz, clusterTag, serverTag, archive.TagServerJetStream())
			if err != nil {
				logWarning("Artifact 'JSZ' is missing for server %s", serverName)
				continue
			}

			// check if jetstream domain contains whitespace
			if strings.ContainsAny(serverJsz.Config.Domain, " \n") {
				examples.add("Cluster %s Server %s Domain %s", clusterName, serverName, serverJsz.Config.Domain)
			}
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d servers with JetStream domains containing whitespace", examples.Count())
		return Fail, nil
	}

	return Pass, nil
}

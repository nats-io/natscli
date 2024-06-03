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
	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/archive"
)

// checkServerHealth verify all known servers are reporting healthy
func checkServerHealth(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
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
func checkServerVersion(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
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

// makeCheckServerCPUUsage create a parametrized check to verify CPU usage is below the given threshold for each server
func makeCheckServerCPUUsage(cpuThreshold float64) checkFunc {
	return func(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
		severVarsTag := archive.TagServerVars()

		for _, clusterName := range r.GetClusterNames() {
			clusterTag := archive.TagCluster(clusterName)
			for _, serverName := range r.GetClusterServerNames(clusterName) {
				serverTag := archive.TagServer(serverName)
				var serverVarz server.Varz
				if err := r.Load(&serverVarz, serverTag, clusterTag, severVarsTag); errors.Is(err, archive.ErrNoMatches) {
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
}

// checkSlowConsumers verify that no server is reporting slow consumers
func checkSlowConsumers(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
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

// makeCheckServerResourceLimits create a parametrized check to verify that the resource usage of memory and store is
// not approaching the reserved amount for each known server
func makeCheckServerResourceLimits(memoryUsageThreshold, storeUsageThreshold float64) checkFunc {
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
}

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
	"fmt"
	"github.com/nats-io/natscli/internal/archive"
)

type checkFunc func(reader *archive.Reader, examples *ExamplesCollection) (Outcome, error)

// Check is the basic unit of analysis that is run against a data archive
type Check struct {
	Name        string
	Description string
	fun         checkFunc
}

// Outcome of running a check against the data gathered into an archive
type Outcome int

const (
	// Pass is for no issues detected
	Pass Outcome = iota
	// PassWithIssues is for non-critical problems
	PassWithIssues Outcome = iota
	// Fail indicates a bad state is detected
	Fail Outcome = iota
	// Skipped is for checks that failed to run (no data, runtime error, ...)
	Skipped Outcome = iota
)

// Outcomes is the list of possible outcomes values
var Outcomes = [...]Outcome{
	Pass,
	PassWithIssues,
	Fail,
	Skipped,
}

// String converts an outcome into a 4-letter string value
func (o Outcome) String() string {
	switch o {
	case Fail:
		return "FAIL"
	case Pass:
		return "PASS"
	case PassWithIssues:
		return "WARN"
	case Skipped:
		return "SKIP"
	default:
		panic(fmt.Sprintf("Uknown outcome code: %d", o))
	}
}

// GetDefaultChecks creates the default list of check using default parameters
func GetDefaultChecks() []Check {

	// Defaults
	const (
		cpuThreshold             = 0.9       // >90% CPU usage
		memoryOutlierThreshold   = 1.5       // Memory usage is >1.5X the average
		lastSequenceLagThreshold = 0.1       // Replica is >10% behind highest known lastSeq
		numSubjectsThreshold     = 1_000_000 // Stream has >1M unique subjects
		haAssetsThreshold        = 1000      // Server has more than 1000 HA assets
		messagesLimitThreshold   = 0.9       // Messages count is >90% of configured limit
		bytesLimitThreshold      = 0.9       // Bytes amount is >90% of configured limit
		consumersLimitThreshold  = 0.9       // Number of consumers is >90% of configured limit
		memoryUsageThreshold     = 0.90      // Memory usage is 90% of configured limit
		storeUsageThreshold      = 0.90      // Store disk usage is 90% of configured limit
		connectionsThreshold     = 0.9       // Number of connections is >90% of configured limit
		subscriptionsThreshold   = 0.9       // Number of subscriptions is 90% of configured limit

	)

	return []Check{
		{
			Name:        "Server Health",
			Description: "Verify that all known nodes are healthy",
			fun:         checkServerHealth,
		},
		{
			Name:        "Server Version",
			Description: "Verify that all known nodes are running the same software version",
			fun:         checkServerVersion,
		},
		{
			Name:        "Server CPU Usage",
			Description: "Verify that CPU usage for all known nodes is below a given threshold",
			fun:         makeCheckServerCPUUsage(cpuThreshold),
		},
		{
			Name:        "Server Slow Consumers",
			Description: "Verify that no node is reporting slow consumers",
			fun:         checkSlowConsumers,
		},
		{
			Name:        "Server Resources Limits ",
			Description: "Verify that resource are below a given threshold compared to the configured limit",
			fun:         makeCheckServerResourceLimits(memoryUsageThreshold, storeUsageThreshold),
		},
		{
			Name:        "Cluster Memory Usage Outliers",
			Description: "Verify memory usage is uniform across nodes in a cluster",
			fun:         createCheckClusterMemoryUsageOutliers(memoryOutlierThreshold),
		},
		{
			Name:        "Cluster Uniform Gateways",
			Description: "Verify all nodes in a cluster share the same gateways configuration",
			fun:         checkClusterUniformGatewayConfig,
		},
		{
			Name:        "Cluster High HA Assets",
			Description: "Verify that the number of HA assets is below a given threshold",
			fun:         makeCheckClusterHighHAAssets(haAssetsThreshold),
		},
		{
			Name:        "Stream Lagging Replicas",
			Description: "Verify that all replicas of a stream are keeping up",
			fun:         makeCheckStreamLaggingReplicas(lastSequenceLagThreshold),
		},
		{
			Name:        "Stream High Cardinality",
			Description: "Verify that streams unique subjects do not exceed a given threshold",
			fun:         makeCheckStreamHighCardinality(numSubjectsThreshold),
		},
		{
			Name:        "Stream Limits",
			Description: "Verify that streams usage is below the configured limits (messages, bytes, consumers)",
			fun:         makeCheckStreamLimits(messagesLimitThreshold, bytesLimitThreshold, consumersLimitThreshold),
		},
		{
			Name:        "Account Limits",
			Description: "Verify that account usage is below the configured limits (connections, subscriptions)",
			fun:         makeCheckAccountLimits(connectionsThreshold, subscriptionsThreshold),
		},
		{
			Name:        "Meta cluster offline replicas",
			Description: "Verify that all nodes part of the meta group are online",
			fun:         checkMetaClusterOfflineReplicas,
		},
		{
			Name:        "Meta cluster leader",
			Description: "Verify that all nodes part of the meta group agree on the meta cluster leader",
			fun:         checkMetaClusterLeader,
		},
		{
			Name:        "Whitespace in leafnode server names",
			Description: "Verify that no leafnode contains whitespace in its name",
			fun:         checkLeafnodeServerNamesForWhitespace,
		},
		{
			Name:        "Whitespace in JetStream domains",
			Description: "Verify that no JetStream server is configured with whitespace in its domain",
			fun:         checkJetStreamDomainsForWhitespace,
		},
		{
			Name:        "Whitespace in cluster name",
			Description: "Verify that no cluster name contains whitespace",
			fun:         checkClusterNamesForWhitespace,
		},
	}
}

// RunCheck is a wrapper to run a check, handling setup and errors
func RunCheck(check Check, ar *archive.Reader, limit uint) (Outcome, *ExamplesCollection) {
	examples := newExamplesCollection(limit)
	outcome, err := check.fun(ar, examples)
	if err != nil {
		// If a check throws an error, mark it as skipped
		fmt.Printf("Check %s failed: %s\n", check.Name, err)
		return Skipped, examples
	}
	return outcome, examples
}

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

package cli

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/archive"
)

type auditAnalyzeCmd struct {
	archivePath          string
	veryVerbose          bool
	exampleIssuesLimit   uint
	exampleIssues        []string
	omittedExampleIssues int
	checks               []struct {
		checkName string
		checkFunc func(reader *archive.Reader) (auditCheckOutcome, error)
	}
}

type auditCheckOutcome int

func (s auditCheckOutcome) badge() string {
	switch s {
	case Pass:
		return "PASS"
	case Fail:
		return "FAIL"
	case SomeIssues:
		return "WARN"
	case Skipped:
		return "SKIP"
	default:
		panic(s)
	}
}

const (
	Skipped auditCheckOutcome = iota
	Pass
	Fail
	SomeIssues
)

func configureAuditAnalyzeCommand(srv *fisk.CmdClause) {
	c := &auditAnalyzeCmd{}
	c.checks = []struct {
		checkName string
		checkFunc func(reader *archive.Reader) (auditCheckOutcome, error)
	}{
		{
			"Server health",
			c.checkServerHealth,
		},
		{
			"Uniform server version",
			c.checkServerVersions,
		},
		{
			"Slow consumers",
			c.checkSlowConsumers,
		},
		{
			"Cluster memory usage",
			c.checkClusterMemoryUsageOutliers,
		},
		{
			"Lagging stream replicas",
			c.checkLaggingStreamReplicas,
		},
		{
			"CPU usage",
			c.checkCpuUsage,
		},
		{
			"High subject cardinality streams",
			c.checkHighSubjectCardinalityStreams,
		},
		{
			"High number of HA assets",
			c.checkHighHAAssetCardinality,
		},
		{
			"Reserved resources limit",
			c.checkResourceLimits,
		},
		{
			"Account limits",
			c.checkAccountLimits,
		},
		{
			"Stream limits",
			c.checkStreamLimits,
		},
		{
			"Meta cluster offline replicas",
			c.checkMetaClusterOfflineReplicas,
		},
		{
			"Meta cluster leader",
			c.checkMetaClusterLeader,
		},
		{
			"Configured gateways",
			c.checkConfiguredGateways,
		},
	}

	analyze := srv.Command("analyze", "perform checks against an archive created by the 'gather' subcommand").Action(c.analyze)
	analyze.Arg("archive", "path to input archive to analyze").Required().ExistingFileVar(&c.archivePath)
	analyze.Flag("limit", "How many example issues to display for each failed check (Set to 0 to show all)").Default("5").UintVar(&c.exampleIssuesLimit)
	// Hidden flags
	analyze.Flag("very-verbose", "Enable debug console messages during analysis").Hidden().BoolVar(&c.veryVerbose)
}

func (cmd *auditAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	// Open archive
	ar, err := archive.NewReader(cmd.archivePath)
	if err != nil {
		return err
	}
	defer func() {
		err := ar.Close()
		if err != nil {
			fmt.Printf("Failed to close archive reader: %s\n", err)
		}
	}()

	// Prepare table of check and their outcome
	type checkSummary struct {
		name    string
		outcome auditCheckOutcome
	}
	summaryTable := make([]checkSummary, len(cmd.checks))
	for i, check := range cmd.checks {
		// Initialize all to skipped
		summaryTable[i] = checkSummary{
			name:    check.checkName,
			outcome: Skipped,
		}
	}
	// Always print summary on exit
	defer func() {
		fmt.Printf("\nSummary:\n")
		for _, s := range summaryTable {
			fmt.Printf("%s: %s\n", s.outcome.badge(), s.name)
		}
	}()

	// Run all configured checks
	for i, check := range cmd.checks {
		cmd.logDebug("Running check: %s", check.checkName)
		cmd.resetExampleIssues()
		outcome, err := check.checkFunc(ar)
		if err != nil {
			return fmt.Errorf("check '%s' error: %w", check.checkName, err)
		}
		if len(cmd.exampleIssues) > 0 {
			for _, example := range cmd.exampleIssues {
				// NOTE: Do not use printf here or percentage signs in the string will be (wrongly) interpreted.
				// Must print string as-is with println or similar.
				fmt.Println("   - " + example)
			}
			if cmd.omittedExampleIssues > 0 {
				fmt.Printf("   - ...%d more...\n", cmd.omittedExampleIssues)
			}
		}
		summaryTable[i].outcome = outcome
	}

	return nil
}

// checkServerVersions verify all known servers are running the same software version
func (cmd *auditAnalyzeCmd) checkServerVersions(r *archive.Reader) (auditCheckOutcome, error) {
	versionsToServersMap := make(map[string][]string)

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverVarz server.Varz

			err := r.Load(&serverVarz, clusterTag, serverTag, archive.TagServerVars())
			if errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'VARZ' is missing for server %s", serverName)
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
				cmd.addExampleIssue("%s - %s", serverName, version)
			}
			// Add this server to the list running this version
			versionsToServersMap[version] = append(versionsToServersMap[version], serverName)
		}
	}

	if len(versionsToServersMap) == 0 {
		cmd.logWarning("No servers version information found")
		return Skipped, nil
	} else if len(versionsToServersMap) > 1 {
		cmd.logIssue("Servers are running %d different versions", len(versionsToServersMap))
		return SomeIssues, nil
	}

	// Map contains exactly one element
	cmd.resetExampleIssues()
	var version string
	for v := range versionsToServersMap {
		version = v
		break
	}

	cmd.logInfo("All servers are running version %s", version)
	return Pass, nil
}

// checkServerHealth verify all known servers are reporting healthy
func (cmd *auditAnalyzeCmd) checkServerHealth(r *archive.Reader) (auditCheckOutcome, error) {
	notHealthy, healthy := 0, 0

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var health server.HealthStatus

			err := r.Load(&health, clusterTag, serverTag, archive.TagServerHealth())
			if errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'HEALTHZ' is missing for server %s", serverName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load health for server %s: %w", serverName, err)
			}

			if health.Status != "ok" {
				cmd.addExampleIssue("%s: %d - %s", serverName, health.StatusCode, health.Status)
				notHealthy += 1
			} else {
				healthy += 1
			}
		}
	}

	if notHealthy > 0 {
		cmd.logIssue("%d/%d servers are not healthy", notHealthy, healthy+notHealthy)
		return SomeIssues, nil
	}

	cmd.logInfo("%d/%d servers are healthy", healthy, healthy)
	return Pass, nil
}

// checkSlowConsumers alert for any server that is reporting slow consumers
func (cmd *auditAnalyzeCmd) checkSlowConsumers(r *archive.Reader) (auditCheckOutcome, error) {

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
				cmd.addExampleIssue("%s/%s: %d slow consumers", clusterName, serverName, slowConsumers)
				totalSlowConsumers += slowConsumers
			}
		}
	}

	if totalSlowConsumers > 0 {
		cmd.logIssue("Total slow consumers: %d", totalSlowConsumers)
		return SomeIssues, nil
	}

	cmd.logInfo("No slow consumers detected")
	return Pass, nil
}

// checkClusterMemoryUsageOutliers calculates per-cluster memory usage and warns if any server in the cluster is using
// significantly more
func (cmd *auditAnalyzeCmd) checkClusterMemoryUsageOutliers(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		outlierThreshold = 1.5 // Warn if one node is using over >1.5X the cluster average
	)

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
				cmd.logWarning("Artifact 'VARZ' is missing for server %s in cluster %s", serverName, clusterName)
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
				cmd.addExampleIssue(
					"Cluster %s avg: %s, server %s: %s",
					clusterName,
					fiBytes(uint64(clusterMemoryUsageMean)),
					serverName,
					fiBytes(uint64(serverMemoryUsage)),
				)
				clustersWithIssuesMap[clusterName] = nil
			}
		}
	}

	if len(clustersWithIssuesMap) > 0 {
		cmd.logIssue(
			"Servers with memory usage above %.1fX the cluster average: %d in %d clusters",
			outlierThreshold,
			cmd.examplesIssuesCount(),
			len(clustersWithIssuesMap),
		)
		return SomeIssues, nil
	}
	return Pass, nil
}

// checkLaggingStreamReplicas for each stream check if some replicas is too far behind the newest replica, using lastSeq
func (cmd *auditAnalyzeCmd) checkLaggingStreamReplicas(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		lastSequenceLagThreshold = 0.1 // Warn if 10% or more behind highest known lastSeq
	)

	typeTag := archive.TagStreamInfo()
	accountNames := r.GetAccountNames()

	if len(accountNames) == 0 {
		cmd.logInfo("No accounts found in archive")
	}

	accountsWithStreams := make(map[string]any)
	streamsInspected := make(map[string]any)
	laggingReplicas := 0

	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)
		streamNames := r.GetAccountStreamNames(accountName)

		if len(streamNames) == 0 {
			cmd.logDebug("No streams found in account: %s", accountName)
		}

		for _, streamName := range streamNames {

			// Track accounts with at least one streams
			accountsWithStreams[accountName] = nil

			streamTag := archive.TagStream(streamName)
			serverNames := r.GetStreamServerNames(accountName, streamName)

			cmd.logDebug(
				"Inspecting account '%s' stream '%s', found %d servers: %v",
				accountName,
				streamName,
				len(serverNames),
				serverNames,
			)

			// Create map server->streamDetails
			replicasStreamDetails := make(map[string]*server.StreamDetail, len(serverNames))
			streamIsEmpty := true

			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)
				streamDetails := &server.StreamDetail{}
				err := r.Load(streamDetails, accountTag, streamTag, serverTag, typeTag)
				if errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning(
						"Artifact not found: %s for stream %s in account %s by server %s",
						typeTag.Value,
						streamName,
						accountName,
						serverName,
					)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to lookup stream artifact: %w", err)
				}

				if streamDetails.State.LastSeq > 0 {
					streamIsEmpty = false
				}

				replicasStreamDetails[serverName] = streamDetails
				// Track streams with least one artifact
				streamsInspected[accountName+"/"+streamName] = nil
			}

			// Check that all replicas are not too far behind the replica with the highest message & byte count
			if !streamIsEmpty {
				// Find the highest lastSeq
				highestLastSeq, highestLastSeqServer := uint64(0), ""
				for serverName, streamDetail := range replicasStreamDetails {
					lastSeq := streamDetail.State.LastSeq
					if lastSeq > highestLastSeq {
						highestLastSeq = lastSeq
						highestLastSeqServer = serverName
					}
				}
				cmd.logDebug(
					"Stream %s / %s highest last sequence: %d @ %s",
					accountName,
					streamName,
					highestLastSeq,
					highestLastSeqServer,
				)

				// Check if some server's sequence is below warning threshold
				maxDelta := uint64(float64(highestLastSeq) * lastSequenceLagThreshold)
				threshold := uint64(0)
				if maxDelta <= highestLastSeq {
					threshold = highestLastSeq - maxDelta
				}
				for serverName, streamDetail := range replicasStreamDetails {
					lastSeq := streamDetail.State.LastSeq
					if lastSeq < threshold {
						cmd.addExampleIssue(
							"%s/%s server %s lastSequence: %d is behind highest lastSequence: %d on server: %s",
							accountName,
							streamName,
							serverName,
							lastSeq,
							highestLastSeq,
							highestLastSeqServer,
						)
						laggingReplicas += 1
					}
				}
			}
		}
	}

	cmd.logInfo("Inspected %d streams across %d accounts", len(streamsInspected), len(accountsWithStreams))

	if laggingReplicas > 0 {
		cmd.logIssue("Found %d replicas lagging behind", laggingReplicas)
		return SomeIssues, nil
	}
	return Pass, nil
}

// checkCpuUsage verify that CPU usage is below a certain threshold for all known servers
func (cmd *auditAnalyzeCmd) checkCpuUsage(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		cpuThreshold = 0.9 // Warn using >90% CPU
	)

	severVarsTag := archive.TagServerVars()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)
			var serverVarz server.Varz
			if err := r.Load(&serverVarz, serverTag, clusterTag, severVarsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'VARZ' is missing for server %s", serverName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			// Example: 350% usage with 4 cores => 87.5% averaged
			averageCpuUtilization := serverVarz.CPU / float64(serverVarz.Cores)

			if averageCpuUtilization > cpuThreshold {
				cmd.addExampleIssue("%s - %s: %.1f%%", clusterName, serverName, averageCpuUtilization)
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d servers with high CPU usage", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkHighSubjectCardinalityStreams verify that the number of unique subjects is below some magic number for each known stream
func (cmd *auditAnalyzeCmd) checkHighSubjectCardinalityStreams(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		numSubjectsThreshold = 1_000_000 // Warn if a stream has >1M unique subjects
	)

	streamDetailsTag := archive.TagStreamInfo()

	for _, accountName := range r.GetAccountNames() {
		accountTag := archive.TagAccount(accountName)

		for _, streamName := range r.GetAccountStreamNames(accountName) {
			streamTag := archive.TagStream(streamName)

			serverNames := r.GetStreamServerNames(accountName, streamName)
			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var streamDetails server.StreamDetail
				if err := r.Load(&streamDetails, serverTag, accountTag, streamTag, streamDetailsTag); errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load STREAM_DETAILS for stream %s in account %s: %w", streamName, accountName, err)
				}

				if streamDetails.State.NumSubjects > numSubjectsThreshold {
					cmd.addExampleIssue("%s/%s: %d subjects", accountName, streamName, streamDetails.State.NumSubjects)
					continue // no need to check other servers for this stream
				}
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d streams with high subjects cardinality", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkHighHAAssetCardinality verify that the number of HA assets is below some magic number for each known server
func (cmd *auditAnalyzeCmd) checkHighHAAssetCardinality(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		haAssetsThreshold = 1000 // Warn if a server has more than 1000 HA assets
	)

	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverJSInfo server.JSInfo
			if err := r.Load(&serverJSInfo, clusterTag, serverTag, jsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.HAAssets > haAssetsThreshold {
				cmd.addExampleIssue("%s: %d HA assets", serverName, serverJSInfo.HAAssets)
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d servers with high a large amount of HA assets", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkResourceLimits verify that the resource usage is not approaching the maximum reserved (memory and store) for
// each known server
func (cmd *auditAnalyzeCmd) checkResourceLimits(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		usageThreshold = 0.90 // Warn if usage is 90% of reserved
	)

	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var serverJSInfo server.JSInfo
			if err := r.Load(&serverJSInfo, clusterTag, serverTag, jsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if serverJSInfo.ReservedMemory > 0 {
				threshold := uint64(float64(serverJSInfo.ReservedMemory) * usageThreshold)
				if serverJSInfo.Memory > threshold {
					cmd.addExampleIssue("%s memory usage: %s of %s", serverName, fiBytes(serverJSInfo.Memory), fiBytes(serverJSInfo.ReservedMemory))
				}
			}

			if serverJSInfo.ReservedStore > 0 {
				threshold := uint64(float64(serverJSInfo.ReservedStore) * usageThreshold)
				if serverJSInfo.Store > threshold {
					cmd.addExampleIssue("%s store usage: %s of %s", serverName, fiBytes(serverJSInfo.Store), fiBytes(serverJSInfo.ReservedStore))
				}
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instances of server near reserved usage limit", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkAccountLimits verify that the number of connections & subscriptions is not approaching the limit for
// each server in each known account
func (cmd *auditAnalyzeCmd) checkAccountLimits(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		connectionsThreshold   = 0.9 // 90% of limit
		subscriptionsThreshold = 0.9 // 90% of limit
	)

	// Check value against limit threshold, create example if exceeded
	checkLimit := func(limitName, serverName, accountName string, value, limit int64, percentThreshold float64) {
		if limit <= 0 {
			// Limit not set
			return
		}
		threshold := int64(float64(limit) * percentThreshold)
		if value > threshold {
			cmd.addExampleIssue(
				"account %s (on %s) using %.1f%% of %s limit (%d/%d)",
				accountName,
				serverName,
				float64(value)*100/float64(limit),
				limitName,
				value,
				limit,
			)
		}
	}

	accountsTag := archive.TagServerAccounts()
	accountDetailsTag := archive.TagAccountInfo()
	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var accountz server.Accountz
			if err := r.Load(&accountz, clusterTag, serverTag, accountsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'ACCOUNTZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load ACCOUNTZ for server %s: %w", serverName, err)
			}

			for _, accountName := range accountz.Accounts {
				accountTag := archive.TagAccount(accountName)

				var accountInfo server.AccountInfo
				if err := r.Load(&accountInfo, serverTag, accountTag, accountDetailsTag); errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning("Account details is missing for account %s, server %s", accountName, serverName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load Account details from server %s for account %s, error: %w", serverTag.Value, accountName, err)
				}

				if accountInfo.Claim == nil {
					// Can't check limits without a claim
					continue
				}

				checkLimit(
					"client connections",
					serverTag.Value,
					accountName,
					int64(accountInfo.ClientCnt),
					accountInfo.Claim.Limits.Conn,
					connectionsThreshold,
				)

				checkLimit(
					"client connections (account)",
					serverTag.Value,
					accountName,
					int64(accountInfo.ClientCnt),
					accountInfo.Claim.Limits.AccountLimits.Conn,
					connectionsThreshold,
				)

				checkLimit(
					"leaf connections",
					serverTag.Value,
					accountName,
					int64(accountInfo.LeafCnt),
					accountInfo.Claim.Limits.LeafNodeConn,
					connectionsThreshold,
				)

				checkLimit(
					"leaf connections (account)",
					serverTag.Value,
					accountName,
					int64(accountInfo.LeafCnt),
					accountInfo.Claim.Limits.AccountLimits.LeafNodeConn,
					connectionsThreshold,
				)

				checkLimit(
					"subscriptions",
					serverTag.Value,
					accountName,
					int64(accountInfo.SubCnt),
					accountInfo.Claim.Limits.Subs,
					subscriptionsThreshold,
				)
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instances of accounts approaching limit", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkAccountLimits verify that the number of messages/bytes/consumers is not approaching the limit for
// each known stream
func (cmd *auditAnalyzeCmd) checkStreamLimits(r *archive.Reader) (auditCheckOutcome, error) {
	const (
		messagesThreshold  = 0.9 // Alert if >90% of limit
		bytesThreshold     = 0.9 // Alert if >90% of limit
		consumersThreshold = 0.9 // Alert if >90% of limit
	)

	// Check value against limit threshold, create example if exceeded
	checkLimit := func(limitName, accountName, streamName, serverName string, value, limit int64, percentThreshold float64) {
		if limit <= 0 {
			// Limit not set
			return
		}
		threshold := int64(float64(limit) * percentThreshold)
		if value > threshold {
			cmd.addExampleIssue(
				"stream %s (in %s on %s) using %.1f%% of %s limit (%d/%d)",
				streamName,
				accountName,
				serverName,
				float64(value)*100/float64(limit),
				limitName,
				value,
				limit,
			)
		}
	}

	streamDetailsTag := archive.TagStreamInfo()

	for _, accountName := range r.GetAccountNames() {
		accountTag := archive.TagAccount(accountName)

		for _, streamName := range r.GetAccountStreamNames(accountName) {
			streamTag := archive.TagStream(streamName)

			serverNames := r.GetStreamServerNames(accountName, streamName)
			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var streamDetails server.StreamDetail
				if err := r.Load(&streamDetails, serverTag, accountTag, streamTag, streamDetailsTag); errors.Is(err, archive.ErrNoMatches) {
					cmd.logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load STREAM_DETAILS for stream %s in account %s: %w", streamName, accountName, err)
				}

				checkLimit(
					"messages",
					accountName,
					streamName,
					serverName,
					int64(streamDetails.State.Msgs),
					streamDetails.Config.MaxMsgs,
					messagesThreshold,
				)

				checkLimit(
					"bytes",
					accountName,
					streamName,
					serverName,
					int64(streamDetails.State.Bytes),
					streamDetails.Config.MaxBytes,
					bytesThreshold,
				)

				checkLimit(
					"consumers",
					accountName,
					streamName,
					serverName,
					int64(streamDetails.State.Consumers),
					int64(streamDetails.Config.MaxConsumers),
					consumersThreshold,
				)
			}
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instances of streams approaching limit", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkMetaClusterOfflineReplicas verify that all meta-cluster replicas are online for each known cluster
func (cmd *auditAnalyzeCmd) checkMetaClusterOfflineReplicas(r *archive.Reader) (auditCheckOutcome, error) {
	serverVarsTag := archive.TagServerVars()
	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var jetStreamInfo server.JSInfo
			if err := r.Load(&jetStreamInfo, clusterTag, serverTag, jsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if jetStreamInfo.Disabled {
				continue
			}

			var serverVarz server.Varz
			if err := r.Load(&serverVarz, clusterTag, serverTag, serverVarsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'VARZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			if serverVarz.JetStream.Meta == nil {
				cmd.logWarning("%s / %s does not have meta group info", clusterName, serverName)
				continue
			}

			for _, peerInfo := range serverVarz.JetStream.Meta.Replicas {
				if peerInfo.Offline {
					cmd.addExampleIssue(
						"%s - %s reports meta peer %s as offline",
						clusterName,
						serverName,
						peerInfo.Name,
					)
				}
			}

		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instance of replicas disagreeing on meta-cluster leader", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkMetaClusterLeader verify that all server agree on the same meta group leader in each known cluster
func (cmd *auditAnalyzeCmd) checkMetaClusterLeader(r *archive.Reader) (auditCheckOutcome, error) {
	serverVarsTag := archive.TagServerVars()
	jsTag := archive.TagServerJetStream()

	for _, clusterName := range r.GetClusterNames() {
		clusterTag := archive.TagCluster(clusterName)
		leaderFollowers := make(map[string][]string)

		for _, serverName := range r.GetClusterServerNames(clusterName) {
			serverTag := archive.TagServer(serverName)

			var jetStreamInfo server.JSInfo
			if err := r.Load(&jetStreamInfo, clusterTag, serverTag, jsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'JSZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load JSZ for server %s: %w", serverName, err)
			}

			if jetStreamInfo.Disabled {
				continue
			}

			var serverVarz server.Varz
			if err := r.Load(&serverVarz, clusterTag, serverTag, serverVarsTag); errors.Is(err, archive.ErrNoMatches) {
				cmd.logWarning("Artifact 'VARZ' is missing for server %s cluster %s", serverName, clusterName)
				continue
			} else if err != nil {
				return Skipped, fmt.Errorf("failed to load VARZ for server %s: %w", serverName, err)
			}

			if serverVarz.JetStream.Meta == nil {
				cmd.logWarning("%s / %s does not have meta group info", clusterName, serverName)
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
			cmd.addExampleIssue("Members of %s disagree on meta leader (%v)", clusterName, leaderFollowers)
		}
	}

	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instance of replicas disagreeing on meta-cluster leader", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// checkConfiguredGateways verify that configured gateways match within all servers of each cluster
func (cmd *auditAnalyzeCmd) checkConfiguredGateways(r *archive.Reader) (auditCheckOutcome, error) {
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
				cmd.logWarning("Artifact 'GATEWAYZ' is missing for server %s cluster %s", serverName, clusterName)
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
					cmd.logDebug(
						"Cluster %s - Comparing configured %s gateways of %s (%d) to %s (%d)",
						clusterName,
						t.gatewayType,
						serverName,
						len(targetClusterNames),
						previousServerName,
						len(previousTargetClusterNames),
					)
					if !reflect.DeepEqual(targetClusterNames, previousTargetClusterNames) {
						cmd.addExampleIssue(
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
	if cmd.examplesIssuesCount() > 0 {
		cmd.logIssue("Found %d instance of gateways configurations mismatch", cmd.examplesIssuesCount())
		return SomeIssues, nil
	}

	return Pass, nil
}

// logIssue for issues that need attention that need to be addressed
func (cmd *auditAnalyzeCmd) logIssue(format string, a ...any) {
	fmt.Printf("(!) "+format+"\n", a...)
}

// logInfo for neutral and positive messages
func (cmd *auditAnalyzeCmd) logInfo(format string, a ...any) {
	fmt.Printf(format+"\n", a...)
}

// logWarning for issues running the check itself, but not serious enough to terminate with an error
func (cmd *auditAnalyzeCmd) logWarning(format string, a ...any) {
	fmt.Printf("Warning: "+format+"\n", a...)
}

// logDebug for very fine grained progress, disabled by default
func (cmd *auditAnalyzeCmd) logDebug(format string, a ...any) {
	if cmd.veryVerbose {
		fmt.Printf("(DEBUG) "+format+"\n", a...)
	}
}

func (cmd *auditAnalyzeCmd) resetExampleIssues() {
	cmd.exampleIssues = make([]string, 0)
	cmd.omittedExampleIssues = 0
}

func (cmd *auditAnalyzeCmd) examplesIssuesCount() int {
	return len(cmd.exampleIssues) + cmd.omittedExampleIssues
}

func (cmd *auditAnalyzeCmd) addExampleIssue(format string, a ...any) {
	if cmd.exampleIssuesLimit == 0 || len(cmd.exampleIssues) < int(cmd.exampleIssuesLimit) {
		cmd.exampleIssues = append(cmd.exampleIssues, fmt.Sprintf(format, a...))
	} else {
		cmd.omittedExampleIssues += 1
	}
}

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
			Code:        "STREAM_001",
			Name:        "Stream Lagging Replicas",
			Description: "All replicas of a stream are keeping up",
			Configuration: map[string]*CheckConfiguration{
				"last_seq": {
					Key:         "last_seq",
					Description: "How far a replica may be behind the highest known last sequence",
					Default:     0.1,
					Unit:        PercentageUnit,
				},
			},
			Handler: checkStreamLaggingReplicas,
		},
		Check{
			Code:        "STREAM_002",
			Name:        "Stream High Cardinality",
			Description: "Streams unique subjects do not exceed a given threshold",
			Configuration: map[string]*CheckConfiguration{
				"subjects": {
					Key:         "subjects",
					Description: "Alerting threshold for unique subjects in a stream",
					Default:     1_000_000,
					Unit:        IntUnit,
				},
			},
			Handler: checkStreamHighCardinality,
		},
		Check{
			Code:        "STREAM_003",
			Name:        "Stream Limits",
			Description: "Stream usage is below the configured limits",
			Configuration: map[string]*CheckConfiguration{
				"messages": {
					Key:         "messages",
					Description: "Alert if messages near configured limit",
					Default:     0.9,
					Unit:        PercentageUnit,
				},
				"bytes": {
					Key:         "bytes",
					Description: "Alert if size near configured limit",
					Default:     0.9,
					Unit:        PercentageUnit,
				},
				"consumers": {
					Key:         "consumers",
					Description: "Alert if consumer count near configured limit",
					Default:     0.9,
					Unit:        PercentageUnit,
				},
			},
			Handler: checkStreamLimits,
		},
	)
}

// checkStreamLaggingReplicas verifies that in each known stream no replica is too far behind the most up to date (based on stream last sequence)
func checkStreamLaggingReplicas(check Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	typeTag := archive.TagStreamInfo()
	accountNames := r.GetAccountNames()
	lastSequenceLagThreshold := check.Configuration["last_seq"].Value()

	if len(accountNames) == 0 {
		logInfo("No accounts found in archive")
	}

	accountsWithStreams := make(map[string]any)
	streamsInspected := make(map[string]any)
	laggingReplicas := 0

	for _, accountName := range accountNames {
		accountTag := archive.TagAccount(accountName)
		streamNames := r.GetAccountStreamNames(accountName)

		if len(streamNames) == 0 {
			logDebug("No streams found in account: %s", accountName)
		}

		for _, streamName := range streamNames {

			// Track accounts with at least one streams
			accountsWithStreams[accountName] = nil

			streamTag := archive.TagStream(streamName)
			serverNames := r.GetStreamServerNames(accountName, streamName)

			logDebug(
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
					logWarning(
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
				logDebug(
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
						examples.add(
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

	logInfo("Inspected %d streams across %d accounts", len(streamsInspected), len(accountsWithStreams))

	if laggingReplicas > 0 {
		logCritical("Found %d replicas lagging >%.0f%% behind", laggingReplicas, lastSequenceLagThreshold)
		return Fail, nil
	}
	return Pass, nil
}

// checkHighSubjectCardinalityStreams verify that the number of unique subjects is below some magic number for each known stream
func checkStreamHighCardinality(check Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	streamDetailsTag := archive.TagStreamInfo()
	numSubjectsThreshold := check.Configuration["subjects"].Value()

	for _, accountName := range r.GetAccountNames() {
		accountTag := archive.TagAccount(accountName)

		for _, streamName := range r.GetAccountStreamNames(accountName) {
			streamTag := archive.TagStream(streamName)

			serverNames := r.GetStreamServerNames(accountName, streamName)
			for _, serverName := range serverNames {
				serverTag := archive.TagServer(serverName)

				var streamDetails server.StreamDetail
				err := r.Load(&streamDetails, serverTag, accountTag, streamTag, streamDetailsTag)
				if errors.Is(err, archive.ErrNoMatches) {
					logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load STREAM_DETAILS for stream %s in account %s: %w", streamName, accountName, err)
				}

				if float64(streamDetails.State.NumSubjects) > numSubjectsThreshold {
					examples.add("%s/%s: %d subjects", accountName, streamName, streamDetails.State.NumSubjects)
					continue // no need to check other servers for this stream
				}
			}
		}
	}

	if examples.Count() > 0 {
		logCritical("Found %d streams with subjects cardinality exceeding %s", examples.Count(), numSubjectsThreshold)
		return PassWithIssues, nil
	}

	return Pass, nil
}

// checkStreamLimits verifies that the number of messages/bytes/consumers is below a given threshold from the the configured limit for each known stream
func checkStreamLimits(check Check, r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
	messagesThreshold := check.Configuration["messages"].Value()
	bytesThreshold := check.Configuration["bytes"].Value()
	consumersThreshold := check.Configuration["consumers"].Value()

	// Check value against limit threshold, create example if exceeded
	checkLimit := func(limitName, accountName, streamName, serverName string, value, limit int64, percentThreshold float64) {
		if limit <= 0 {
			// Limit not set
			return
		}
		threshold := int64(float64(limit) * percentThreshold)
		if value > threshold {
			examples.add(
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
				err := r.Load(&streamDetails, serverTag, accountTag, streamTag, streamDetailsTag)
				if errors.Is(err, archive.ErrNoMatches) {
					logWarning("Artifact 'STREAM_DETAILS' is missing for stream %s in account %s", streamName, accountName)
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

	if examples.Count() > 0 {
		logCritical("Found %d instances of streams approaching limit", examples.Count())
		return PassWithIssues, nil
	}

	return Pass, nil
}

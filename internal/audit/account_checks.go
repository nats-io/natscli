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

// makeCheckAccountLimits create a parametrized check to verify that the number of connections & subscriptions is not
// approaching the limit set for the account
func makeCheckAccountLimits(connectionsThreshold, subscriptionsThreshold float64) checkFunc {
	return func(r *archive.Reader, examples *ExamplesCollection) (Outcome, error) {
		// Check value against limit threshold, create example if exceeded
		checkLimit := func(limitName, serverName, accountName string, value, limit int64, percentThreshold float64) {
			if limit <= 0 {
				// Limit not set
				return
			}
			threshold := int64(float64(limit) * percentThreshold)
			if value > threshold {
				examples.add(
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
					logWarning("Artifact 'ACCOUNTZ' is missing for server %s cluster %s", serverName, clusterName)
					continue
				} else if err != nil {
					return Skipped, fmt.Errorf("failed to load ACCOUNTZ for server %s: %w", serverName, err)
				}

				for _, accountName := range accountz.Accounts {
					accountTag := archive.TagAccount(accountName)

					var accountInfo server.AccountInfo
					if err := r.Load(&accountInfo, serverTag, accountTag, accountDetailsTag); errors.Is(err, archive.ErrNoMatches) {
						logWarning("Account details is missing for account %s, server %s", accountName, serverName)
						continue
					} else if err != nil {
						return Skipped, fmt.Errorf(
							"failed to load Account details from server %s for account %s, error: %w",
							serverTag.Value,
							accountName,
							err,
						)
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

		if examples.Count() > 0 {
			logCritical("Found %d instances of accounts approaching limit", examples.Count())
			return PassWithIssues, nil
		}

		return Pass, nil
	}
}

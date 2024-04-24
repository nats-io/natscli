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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/archive"
)

type auditGatherCmd struct {
	archiveFilePath string
	progress        bool
	include         struct {
		serverEndpoints  bool
		serverProfiles   bool
		accountEndpoints bool
		streams          bool
		consumers        bool
	}
	captureLogWriter       io.Writer
	serverEndpointConfigs  []auditEndpointCaptureConfig
	accountEndpointConfigs []auditEndpointCaptureConfig
	serverProfileNames     []string
}

// auditEndpointCaptureConfig configuration for capturing and tagging server and account endpoints
type auditEndpointCaptureConfig struct {
	apiSuffix     string
	responseValue any
	typeTag       *archive.Tag
}

// serverAPIResponseNoData is a modified version of server.ServerAPIResponse that inhibits deserialization of the
// `data` field by using `json.RawMessage`. This is necessary because deserializing into a generic object (i.e. map)
// can cause loss of precision for large numbers.
type serverAPIResponseNoData struct {
	Server *server.ServerInfo `json:"server"`
	Data   json.RawMessage    `json:"data,omitempty"`
	Error  *server.ApiError   `json:"error,omitempty"`
}

func configureAuditGatherCommand(srv *fisk.CmdClause) {
	c := &auditGatherCmd{
		serverEndpointConfigs: []auditEndpointCaptureConfig{
			{
				"VARZ",
				server.Varz{},
				archive.TagServerVars(),
			},
			{
				"CONNZ",
				server.Connz{},
				archive.TagServerConnections(),
			},
			{
				"ROUTEZ",
				server.Routez{},
				archive.TagServerRoutes(),
			},
			{
				"GATEWAYZ",
				server.Gatewayz{},
				archive.TagServerGateways(),
			},
			{
				"LEAFZ",
				server.Leafz{},
				archive.TagServerLeafs(),
			},
			{
				"SUBSZ",
				server.Subsz{},
				archive.TagServerSubs(),
			},
			{
				"JSZ",
				server.JSInfo{},
				archive.TagServerJetStream(),
			},
			{
				"ACCOUNTZ",
				server.Accountz{},
				archive.TagServerAccounts(),
			},
			{
				"HEALTHZ",
				server.HealthStatus{},
				archive.TagServerHealth(),
			},
		},
		accountEndpointConfigs: []auditEndpointCaptureConfig{
			{
				"CONNZ",
				server.Connz{},
				archive.TagAccountConnections(),
			},
			{
				"LEAFZ",
				server.Leafz{},
				archive.TagAccountLeafs(),
			},
			{
				"SUBSZ",
				server.Subsz{},
				archive.TagAccountSubs(),
			},
			{
				"INFO",
				server.AccountInfo{},
				archive.TagAccountInfo(),
			},
			{
				"JSZ",
				server.JetStreamStats{},
				archive.TagAccountJetStream(),
			},
		},
		serverProfileNames: []string{
			"goroutine",
			"heap",
			"allocs",
		},
	}

	gather := srv.Command("gather", "capture a variety of data from a deployment into an archive file").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.archiveFilePath)
	gather.Flag("progress", "Display progress messages during gathering").Default("true").BoolVar(&c.progress)
	gather.Flag("server-endpoints", "Capture monitoring endpoints for each server").Default("true").BoolVar(&c.include.serverEndpoints)
	gather.Flag("server-profiles", "Capture profiles for each server").Default("true").BoolVar(&c.include.serverProfiles)
	gather.Flag("account-endpoints", "Capture monitoring endpoints for each account").Default("true").BoolVar(&c.include.accountEndpoints)
	gather.Flag("streams", "Capture state of each stream").Default("true").BoolVar(&c.include.streams)
	gather.Flag("consumers", "Capture state of each stream consumers").Default("true").BoolVar(&c.include.consumers)
}

const auditServerProfilesFileExtension = "prof"

func (c *auditGatherCmd) gather(_ *fisk.ParseContext) error {

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// If no output path is specified, create one
	if c.archiveFilePath == "" {
		c.archiveFilePath = filepath.Join(os.TempDir(), "archive.zip")
	}

	// Gathering prints messages to stdout, but they are also written into this buffer.
	// A copy of the output is included in the archive itself.
	var captureLogBuffer bytes.Buffer
	c.captureLogWriter = &captureLogBuffer

	// Create an archive writer
	aw, err := archive.NewWriter(c.archiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer func() {
		// Add the output of this command (so far) to the archive as additional log artifact
		if c.captureLogWriter != nil {
			err = aw.AddRaw(bytes.NewReader(captureLogBuffer.Bytes()), "log", archive.TagSpecial("audit_gather_log"))
			if err != nil {
				fmt.Printf("Failed to add capture log: %s\n", err)
			}
			c.captureLogWriter = nil
		}

		err := aw.Close()
		if err != nil {
			fmt.Printf("Failed to close archive: %s\n", err)
		}
		fmt.Printf("Archive created at: %s\n", c.archiveFilePath)
	}()

	// Discover servers, create map with servers info
	serverInfoMap, err := c.discoverServers(nc)
	if err != nil {
		return fmt.Errorf("failed to discover servers: %w", err)
	}

	// Discover accounts, create map with count of server for each account
	accountIdsToServersCountMap, systemAccount, err := c.discoverAccounts(nc, serverInfoMap)
	if err != nil {
		return fmt.Errorf("failed to discover accounts: %w", err)
	}

	// Capture server endpoints
	if c.include.serverEndpoints {
		err := c.captureServerEndpoints(nc, serverInfoMap, aw)
		if err != nil {
			return fmt.Errorf("failed to capture server endpoints")
		}
	} else {
		c.logProgress("Skipping servers endpoints data gathering")
	}

	// Capture server profiles
	if c.include.serverProfiles {
		err := c.captureServerProfiles(nc, serverInfoMap, aw)
		if err != nil {
			return fmt.Errorf("failed to capture server profiles: %w", err)
		}
	} else {
		c.logProgress("Skipping server profiles gathering")
	}

	// Capture account endpoints
	if c.include.accountEndpoints {
		err := c.captureAccountEndpoints(nc, serverInfoMap, accountIdsToServersCountMap, aw)
		if err != nil {
			return fmt.Errorf("failed to capture account endpoints: %w", err)
		}
	} else {
		c.logProgress("Skipping accounts endpoints data gathering")
	}

	// Discover and capture streams in each account
	if c.include.streams {
		c.logProgress("Gathering streams data...")
		for accountId, numServers := range accountIdsToServersCountMap {
			// Skip system account, JetStream is probably not enabled
			if accountId == systemAccount {
				continue
			}
			err := c.captureAccountStreams(nc, serverInfoMap, accountId, numServers, aw)
			if err != nil {
				c.logWarning("Failed to capture streams for account %s", accountId)
			}
		}
	} else {
		c.logProgress("Skipping streams data gathering")
	}

	// Capture metadata
	err = c.captureMetadata(nc, aw)
	if err != nil {
		return fmt.Errorf("failed to capture metadata: %w", err)
	}

	return nil
}

// Discover servers by broadcasting a PING and then collecting responses
func (c *auditGatherCmd) discoverServers(nc *nats.Conn) (map[string]*server.ServerInfo, error) {
	var serverInfoMap = make(map[string]*server.ServerInfo)
	c.logProgress("Broadcasting PING to discover servers... (this may take a few seconds)")
	err := doReqAsync(nil, "$SYS.REQ.SERVER.PING", 0, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err := json.Unmarshal(b, &apiResponse); err != nil {
			c.logWarning("Failed to deserialize PING response: %s", err)
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		_, exists := serverInfoMap[apiResponse.Server.ID]
		if exists {
			c.logWarning("Duplicate server %s (%s) response to PING, ignoring", serverId, serverName)
			return
		}

		serverInfoMap[serverId] = apiResponse.Server
		c.logProgress("Discovered server '%s' (%s)", serverName, serverId)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to gather server responses: %w", err)
	}
	c.logProgress("Discovered %d servers", len(serverInfoMap))
	return serverInfoMap, nil
}

// Discover accounts by broadcasting a PING and then collecting responses
func (c *auditGatherCmd) discoverAccounts(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo) (map[string]int, string, error) {
	// Broadcast PING.ACCOUNTZ to discover (active) accounts
	// N.B. Inactive accounts (no connections) cannot be discovered this way
	c.logProgress("Broadcasting PING to discover accounts... ")
	var accountIdsToServersCountMap = make(map[string]int)
	var systemAccount = ""
	err := doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", len(serverInfoMap), nc, func(b []byte) {
		var apiResponse serverAPIResponseNoData
		err := json.Unmarshal(b, &apiResponse)
		if err != nil {
			c.logWarning("Failed to deserialize accounts response, ignoring")
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		// Ignore responses from servers not discovered earlier.
		// We are discarding useful data, but limiting additional collection to a fixed set of nodes
		// simplifies querying and analysis. Could always re-run gather if a new server just joined.
		if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
			c.logWarning("Ignoring accounts response from unknown server: %s", serverName)
			return
		}

		var accountsResponse server.Accountz
		err = json.Unmarshal(apiResponse.Data, &accountsResponse)
		if err != nil {
			c.logWarning("Failed to deserialize accounts response body: %s", err)
			return
		}

		c.logProgress("Discovered %d accounts on server %s", len(accountsResponse.Accounts), serverName)

		// Track how many servers known any given account
		for _, accountId := range accountsResponse.Accounts {
			_, accountKnown := accountIdsToServersCountMap[accountId]
			if !accountKnown {
				accountIdsToServersCountMap[accountId] = 0
			}
			accountIdsToServersCountMap[accountId] += 1
		}

		// Track system account (normally, only one for the entire ensemble)
		if accountsResponse.SystemAccount == "" {
			c.logWarning("Server %s system account is not set", serverName)
		} else if systemAccount == "" {
			systemAccount = accountsResponse.SystemAccount
			c.logProgress("Discovered system account name: %s", systemAccount)
		} else if systemAccount != accountsResponse.SystemAccount {
			// This should not happen under normal circumstances!
			c.logWarning("Multiple system accounts detected (%s, %s)", systemAccount, accountsResponse.SystemAccount)
		}
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to discover accounts: %w", err)
	}
	c.logProgress("Discovered %d accounts over %d servers", len(accountIdsToServersCountMap), len(serverInfoMap))
	return accountIdsToServersCountMap, systemAccount, nil
}

// Capture configured endpoints for each known server
func (c *auditGatherCmd) captureServerEndpoints(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo, aw *archive.Writer) error {
	c.logProgress("Querying %d endpoints on %d known servers...", len(c.serverEndpointConfigs), len(serverInfoMap))
	capturedCount := 0
	for serverId, serverInfo := range serverInfoMap {
		serverName := serverInfo.Name
		for _, endpoint := range c.serverEndpointConfigs {

			subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", serverId, endpoint.apiSuffix)

			endpointResponse := reflect.New(reflect.TypeOf(endpoint.responseValue)).Interface()

			responses, err := doReq(nil, subject, 1, nc)
			if err != nil {
				c.logWarning("Failed to request %s from server %s: %s", endpoint.apiSuffix, serverName, err)
				continue
			}

			if len(responses) != 1 {
				c.logWarning("Unexpected number of responses to %s from server %s: %d", endpoint.apiSuffix, serverName, len(responses))
				continue
			}

			responseBytes := responses[0]

			var apiResponse serverAPIResponseNoData
			if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
				c.logWarning("Failed to deserialize %s response from server %s: %s", endpoint.apiSuffix, serverName, err)
				continue
			}

			err = json.Unmarshal(apiResponse.Data, endpointResponse)
			if err != nil {
				c.logWarning("Failed to deserialize %s response data from server %s: %s", endpoint.apiSuffix, serverName, err)
				continue
			}

			tags := []*archive.Tag{
				archive.TagServer(serverName), // Source server
				endpoint.typeTag,              // Type of artifact
			}

			if serverInfo.Cluster != "" {
				tags = append(tags, archive.TagCluster(serverInfo.Cluster))
			} else {
				tags = append(tags, archive.TagNoCluster())
			}

			err = aw.Add(endpointResponse, tags...)
			if err != nil {
				return fmt.Errorf("failed to add endpoint %s response to archive: %w", subject, err)
			}

			capturedCount += 1
		}
	}
	c.logProgress("Captured %d endpoint responses from %d servers", capturedCount, len(serverInfoMap))
	return nil
}

// Capture configured profiles for each known server
func (c *auditGatherCmd) captureServerProfiles(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo, aw *archive.Writer) error {
	c.logProgress("Capturing %d profiles on %d known servers...", len(c.serverProfileNames), len(serverInfoMap))
	capturedCount := 0
	for serverId, serverInfo := range serverInfoMap {
		serverName := serverInfo.Name
		clusterTag := archive.TagNoCluster()
		if serverInfo.Cluster != "" {
			clusterTag = archive.TagCluster(serverInfo.Cluster)
		}

		for _, profileName := range c.serverProfileNames {

			subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.PROFILEZ", serverId)
			payload := server.ProfilezOptions{
				Name:  profileName,
				Debug: 0,
			}

			responses, err := doReq(payload, subject, 1, nc)
			if err != nil {
				c.logWarning("Failed to request %s profile from server %s: %s", profileName, serverName, err)
				continue
			}

			if len(responses) != 1 {
				c.logWarning("Unexpected number of responses to %s profile from server %s: %d", profileName, serverName, len(responses))
				continue
			}

			responseBytes := responses[0]

			var apiResponse struct {
				Server *server.ServerInfo     `json:"server"`
				Data   *server.ProfilezStatus `json:"data,omitempty"`
				Error  *server.ApiError       `json:"error,omitempty"`
			}
			if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
				c.logWarning("Failed to deserialize %s profile response from server %s: %s", profileName, serverName, err)
				continue
			}
			if apiResponse.Error != nil {
				c.logWarning("Failed to retrieve %s profile from server %s: %s", profileName, serverName, apiResponse.Error.Description)
				continue
			}

			profileStatus := apiResponse.Data
			if profileStatus.Error != "" {
				c.logWarning("Failed to retrieve %s profile from server %s: %s", profileName, serverName, profileStatus.Error)
				continue
			}

			tags := []*archive.Tag{
				archive.TagServer(serverName),
				archive.TagServerProfile(),
				archive.TagProfileName(profileName),
				clusterTag,
			}

			profileDataBytes := apiResponse.Data.Profile

			err = aw.AddRaw(bytes.NewReader(profileDataBytes), auditServerProfilesFileExtension, tags...)
			if err != nil {
				return fmt.Errorf("failed to add %s profile from to archive: %w", profileName, err)
			}

			capturedCount += 1
		}
	}
	c.logProgress("Captured %d server profiles from %d servers", capturedCount, len(serverInfoMap))
	return nil
}

// Capture configured endpoints for each known account
func (c *auditGatherCmd) captureAccountEndpoints(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo, accountIdsToServersCountMap map[string]int, aw *archive.Writer) error {
	type Responder struct {
		ClusterName string
		ServerName  string
	}
	capturedCount := 0
	c.logProgress("Querying %d endpoints for %d known accounts...", len(c.accountEndpointConfigs), len(accountIdsToServersCountMap))
	for accountId, serversCount := range accountIdsToServersCountMap {
		for _, endpoint := range c.accountEndpointConfigs {
			subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountId, endpoint.apiSuffix)
			endpointResponses := make(map[Responder]any, serversCount)

			err := doReqAsync(nil, subject, serversCount, nc, func(b []byte) {
				var apiResponse serverAPIResponseNoData
				err := json.Unmarshal(b, &apiResponse)
				if err != nil {
					c.logWarning("Failed to deserialize %s response for account %s: %s", endpoint.apiSuffix, accountId, err)
					return
				}

				serverId := apiResponse.Server.ID

				// Ignore responses from servers not discovered earlier.
				// We are discarding useful data, but limiting additional collection to a fixed set of nodes
				// simplifies querying and analysis. Could always re-run gather if a new server just joined.
				if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
					c.logWarning("Ignoring account %s response from unknown server: %s", endpoint.apiSuffix, serverId)
					return
				}

				endpointResponse := reflect.New(reflect.TypeOf(endpoint.responseValue)).Interface()
				err = json.Unmarshal(apiResponse.Data, endpointResponse)
				if err != nil {
					c.logWarning("Failed to deserialize %s response for account %s: %s", endpoint.apiSuffix, accountId, err)
					return
				}

				responder := Responder{
					ClusterName: apiResponse.Server.Cluster,
					ServerName:  apiResponse.Server.Name,
				}

				if _, isDuplicateResponse := endpointResponses[responder]; isDuplicateResponse {
					c.logWarning("Ignoring duplicate account %s response from server %s", endpoint.apiSuffix, responder.ServerName)
					return
				}

				endpointResponses[responder] = endpointResponse
			})
			if err != nil {
				c.logWarning("Failed to request %s for account %s: %s", endpoint.apiSuffix, accountId, err)
				continue
			}

			// Store all responses for this account endpoint
			for responder, endpointResponse := range endpointResponses {
				clusterTag := archive.TagNoCluster()
				if responder.ClusterName != "" {
					clusterTag = archive.TagCluster(responder.ClusterName)
				}

				tags := []*archive.Tag{
					archive.TagAccount(accountId),
					archive.TagServer(responder.ServerName),
					clusterTag,
					endpoint.typeTag,
				}

				err = aw.Add(endpointResponse, tags...)
				if err != nil {
					return fmt.Errorf("failed to add response to %s to archive: %w", subject, err)
				}

				capturedCount += 1
			}
		}
	}
	c.logProgress("Captured %d endpoint responses from %d accounts", capturedCount, len(accountIdsToServersCountMap))
	return nil
}

// Discover streams in given account, and capture info for each one
func (c *auditGatherCmd) captureAccountStreams(nc *nats.Conn, serverInfoMap map[string]*server.ServerInfo, accountId string, numServers int, aw *archive.Writer) error {

	jszOptions := server.JSzOptions{
		Account:    accountId,
		Streams:    true,
		Consumer:   c.include.consumers, // Capture consumers, unless configured to skip
		Config:     true,
		RaftGroups: true,
	}

	jsInfoResponses := make(map[string]*server.JSInfo, numServers)
	err := doReqAsync(jszOptions, "$SYS.REQ.SERVER.PING.JSZ", numServers, nc, func(b []byte) {
		var apiResponse serverAPIResponseNoData
		err := json.Unmarshal(b, &apiResponse)
		if err != nil {
			c.logWarning("Failed to deserialize JS info response for account %s: %s", accountId, err)
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		// Ignore responses from servers not discovered earlier.
		// We are discarding useful data, but limiting additional collection to a fixed set of nodes
		// simplifies querying and analysis. Could always re-run gather if a new server just joined.
		if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
			c.logWarning("Ignoring JS info response from unknown server: %s", serverName)
			return
		}

		if _, isDuplicateResponse := jsInfoResponses[serverName]; isDuplicateResponse {
			c.logWarning("Ignoring duplicate JS info response for account %s from server %s", accountId, serverName)
			return
		}

		jsInfoResponse := &server.JSInfo{}
		err = json.Unmarshal(apiResponse.Data, jsInfoResponse)
		if err != nil {
			c.logWarning("Failed to deserialize JS info response data for account %s: %s", accountId, err)
			return
		}

		if len(jsInfoResponse.AccountDetails) == 0 {
			// No account details in response, don't bother saving this
			//c.logWarning("🐛 Skip JSZ response from %s, no accounts details", serverName)
			return
		} else if len(jsInfoResponse.AccountDetails) > 1 {
			// Server will respond with multiple accounts if the one specified in the request is not found
			// https://github.com/nats-io/nats-server/pull/5229
			//c.logWarning("🐛 Skip JSZ response from %s, account not found", serverName)
			return
		}

		jsInfoResponses[serverName] = jsInfoResponse
	})
	if err != nil {
		return fmt.Errorf("failed to retrieve account %s streams: %w", accountId, err)
	}

	streamNamesSet := make(map[string]any)

	// Capture stream info from each known replica
	for serverName, jsInfo := range jsInfoResponses {
		// Cases where len(jsInfo.AccountDetails) != 1 are filtered above
		accountDetail := jsInfo.AccountDetails[0]

		for _, streamInfo := range accountDetail.Streams {
			streamName := streamInfo.Name

			_, streamKnown := streamNamesSet[streamName]
			if !streamKnown {
				c.logProgress("Discovered stream %s in account %s", streamName, accountId)
			}

			clusterTag := archive.TagNoCluster()
			if streamInfo.Cluster != nil {
				clusterTag = archive.TagCluster(streamInfo.Cluster.Name)
			}

			tags := []*archive.Tag{
				archive.TagAccount(accountId),
				archive.TagServer(serverName),
				clusterTag,
				archive.TagStream(streamName),
				archive.TagStreamInfo(),
			}

			err = aw.Add(streamInfo, tags...)
			if err != nil {
				return fmt.Errorf("failed to add stream %s info to archive: %w", streamName, err)
			}

			streamNamesSet[streamName] = nil
		}
	}

	c.logProgress("Discovered %d streams in account %s", len(streamNamesSet), accountId)
	return nil
}

// Capture runtime information about the capture
func (c *auditGatherCmd) captureMetadata(nc *nats.Conn, aw *archive.Writer) error {
	{
		username := "?"
		currentUser, err := user.Current()
		if err != nil {
			c.logWarning("Failed to capture username: %s", err)
		} else {
			username = fmt.Sprintf("%s (%s)", currentUser.Username, currentUser.Name)
		}

		metadata := &auditMetadata{
			Timestamp:              time.Now(),
			ConnectedServerName:    nc.ConnectedServerName(),
			ConnectedServerVersion: nc.ConnectedServerVersion(),
			ConnectURL:             nc.ConnectedUrl(),
			UserName:               username,
			CLIVersion:             Version,
		}

		err = aw.Add(&metadata, archive.TagSpecial("audit_gather_metadata"))
		if err != nil {
			return fmt.Errorf("failed to save metadata: %w", err)
		}
	}
	return nil
}

// logProgress prints updates to the gathering process. It can be turned off to make capture less verbose.
// Updates are also tee'd to the capture log
func (c *auditGatherCmd) logProgress(format string, args ...any) {
	if c.progress {
		fmt.Printf(format+"\n", args...)
	}
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}

// logWarning prints non-fatal errors during the gathering process. Messages are also tee'd to the capture log
func (c *auditGatherCmd) logWarning(format string, args ...any) {
	fmt.Printf("(!) "+format+"\n", args...)
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}

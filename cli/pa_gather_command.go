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
	"github.com/mprimi/natscli/archive"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type PaGatherCmd struct {
	archiveFilePath    string
	noConsumerInfo     bool
	noStreamInfo       bool
	noPrintProgress    bool
	noServerEndpoints  bool
	noAccountEndpoints bool
	serverProfiles     bool
	captureLogWriter   io.Writer
}

// CustomServerAPIResponse is a modified version of server.ServerAPIResponse that inhibits deserialization of the
// `data` field by making it a `json.RawMessage`.
// This is necessary because deserializing into a generic map can cause loss of precision for large numbers.
type CustomServerAPIResponse struct {
	Server *server.ServerInfo `json:"server"`
	Data   json.RawMessage    `json:"data,omitempty"`
	Error  *server.ApiError   `json:"error,omitempty"`
}

var profileTypes = []string{
	"goroutine",
	"heap",
	"allocs",
	//"threadcreate",
	//"block",
	//"mutex",
}

type endpointCaptureConfig struct {
	apiSuffix     string
	responseValue any
	typeTag       *archive.Tag
}

var serverEndpoints = []endpointCaptureConfig{
	{
		"VARZ",
		server.Varz{},
		archive.TagServerVars(),
	},
	{
		"CONNZ",
		server.Connz{},
		archive.TagConnections(),
	},
	{
		"ROUTEZ",
		server.Routez{},
		archive.TagRoutes(),
	},
	{
		"GATEWAYZ",
		server.Gatewayz{},
		archive.TagGateways(),
	},
	{
		"LEAFZ",
		server.Leafz{},
		archive.TagLeafs(),
	},
	{
		"SUBSZ",
		server.Subsz{},
		archive.TagSubs(),
	},
	{
		"JSZ",
		server.JSInfo{},
		archive.TagJetStream(),
	},
	{
		"ACCOUNTZ",
		server.Accountz{},
		archive.TagAccounts(),
	},
	{
		"HEALTHZ",
		server.HealthStatus{},
		archive.TagHealth(),
	},
}

var accountEndpoints = []endpointCaptureConfig{
	{
		"CONNZ",
		server.Connz{},
		archive.TagConnections(),
	},
	{
		"LEAFZ",
		server.Leafz{},
		archive.TagLeafs(),
	},
	{
		"SUBSZ",
		server.Subsz{},
		archive.TagSubs(),
	},
	{
		"INFO",
		server.AccountInfo{},
		archive.TagAccounts(),
	},
	{
		"JSZ",
		server.JetStreamStats{},
		archive.TagJetStream(),
	},
}

func configurePaGatherCommand(srv *fisk.CmdClause) {
	c := &PaGatherCmd{}

	gather := srv.Command("gather", "capture a variety of data from a deployment into an archive file").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.archiveFilePath)
	gather.Flag("no-server-endpoints", "skip capturing of server endpoints").UnNegatableBoolVar(&c.noServerEndpoints)
	gather.Flag("no-account-endpoints", "skip capturing of account endpoints").UnNegatableBoolVar(&c.noAccountEndpoints)
	gather.Flag("no-streams", "skip capturing of stream details").UnNegatableBoolVar(&c.noStreamInfo)
	gather.Flag("no-consumers", "skip capturing of stream consumer details").UnNegatableBoolVar(&c.noConsumerInfo)
	gather.Flag("profiles", "capture profiles for each servers").UnNegatableBoolVar(&c.serverProfiles)
	gather.Flag("no-progress", "silence log messages detailing progress during gathering").UnNegatableBoolVar(&c.noPrintProgress)
}

/*
Overview of gathering strategy:

 1. Query $SYS.REQ.SERVER.PING to discover servers
    ··Foreach answer, save server name and ID

 2. Query $SYS.REQ.SERVER.PING.ACCOUNTZ to discover accounts
    ··Foreach answer, save list of known accounts
    ··Also track the system account name

 3. Foreach known server
    ··Foreach server endpoint
    ····Request $SYS.REQ.SERVER.<Server ID>.<Endpoint>, save the response
    ··Foreach profile type
    ····Request $SYS.REQ.SERVER.<Profile>.PROFILEZ and save the response

 4. Foreach known account
    ··Foreach server endpoint
    ····Foreach response to $SYS.REQ.ACCOUNT.<Account name>.<Endpoint>
    ······Save the response

 5. Foreach known account
    ··Foreach response to $SYS.REQ.SERVER.PING.JSZ filtered by account
    ····Foreach stream in response
    ······Save the stream details
*/
func (c *PaGatherCmd) gather(_ *fisk.ParseContext) error {
	// nats connection
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// If no output path is provided, create one in os.Temp
	if c.archiveFilePath == "" {
		c.archiveFilePath = filepath.Join(os.TempDir(), "archive.zip")
	}

	// Initialize buffer to capture gathering log
	var captureLogBuffer bytes.Buffer
	c.captureLogWriter = &captureLogBuffer

	// Create an archive writer
	aw, err := archive.NewWriter(c.archiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer func() {
		// Add the (buffered) gathering log to the archive
		if c.captureLogWriter != nil {
			err = aw.AddCaptureLog(bytes.NewReader(captureLogBuffer.Bytes()))
			if err != nil {
				fmt.Printf("Failed to add capture log artifact: %s", err)
			}
			c.captureLogWriter = nil
		}

		err := aw.Close()
		if err != nil {
			fmt.Printf("Failed to close archive: %s", err)
		}
		fmt.Printf("📁 Archive created at: %s\n", c.archiveFilePath)
	}()

	err = c.captureMetadata(aw, nc)
	if err != nil {
		return fmt.Errorf("failed to save capture metadata: %w", err)
	}

	// Server ID -> ServerInfo map
	var serverInfoMap = make(map[string]*server.ServerInfo)

	// Discover servers by broadcasting a PING and then waiting for responses
	c.logProgress("⏳ Broadcasting PING to discover servers... (this may take a few seconds)")
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING", 0, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err = json.Unmarshal(b, &apiResponse); err != nil {
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
		c.logProgress("📣 Discovered server '%s' (%s)", serverName, serverId)
	})
	if err != nil {
		return fmt.Errorf("failed to PING: %w", err)
	}
	c.logProgress("ℹ️ Discovered %d servers", len(serverInfoMap))

	// Account name -> count of servers
	var accountIdsToServersCountMap = make(map[string]int)
	var systemAccount = ""

	// Broadcast PING.ACCOUNTZ to discover accounts
	c.logProgress("⏳ Broadcasting PING to discover accounts... ")
	err = doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", len(serverInfoMap), nc, func(b []byte) {
		var apiResponse CustomServerAPIResponse
		err = json.Unmarshal(b, &apiResponse)
		if err != nil {
			c.logWarning("Failed to deserialize ACCOUNTZ response, ignoring")
			return
		}

		serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

		// Ignore responses from servers not discovered earlier.
		// We are discarding useful data, but limiting additional collection to a fixed set of nodes
		// simplifies querying and analysis. Could always re-run gather if a new server just joined.
		if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
			c.logWarning("Ignoring ACCOUNTZ response from unknown server: %s", serverName)
			return
		}

		var accountsResponse server.Accountz
		err = json.Unmarshal(apiResponse.Data, &accountsResponse)
		if err != nil {
			c.logWarning("Failed to deserialize PING.ACCOUNTZ response: %s", err)
			return
		}

		c.logProgress("📣 Discovered %d accounts on server %s", len(accountsResponse.Accounts), serverName)

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
			c.logProgress("ℹ️ Discovered system account name: %s", systemAccount)
		} else if systemAccount != accountsResponse.SystemAccount {
			// This should not happen under normal circumstances!
			c.logWarning("Multiple system accounts detected (%s, %s)", systemAccount, accountsResponse.SystemAccount)
		} else {
			// Known system account matches the one in the response, nothing to do
		}
	})
	if err != nil {
		return fmt.Errorf("failed to PING.ACCOUNTZ: %w", err)
	}
	c.logProgress("ℹ️ Discovered %d accounts over %d servers", len(accountIdsToServersCountMap), len(serverInfoMap))

	if c.noServerEndpoints {
		c.logProgress("Skipping servers endpoints data gathering")
	} else {
		// For each known server, query a set of endpoints
		c.logProgress("⏳ Querying %d endpoints on %d known servers...", len(serverEndpoints), len(serverInfoMap))
		capturedCount := 0
		for serverId, serverInfo := range serverInfoMap {
			serverName := serverInfo.Name
			for _, endpoint := range serverEndpoints {

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

				var apiResponse CustomServerAPIResponse
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
					return fmt.Errorf("failed to add response to %s from to archive: %w", subject, err)
				}

				capturedCount += 1
			}
		}
		c.logProgress("ℹ️ Captured %d endpoint responses from %d servers", capturedCount, len(serverInfoMap))
	}

	if !c.serverProfiles {
		c.logProgress("Skipping server profiles gathering")
	} else {
		// For each known server, query for a set of profiles
		c.logProgress("⏳ Querying %d profiles endpoints on %d known servers...", len(profileTypes), len(serverInfoMap))
		capturedCount := 0
		for serverId, serverInfo := range serverInfoMap {
			serverName := serverInfo.Name
			for _, profileType := range profileTypes {

				subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.PROFILEZ", serverId)
				payload := server.ProfilezOptions{
					Name:  profileType,
					Debug: 0,
				}

				responses, err := doReq(payload, subject, 1, nc)
				if err != nil {
					c.logWarning("Failed to request profile %s from server %s: %s", profileType, serverName, err)
					continue
				}

				if len(responses) != 1 {
					c.logWarning("Unexpected number of responses for PROFILEZ from server %s: %d", serverName, len(responses))
					continue
				}

				responseBytes := responses[0]

				var apiResponse struct {
					Server *server.ServerInfo     `json:"server"`
					Data   *server.ProfilezStatus `json:"data,omitempty"`
					Error  *server.ApiError       `json:"error,omitempty"`
				}
				if err = json.Unmarshal(responseBytes, &apiResponse); err != nil {
					c.logWarning("Failed to deserialize PROFILEZ response from server %s: %s", serverName, err)
					continue
				}
				if apiResponse.Error != nil {
					c.logWarning("Failed to retrieve profile %s from server %s: %s", profileType, serverName, apiResponse.Error.Description)
					continue
				}

				profileStatus := apiResponse.Data
				if profileStatus.Error != "" {
					c.logWarning("Failed to retrieve profile %s from server %s: %s", profileType, serverName, profileStatus.Error)
					continue
				}

				tags := []*archive.Tag{
					archive.TagServer(serverName), // Source server
					archive.TagServerProfile(),
					archive.TagProfileName(profileType),
				}

				if serverInfo.Cluster != "" {
					tags = append(tags, archive.TagCluster(serverInfo.Cluster))
				} else {
					tags = append(tags, archive.TagNoCluster())
				}

				profileDataBytes := apiResponse.Data.Profile

				err = aw.AddObject(bytes.NewReader(profileDataBytes), tags...)
				if err != nil {
					return fmt.Errorf("failed to add profile %s from to archive: %w", profileType, err)
				}

				capturedCount += 1

			}
		}
		c.logProgress("ℹ️ Captured %d server profiles from %d servers", capturedCount, len(serverInfoMap))
	}

	if c.noAccountEndpoints {
		c.logProgress("Skipping accounts endpoints data gathering")
	} else {
		// For each known account, query a set of endpoints
		capturedCount := 0
		c.logProgress("⏳ Querying %d endpoints for %d known accounts...", len(accountEndpoints), len(accountIdsToServersCountMap))
		for accountId, serversCount := range accountIdsToServersCountMap {
			for _, endpoint := range accountEndpoints {
				subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountId, endpoint.apiSuffix)
				endpointResponses := make(map[string]interface{}, serversCount)

				err = doReqAsync(nil, subject, serversCount, nc, func(b []byte) {
					var apiResponse CustomServerAPIResponse
					err := json.Unmarshal(b, &apiResponse)
					if err != nil {
						c.logWarning("Failed to deserialize %s response for account %s: %s", endpoint.apiSuffix, accountId, err)
						return
					}

					serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

					// Ignore responses from servers not discovered earlier.
					// We are discarding useful data, but limiting additional collection to a fixed set of nodes
					// simplifies querying and analysis. Could always re-run gather if a new server just joined.
					if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
						c.logWarning("Ignoring ACCOUNT.%s response from unknown server: %s\n", endpoint.apiSuffix, serverName)
						return
					}

					endpointResponse := reflect.New(reflect.TypeOf(endpoint.responseValue)).Interface()
					err = json.Unmarshal(apiResponse.Data, endpointResponse)
					if err != nil {
						c.logWarning("Failed to deserialize ACCOUNT.%s response for account %s: %s\n", endpoint.apiSuffix, accountId, err)
						return
					}

					if _, isDuplicateResponse := endpointResponses[serverName]; isDuplicateResponse {
						c.logWarning("Ignoring duplicate ACCOUNT.%s response from server %s", endpoint.apiSuffix, serverName)
						return
					}

					endpointResponses[serverName] = endpointResponse
				})
				if err != nil {
					c.logWarning("Failed to request %s for account %s: %s", endpoint.apiSuffix, accountId, err)
					continue
				}

				// Store all responses for this account endpoint
				for serverName, endpointResponse := range endpointResponses {
					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(serverName), // Source server
						endpoint.typeTag,              // Type of artifact
					}

					err = aw.Add(endpointResponse, tags...)
					if err != nil {
						return fmt.Errorf("failed to add response to %s to archive: %w", subject, err)
					}

					capturedCount += 1
				}
			}
		}
		c.logProgress("ℹ️ Captured %d endpoint responses from %d accounts", capturedCount, len(accountIdsToServersCountMap))
	}

	// Capture streams info using JSZ, unless configured to skip
	if c.noStreamInfo {
		c.logProgress("Skipping streams data gathering")
	} else {
		c.logProgress("⏳ Gathering streams data...")
		capturedCount := 0
		for accountId, numServers := range accountIdsToServersCountMap {

			// Skip system account, JetStream is probably not enabled
			if accountId == systemAccount {
				continue
			}

			jszOptions := server.JSzOptions{
				Account:    accountId,
				Streams:    true,
				Consumer:   !c.noConsumerInfo, // Capture consumers, unless configured to skip
				Config:     true,
				RaftGroups: true,
			}

			jsInfoResponses := make(map[string]*server.JSInfo, numServers)
			err = doReqAsync(jszOptions, "$SYS.REQ.SERVER.PING.JSZ", numServers, nc, func(b []byte) {
				var apiResponse CustomServerAPIResponse
				err := json.Unmarshal(b, &apiResponse)
				if err != nil {
					c.logWarning("Failed to deserialize JSZ response for account %s: %s", accountId, err)
					return
				}

				serverId, serverName := apiResponse.Server.ID, apiResponse.Server.Name

				// Ignore responses from servers not discovered earlier.
				// We are discarding useful data, but limiting additional collection to a fixed set of nodes
				// simplifies querying and analysis. Could always re-run gather if a new server just joined.
				if _, serverKnown := serverInfoMap[serverId]; !serverKnown {
					c.logWarning("Ignoring JSZ response from unknown server: %s", serverName)
					return
				}

				if _, isDuplicateResponse := jsInfoResponses[serverName]; isDuplicateResponse {
					c.logWarning("Ignoring duplicate JSZ response for account %s from server %s", accountId, serverName)
					return
				}

				jsInfoResponse := &server.JSInfo{}
				err = json.Unmarshal(apiResponse.Data, jsInfoResponse)
				if err != nil {
					c.logWarning("Failed to deserialize JSZ response data for account %s: %s", accountId, err)
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
				c.logWarning("Failed to request JSZ for account %s: %s", accountId, err)
				continue
			}

			streamNamesMap := make(map[string]any)

			for serverName, jsInfo := range jsInfoResponses {

				// Cases where len(jsInfo.AccountDetails) != 1 are filtered above
				accountDetails := jsInfo.AccountDetails[0]

				for _, streamDetail := range accountDetails.Streams {
					streamName := streamDetail.Name

					_, streamKnown := streamNamesMap[streamName]
					if !streamKnown {
						c.logProgress("📣 Discovered stream %s in account %s", streamName, accountId)
					}

					tags := []*archive.Tag{
						archive.TagAccount(accountId),
						archive.TagServer(serverName), // Source server
						archive.TagStreamDetails(),
						archive.TagStream(streamName),
					}

					if streamDetail.Cluster != nil {
						tags = append(tags, archive.TagCluster(streamDetail.Cluster.Name))
					} else {
						tags = append(tags, archive.TagNoCluster())
					}

					err = aw.Add(streamDetail, tags...)
					if err != nil {
						return fmt.Errorf("failed to add stream %s details to archive: %w", streamName, err)
					}

					streamNamesMap[streamName] = nil
				}
			}

			c.logProgress("ℹ️ Discovered %d streams in account %s", len(streamNamesMap), accountId)
			capturedCount += len(streamNamesMap)

		}
		c.logProgress("ℹ️ Discovered %d streams in %d accounts", capturedCount, len(accountIdsToServersCountMap))
	}

	return nil
}

type gatherMetadata struct {
	Timestamp              time.Time `json:"capture_timestamp"`
	ConnectedServerName    string    `json:"connected_server_name"`
	ConnectedServerVersion string    `json:"connected_server_version"`
	ConnectURL             string    `json:"connect_url"`
	UserName               string    `json:"user_name"`
	CLIVersion             string    `json:"cli_version"`
}

// captureMetadata captures some runtime metadata and saves it into a special file in the output archive
// This is useful to know who/when/where ran the gather command.
func (c *PaGatherCmd) captureMetadata(aw *archive.Writer, nc *nats.Conn) error {

	username := "?"
	currentUser, err := user.Current()
	if err != nil {
		c.logWarning("Failed to capture username: %s", err)
	} else {
		username = fmt.Sprintf("%s (%s)", currentUser.Username, currentUser.Name)
	}

	metadata := &gatherMetadata{
		Timestamp:              time.Now(),
		ConnectedServerName:    nc.ConnectedServerName(),
		ConnectedServerVersion: nc.ConnectedServerVersion(),
		ConnectURL:             nc.ConnectedUrl(),
		UserName:               username,
		CLIVersion:             Version,
	}

	return aw.AddCaptureMetadata(metadata)
}

// logProgress prints updates to the gathering process. It can be turned off to make capture less verbose.
// Updates are also tee'd to the capture log
func (c *PaGatherCmd) logProgress(format string, args ...any) {
	if !c.noPrintProgress {
		fmt.Printf(format+"\n", args...)
	}
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}

// logWarning prints non-fatal errors during the gathering process. Messages are also tee'd to the capture log
func (c *PaGatherCmd) logWarning(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	if c.captureLogWriter != nil {
		_, _ = fmt.Fprintf(c.captureLogWriter, format+"\n", args...)
	}
}

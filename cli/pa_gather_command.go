package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/choria-io/fisk"
	"github.com/mprimi/natscli/archive"
	"github.com/nats-io/nats-server/v2/server"
)

type PaGatherCmd struct {
	archiveFilePath string
	noConsumerInfo  bool
	noStreamInfo    bool
}

type Endpoint struct {
	name           string
	expectedStruct any
	typeTag        *archive.Tag
}

// We can't use server.ServerAPIResponse since it will unmarshal Data into a map, causing
// a loss of precision for large numbers when remarshalling and unmarshalling again
type CustomServerAPIResponse struct {
	Server *server.ServerInfo `json:"server"`
	Data   json.RawMessage    `json:"data,omitempty"`
	Error  *server.ApiError   `json:"error,omitempty"`
}

var serverEndpoints = []Endpoint{
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

var accountEndpoints = []Endpoint{
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

	gather := srv.Command("gather", "create archive of monitoring data for all servers and accounts").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.archiveFilePath)
	gather.Flag("no-consumers", "do not include consumer data").UnNegatableBoolVar(&c.noConsumerInfo)
	gather.Flag("no-streams", "do not include stream info data").UnNegatableBoolVar(&c.noStreamInfo)
}

// This method collects data from all servers and accounts and writes it to an archive. The following instruction are describe what information it collects and how it stores it in the archive:
// 1. Discover all servers by sending a request to subject `$SYS.REQ.SERVER.PING` waiting until all possible responses have been returned by the timeout.
// 2. Iterate over the list of servers we discovered to get data on each endpoint (JSZ, VARZ, etc.) by sending a request to `$SYS.REQ.SERVER.<server_id>.<endpoint>`. Each request is stored in an artifact tagged with: the name of the server, the cluster name, and the endpoint type.
// 3. Discover all account ids by sending a request to subject `$SYS.REQ.SERVER.PING.ACCOUNTZ`, using all responses returned to create a set of account ids. We also keep track of the system account and panic if more than one system account is found.
// 4. Send a request to `$SYS.REQ.ACCOUNT.<account_id>.<endpoint>`, to get endpoint (connz, subsz, etc.) information for each account discovered. An artifact is stored for each endpoint for an account, tagged with account id and the endpoint type.
// 5. For each account (except for the system account), request jsz info for the account id from all servers. This is done by requesting from the subject `$SYS.REQ.SERVER.PING.JSZ` with a json payload of type `server.JSzOptions`, specifiying the account id. The response will contain a list of account details (which should only contain a single element since we only requested info on one account), with a list of streams. Information on each stream (and their consumers, config, raft-group, etc.) is stored as an artifact tagged with the account id, cluster name, server name, and the stream name.
func (c *PaGatherCmd) gather(_ *fisk.ParseContext) error {
	// nats connection
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	// create temp file if not provided
	if c.archiveFilePath == "" {
		c.archiveFilePath = filepath.Join(os.TempDir(), "archive.zip")
	}

	// archive writer
	aw, err := archive.NewWriter(c.archiveFilePath)
	if err != nil {
		return err
	}
	defer aw.Close()

	// discover servers
	servers := []*server.ServerInfo{}
	if err = doReqAsync(nil, "$SYS.REQ.SERVER.PING", 0, nc, func(b []byte) {
		var apiResponse server.ServerAPIResponse
		if err = json.Unmarshal(b, &apiResponse); err != nil {
			panic(err)
		}
		servers = append(servers, apiResponse.Server)
	}); err != nil {
		return err
	}

	// get data from all endpoints for all servers
	for _, serverInfo := range servers {
		for _, endpoint := range serverEndpoints {

			subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", serverInfo.ID, endpoint.name)

			var apiResponseBytes []byte
			err = doReqAsync(nil, subject, 1, nc, func(b []byte) {
				apiResponseBytes = b
			})

			var apiResponse CustomServerAPIResponse
			if err = json.Unmarshal(apiResponseBytes, &apiResponse); err != nil {
				return err
			}

			// remarshal to get the api response payload struct
			var apiResponseDataBytes []byte
			apiResponseDataBytes, err = json.Marshal(apiResponse.Data)
			if err != nil {
				return err
			}

			resp := reflect.New(reflect.TypeOf(endpoint.expectedStruct)).Interface()
			if err = json.Unmarshal(apiResponseDataBytes, resp); err != nil {
				return err
			}
			aw.Add(resp, archive.TagServer(serverInfo.Name), archive.TagCluster(serverInfo.Cluster), endpoint.typeTag)
		}
	}
	fmt.Printf("🔬 Collected data for %d servers\n", len(servers))

	// retrieve all accounts and system account id
	var (
		systemAccountId string
		accountIdsSet   = make(map[string]struct{})
	)

	if err = doReqAsync(nil, "$SYS.REQ.SERVER.PING.ACCOUNTZ", 0, nc, func(b []byte) {
		var apiResponse CustomServerAPIResponse
		if err = json.Unmarshal(b, &apiResponse); err != nil {
			panic(err)
		}

		var bytes []byte
		bytes, err = json.Marshal(apiResponse.Data)
		if err != nil {
			panic(err)
		}

		var accounts *server.Accountz
		if err = json.Unmarshal(bytes, &accounts); err != nil {
			panic(err)
		}
		for _, account := range accounts.Accounts {
			accountIdsSet[account] = struct{}{}
		}

		// get system account id
		if accounts.SystemAccount != "" {
			systemAccountId = accounts.SystemAccount
		}

		// guard against system account id mismatch
		if systemAccountId != accounts.SystemAccount {
			panic(fmt.Sprintf("system account id mismatch: %s != %s\n", systemAccountId, accounts.SystemAccount))
		}
	}); err != nil {
		return err
	}

	// get account endpoint data
	for accountId := range accountIdsSet {
		for _, endpoint := range accountEndpoints {
			subject := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.%s", accountId, endpoint.name)

			var apiResponseBytes []byte
			if err = doReqAsync(nil, subject, 1, nc, func(b []byte) {
				apiResponseBytes = b
			}); err != nil {
				return err
			}

			var apiResponse CustomServerAPIResponse
			if err = json.Unmarshal(apiResponseBytes, &apiResponse); err != nil {
				return err
			}
			// handle api response error
			if apiResponse.Error != nil {
				fmt.Printf("%s cannot be collected for account %s, error: %s\n", endpoint.name, accountId, apiResponse.Error)
				continue
			}

			var apiResponseDataBytes []byte
			apiResponseDataBytes, err = json.Marshal(apiResponse.Data)
			if err != nil {
				return err
			}

			resp := reflect.New(reflect.TypeOf(endpoint.expectedStruct)).Interface()
			if err = json.Unmarshal(apiResponseDataBytes, resp); err != nil {
				fmt.Printf("failed to unmarshal %s api response from account %s: %s\n", endpoint.name, accountId, err.Error())
				continue
			}

			// add to archive
			if err = aw.Add(resp, archive.TagAccount(accountId), endpoint.typeTag); err != nil {
				return err
			}
		}
	}
	fmt.Printf("🔬 Collected data for %d accounts\n", len(accountIdsSet))

	if !c.noStreamInfo {
		streamsCollected := 0
		for accountId := range accountIdsSet {
			// skip system account
			if accountId == systemAccountId {
				continue
			}
			jszOptions := server.JSzOptions{
				Account:    accountId,
				Streams:    true,
				Consumer:   !c.noConsumerInfo,
				Config:     true,
				RaftGroups: true,
			}

			var serverResponsesBytes [][]byte
			if err = doReqAsync(jszOptions, "$SYS.REQ.SERVER.PING.JSZ", 0, nc, func(bytePayload []byte) {
				serverResponsesBytes = append(serverResponsesBytes, bytePayload)
			}); err != nil {
				return err
			}

			for _, serverResponseBytes := range serverResponsesBytes {

				var apiResponse CustomServerAPIResponse
				if err = json.Unmarshal(serverResponseBytes, &apiResponse); err != nil {
					return err
				}

				// handle api response error
				if apiResponse.Error != nil {
					fmt.Printf("%s cannot be collected for account %s, error: %s\n", accountId, accountId, apiResponse.Error)
					continue
				}

				var apiResponseDataBytes []byte
				apiResponseDataBytes, err = json.Marshal(apiResponse.Data)
				if err != nil {
					return err
				}

				var jsInfo server.JSInfo
				if err = json.Unmarshal(apiResponseDataBytes, &jsInfo); err != nil {
					fmt.Printf("failed to unmarshal Jsz api response from account %s: %s\n", accountId, err.Error())
					continue
				}

				// skip if too many accounts are returned
				if len(jsInfo.AccountDetails) > 1 {
					continue
				}
				for _, ad := range jsInfo.AccountDetails {
					// skip if wrong account id is returned
					if accountId != ad.Id {
						continue
					}

					for _, sd := range ad.Streams {
						if err = aw.Add(sd, archive.TagAccount(accountId), archive.TagCluster(sd.Cluster.Name), archive.TagServer(apiResponse.Server.Name), archive.TagStream(sd.Name)); err != nil {
							return err
						}
						streamsCollected++
					}
				}
			}
		}
		fmt.Printf("🔬 Collected %d streams\n", streamsCollected)
	}

	// print path to archive
	fmt.Printf("📁 Archive created at: %s\n", c.archiveFilePath)
	return nil
}

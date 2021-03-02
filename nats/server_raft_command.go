// Copyright 2020 The NATS Authors
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

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvRaftCmd struct {
	json             bool
	force            bool
	peer             string
	placementCluster string
}

func configureServerRaftCommand(srv *kingpin.CmdClause) {
	c := &SrvRaftCmd{}

	raft := srv.Command("raft", "Manage JetStream Clustering").Alias("r")
	raft.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	sd := raft.Command("step-down", "Force a new leader election by standing down the current meta leader").Alias("stepdown").Alias("sd").Alias("elect").Alias("down").Alias("d").Action(c.metaLeaderStandDown)
	sd.Flag("cluster", "Request placement of the leader in a specific cluster").StringVar(&c.placementCluster)

	rm := raft.Command("peer-remove", "Removes a server from a JetStream cluster").Alias("rm").Alias("pr").Action(c.metaPeerRemove)
	rm.Arg("id", "The Server ID to remove from the JetStream cluster").StringVar(&c.peer)
	rm.Flag("f", "Force removal without prompting").BoolVar(&c.force)
}

func (c *SrvRaftCmd) metaPeerRemove(_ *kingpin.ParseContext) error {
	timeout = 2 * time.Second

	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := doReq(nil, "$SYS.REQ.SERVER.PING.JSZ", -1, nc)
	if err != nil {
		return err
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	found := false
	var server *jszr

	for _, r := range res {
		response := jszr{}

		err = json.Unmarshal(r, &response)
		if err != nil {
			return err
		}

		if response.Server.ID == c.peer {
			found = true
			server = &response
		}
	}

	if !found {
		return fmt.Errorf("did not find a JetStream server with ID %s", c.peer)
	}

	if !c.force {
		bold := color.New(color.Bold)
		fmt.Printf("Removing server %s (%s) in cluster %s with %s Streams, %s Consumers holding %s data\n", bold.Sprint(server.Server.Name), server.Server.ID, bold.Sprint(server.Server.Cluster), bold.Sprint(humanize.Comma(int64(server.Data.StreamCnt))), bold.Sprint(humanize.Comma(int64(server.Data.ConsumerCnt))), bold.Sprint(humanize.IBytes(server.Data.MessageBytes)))
		fmt.Println()
		fmt.Printf("This will disable JetStream on the server:\n\n * All data will become inaccessible\n * Streams with multiple replicas will immediately initiate data synchronization\n * R1 Streams on this server will be lost\n * This might cause short periods of downtime on those Streams and Consumers\n")
		fmt.Println()
		bold.Println("Only perform this action on servers that will not intend to return, for normal maintenance just shut the servers down instead.")
		fmt.Println()

		remove, err := askConfirmation(fmt.Sprintf("Really remove peer %s", c.peer), false)
		kingpin.FatalIfError(err, "Could not prompt for confirmation")
		if !remove {
			fmt.Println("Removal canceled")
			os.Exit(0)
		}
	}

	err = mgr.MetaPeerRemove(c.peer)
	kingpin.FatalIfError(err, "Could not remove %s: %s", c.peer, err)

	return nil
}

func (c *SrvRaftCmd) metaLeaderStandDown(_ *kingpin.ParseContext) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	jreq, err := json.MarshalIndent(server.JSzOptions{LeaderOnly: true}, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode request: %s", err)
	}

	getJSI := func() (*server.JSInfo, error) {
		if trace {
			log.Printf(">>> $SYS.REQ.SERVER.PING.JSZ: %s\n", string(jreq))
		}

		msg, err := nc.Request("$SYS.REQ.SERVER.PING.JSZ", jreq, timeout)
		if err != nil {
			return nil, err
		}

		if trace {
			log.Printf(">>> %s\n", string(msg.Data))
		}

		resp := map[string]json.RawMessage{}
		err = json.Unmarshal(msg.Data, &resp)
		if err != nil {
			return nil, err
		}

		data, ok := resp["data"]
		if !ok {
			return nil, fmt.Errorf("no data received")
		}

		info := &server.JSInfo{}
		err = json.Unmarshal(data, info)
		if err != nil {
			return nil, err
		}

		return info, nil
	}

	resp, err := getJSI()
	if err != nil {
		return fmt.Errorf("could not obtain cluster information: %s", err)
	}

	if resp.Meta.Leader == "" {
		return fmt.Errorf("cluster has no current leader")
	}

	leader := resp.Meta.Leader

	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(resp.Meta.Replicas)+1)
	if c.placementCluster != "" {
		err = mgr.MetaLeaderStandDown(&api.Placement{Cluster: c.placementCluster})
	} else {
		err = mgr.MetaLeaderStandDown(nil)
	}
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTicker(500 * time.Millisecond).C {
		if ctr == 5 {
			return fmt.Errorf("stream did not elect a new leader in time")
		}
		ctr++

		resp, err = getJSI()
		if err != nil {
			log.Printf("Failed to retrieve Cluster State: %s", err)
			continue
		}

		if resp.Meta.Leader != leader {
			log.Printf("New leader elected %q", resp.Meta.Leader)
			os.Exit(0)
		}
	}

	if resp.Meta.Leader == leader {
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
		os.Exit(1)
	}

	return nil
}

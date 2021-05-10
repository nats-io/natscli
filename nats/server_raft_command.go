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
	rm.Arg("name", "The Server Name to remove from the JetStream cluster").StringVar(&c.peer)
	rm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)
}

func (c *SrvRaftCmd) metaPeerRemove(_ *kingpin.ParseContext) error {
	timeout = 2 * time.Second

	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := doReq(server.JSzOptions{LeaderOnly: true}, "$SYS.REQ.SERVER.PING.JSZ", 1, nc)
	if err != nil {
		return err
	}

	if len(res) != 1 {
		return fmt.Errorf("did not receive a response from the meta leader, ensure the account used has system privileges and appropriate permissions")
	}

	type jszr struct {
		Data   server.JSInfo     `json:"data"`
		Server server.ServerInfo `json:"server"`
	}

	found := false
	srv := &jszr{}
	err = json.Unmarshal(res[0], srv)
	if err != nil {
		return err
	}

	for _, r := range srv.Data.Meta.Replicas {
		if r.Name == c.peer {
			if !r.Offline {
				return fmt.Errorf("can only remove offline nodes")
			}
			found = true
		}
	}

	if !found {
		return fmt.Errorf("did not find a replica named %s", c.peer)
	}

	if !c.force {
		fmt.Printf("Removing %s can not be reversed, data on this node will be\ninaccessible and another one called %s can not join again.\n\n", c.peer, c.peer)

		remove, err := askConfirmation(fmt.Sprintf("Really remove peer %s", c.peer), false)
		kingpin.FatalIfError(err, "Could not prompt for confirmation")
		if !remove {
			fmt.Println("Removal canceled")
			os.Exit(0)
		}
	}

	err = mgr.MetaPeerRemove(c.peer)
	kingpin.FatalIfError(err, "Could not remove %s", c.peer)

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

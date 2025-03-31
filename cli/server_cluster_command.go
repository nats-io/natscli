// Copyright 2020-2024 The NATS Authors
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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/connbalancer"
	"github.com/nats-io/nats-server/v2/server"
)

type SrvClusterCmd struct {
	json              bool
	force             bool
	peer              string
	placementCluster  string
	placementNode     string
	placementTags     []string
	balanceServerName string
	balanceIdle       time.Duration
	balanceAccount    string
	balanceSubject    string
	balanceRunTime    time.Duration
	balanceKinds      []string
}

func configureServerClusterCommand(srv *fisk.CmdClause) {
	c := &SrvClusterCmd{}

	cluster := srv.Command("cluster", "Manage JetStream Clustering").Alias("r").Alias("raft")

	balance := cluster.Command("balance", "Balance cluster connections").Action(c.balanceAction)
	balance.Arg("duration", "Spread balance requests over a certain duration").Default("2m").DurationVar(&c.balanceRunTime)
	balance.Flag("server-name", "Restrict balancing to a specific server").PlaceHolder("NAME").StringVar(&c.balanceServerName)
	balance.Flag("idle", "Balance connections that has been idle for a period").PlaceHolder("DURATION").DurationVar(&c.balanceIdle)
	balance.Flag("account", "Balance connections in a certain account only").StringVar(&c.balanceAccount)
	balance.Flag("subject", "Balance connections interested in certain subjects").StringVar(&c.balanceSubject)
	balance.Flag("kind", "Balance only certain kinds of connection (*Client, Leafnode)").Default("Client").EnumsVar(&c.balanceKinds, "Client", "Leafnode")
	balance.Flag("force", "Force rebalance without prompting").Short('f').UnNegatableBoolVar(&c.force)

	sd := cluster.Command("step-down", "Force a new leader election by standing down the current meta leader").Alias("stepdown").Alias("sd").Alias("elect").Alias("down").Alias("d").Action(c.metaLeaderStandDownAction)
	sd.Flag("cluster", "Request placement of the leader in a specific cluster").StringVar(&c.placementCluster)
	sd.Flag("tags", "Request placement of the leader on nodes with specific tag(s)").StringsVar(&c.placementTags)
	sd.Flag("host", "Request placement of the leader on a specific node").StringVar(&c.placementNode)
	sd.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	sd.Flag("force", "Force leader step down ignoring current leader").Short('f').UnNegatableBoolVar(&c.force)

	rm := cluster.Command("peer-remove", "Removes a server from a JetStream cluster").Alias("rm").Alias("pr").Action(c.metaPeerRemoveAction)
	rm.Arg("name", "The Server Name or ID to remove from the JetStream cluster").Required().StringVar(&c.peer)
	rm.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)
	rm.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
}

func (c *SrvClusterCmd) balanceAction(_ *fisk.ParseContext) error {
	if !c.force {
		fmt.Println("Re-balancing will disconnect clients without knowing their current state.")
		fmt.Println()
		fmt.Println("The clients will trigger normal reconnect behavior. This can interrupt in-flight work.")
		fmt.Println()
		ok, err := askConfirmation("Really re-balance connections", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("balance canceled")
		}
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	level := api.InfoLevel
	if opts().Trace {
		level = api.TraceLevel
	}

	balancer, err := connbalancer.New(nc, c.balanceRunTime, api.NewDefaultLogger(level), connbalancer.ConnectionSelector{
		ServerName:      c.balanceServerName,
		Idle:            c.balanceIdle,
		Account:         c.balanceAccount,
		SubjectInterest: c.balanceSubject,
		Kind:            c.balanceKinds,
	})
	if err != nil {
		return err
	}

	balanced, err := balancer.Balance(ctx)
	if err != nil {
		return err
	}

	fmt.Println()

	fmt.Printf("Balanced %s connections\n", f(balanced))

	return nil
}

func (c *SrvClusterCmd) metaPeerRemoveAction(_ *fisk.ParseContext) error {
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
	foundName := ""
	foundID := ""
	state := "offline"

	srv := &jszr{}
	err = json.Unmarshal(res[0], srv)
	if err != nil {
		return err
	}

	for _, r := range srv.Data.Meta.Replicas {
		if r.Name == c.peer || r.Peer == c.peer {
			if !r.Offline {
				state = "online"
			}
			foundID = r.Peer
			foundName = r.Name
			found = true
		}
	}

	if !found {
		return fmt.Errorf("did not find a replica named %s", c.peer)
	}

	if !c.force {
		fmt.Println(`Removing peers is intended for peers that will not come back, generally for peers that will remain
in the cluster removing the peer is not needed - simply do your maintenance with the peer shut down.

To return a removed peer to the cluster you have to restart the entire cluster or super-cluster first.
If the peer was online when it was removed you need to restart the removed peer last.

R1 streams on this peer will migrate to other peers but data will be lost, even if the
peer returns later.

R1 consumers on this peer will lose their state and revert to starting configuring 
which may lead to duplicate deliveries.`)

		fmt.Println()

		var remove bool
		if c.peer == foundName || strings.Contains(foundName, foundID) {
			remove, err = askConfirmation(fmt.Sprintf("Really remove %s peer %s", state, foundName), false)
		} else {
			remove, err = askConfirmation(fmt.Sprintf("Really remove %s peer %s with id %s", state, foundName, foundID), false)
		}
		fisk.FatalIfError(err, "Could not prompt for confirmation")
		if !remove {
			fmt.Println("Removal canceled")
			os.Exit(0)
		}
	}

	if foundID != "" {
		err = mgr.MetaPeerRemove("", foundID)
	} else {
		err = mgr.MetaPeerRemove(foundName, foundID)
	}
	fisk.FatalIfError(err, "Could not remove %s", foundID)

	return nil
}

func (c *SrvClusterCmd) metaLeaderStandDownAction(_ *fisk.ParseContext) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	jreq, err := json.MarshalIndent(server.JSzOptions{LeaderOnly: true}, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode request: %s", err)
	}

	getJSI := func() (*server.JSInfo, error) {
		if opts().Trace {
			log.Printf(">>> $SYS.REQ.SERVER.PING.JSZ: %s\n", string(jreq))
		}

		msg, err := nc.Request("$SYS.REQ.SERVER.PING.JSZ", jreq, opts().Timeout)
		if err != nil {
			return nil, err
		}

		if opts().Trace {
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

	leader := resp.Meta.Leader
	if leader == "" && !c.force {
		return fmt.Errorf("cluster has no current leader")
	} else if leader == "" {
		leader = "<unknown>"
	}

	if c.placementNode != "" || len(c.placementTags) > 0 {
		// TODO: check api level instead but requires https://github.com/nats-io/nats-server/issues/6681
		fmt.Println("WARNING: Using placement tags or node name required NATS Server 2.11 or newer")
		fmt.Println()
	}

	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(resp.Meta.Replicas)+1)
	if c.placementCluster != "" || len(c.placementTags) > 0 || c.placementNode != "" {
		err = mgr.MetaLeaderStandDown(&api.Placement{
			Cluster:   c.placementCluster,
			Tags:      c.placementTags,
			Preferred: c.placementNode,
		})
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

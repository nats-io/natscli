// Copyright 2023 The NATS Authors
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

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
)

type SrvConfigCmd struct {
	serverID string
	force    bool
}

func configureServerConfigCommand(srv *fisk.CmdClause) {
	c := SrvConfigCmd{}

	cfg := srv.Command("config", "Interact with server configuration")

	reload := cfg.Command("reload", "Reloads the runtime configuration").Action(c.reloadAction)
	reload.Arg("id", "The server ID to trigger a reload for").Required().StringVar(&c.serverID)
	reload.Flag("force", "Force reload without prompting").Short('f').BoolVar(&c.force)
}

func (c *SrvConfigCmd) reloadAction(pc *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if !c.force {
		resps, err := doReq(nil, fmt.Sprintf("$SYS.REQ.SERVER.%s.VARZ", c.serverID), 1, nc)
		if err != nil {
			return err
		}

		if len(resps) != 1 {
			return fmt.Errorf("invalid response from %d servers", len(resps))
		}
		vz := server.ServerAPIResponse{}
		err = json.Unmarshal(resps[0], &vz)
		if err != nil {
			return err
		}

		ok, err := askConfirmation(fmt.Sprintf("Really reload configuration for %s (%s) on %s", vz.Server.Name, vz.Server.ID, vz.Server.Host), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	resps, err := doReq(nil, fmt.Sprintf("$SYS.REQ.SERVER.%s.RELOAD", c.serverID), 1, nc)
	if err != nil {
		return err
	}

	if len(resps) != 1 {
		return fmt.Errorf("invalid response from %d servers", len(resps))
	}

	nfo := &SrvInfoCmd{id: c.serverID}
	return nfo.info(pc)
}

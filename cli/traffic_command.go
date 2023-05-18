// Copyright 2020-2022 The NATS Authors
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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats.go"
)

type trafficCmd struct {
	raftVote       rateTrackInt
	raftAppend     rateTrackInt
	raftProp       rateTrackInt
	raftRemovePeer rateTrackInt
	raftReply      rateTrackInt
	raftC          rateTrackInt

	clusterStreamSync   rateTrackInt
	clusterStreamInfo   rateTrackInt
	clusterConsumerInfo rateTrackInt
	clusterJSAUpdate    rateTrackInt
	clusterReply        rateTrackInt
	clusterC            rateTrackInt

	jsAck rateTrackInt
	jsAPI rateTrackInt

	requests  rateTrackInt
	msgs      rateTrackInt
	systemMsg rateTrackInt
	genC      rateTrackInt
	size      rateTrackInt

	subjects string
}

type rateTrackInt struct {
	n int64
	p int64
	sync.Mutex
}

func (r *rateTrackInt) Comma() string  { return f(r.Rate()) }
func (r *rateTrackInt) IBytes() string { return humanize.IBytes(uint64(r.Rate())) }
func (r *rateTrackInt) Inc()           { r.IncN(1) }

func (r *rateTrackInt) Value() int64 {
	r.Lock()
	defer r.Unlock()
	return r.n
}

func (r *rateTrackInt) IncN(c int64) {
	r.Lock()
	r.n += c
	r.Unlock()
}

func (r *rateTrackInt) Rate() int64 {
	r.Lock()
	defer r.Unlock()

	rate := r.n - r.p
	r.p = r.n

	return rate
}

func configureTrafficCommand(app commandHost) {
	c := &trafficCmd{}

	traffic := app.Command("traffic", "Monitor NATS network traffic").Hidden().Action(c.monitor)
	traffic.Arg("subjects", "Subjects to monitor, defaults to all").Default(">").StringVar(&c.subjects)
}

func init() {
	registerCommand("traffic", 18, configureTrafficCommand)
}

func (c *trafficCmd) monitor(_ *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	sub, err := nc.Subscribe(c.subjects, func(m *nats.Msg) {
		c.size.IncN(int64(len(m.Data)))

		switch {
		case strings.HasPrefix(m.Subject, "$SYS."):
			c.systemMsg.Inc()
			c.genC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.ARU."):
			c.clusterJSAUpdate.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.CI."):
			c.clusterConsumerInfo.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.SI."):
			c.clusterStreamInfo.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.ACK."):
			c.clusterReply.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.R."):
			c.clusterReply.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$JSC.SYNC"):
			c.clusterStreamSync.Inc()
			c.clusterC.Inc()
		case strings.HasPrefix(m.Subject, "$NRG.V."):
			c.raftVote.Inc()
			c.raftC.Inc()
		case strings.HasPrefix(m.Subject, "$NRG.AE."):
			c.raftAppend.Inc()
			c.raftC.Inc()
		case strings.HasPrefix(m.Subject, "$NRG.P."):
			c.raftProp.Inc()
			c.raftC.Inc()
		case strings.HasPrefix(m.Subject, "$NRG.RP."):
			c.raftRemovePeer.Inc()
			c.raftC.Inc()
		case strings.HasPrefix(m.Subject, "$NRG.R."):
			c.raftReply.Inc()
			c.raftC.Inc()
		case strings.HasPrefix(m.Subject, "$JS.ACK."):
			c.jsAck.Inc()
			c.genC.Inc()
		case strings.HasPrefix(m.Subject, "$JS.API."):
			c.jsAPI.Inc()
			c.genC.Inc()
		case m.Reply != "":
			c.requests.Inc()
			c.genC.Inc()
		default:
			c.msgs.Inc()
			c.genC.Inc()
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	ticker := time.NewTicker(time.Second)

	raftRows := [][]any{}
	clusterRows := [][]any{}
	genRows := [][]any{}

	for range ticker.C {
		if runtime.GOOS != "windows" {
			fmt.Print("\033[2J")
			fmt.Print("\033[H")
		}

		if c.raftProp.Value() > 0 || c.raftReply.Value() > 0 || c.raftVote.Value() > 0 || c.raftAppend.Value() > 0 || c.raftRemovePeer.Value() > 0 {
			if len(raftRows) > 10 {
				raftRows = raftRows[1:]
			}
			raftRows = append(raftRows, []any{c.raftProp.Comma(), c.raftVote.Comma(), c.raftAppend.Comma(), c.raftRemovePeer.Comma(), c.raftReply.Comma(), c.raftC.Comma()})

			table := newTableWriter("Raft Traffic")
			table.AddHeaders("Proposal", "Vote", "Append", "Remove Peer", "Reply", "Total Messages")
			for i := range raftRows {
				table.AddRow(raftRows[i]...)
			}
			fmt.Println(table.Render())
		}

		if c.clusterJSAUpdate.Value() > 0 || c.clusterConsumerInfo.Value() > 0 || c.clusterStreamInfo.Value() > 0 || c.clusterReply.Value() > 0 || c.clusterStreamSync.Value() > 0 {
			if len(clusterRows) > 10 {
				clusterRows = clusterRows[1:]
			}
			clusterRows = append(clusterRows, []any{c.clusterJSAUpdate.Comma(), c.clusterStreamInfo.Comma(), c.clusterConsumerInfo.Comma(), c.clusterStreamSync.Comma(), c.clusterReply.Comma(), c.clusterC.Comma()})

			table := newTableWriter("Cluster Traffic")
			table.AddHeaders("JSA Update", "Stream Info", "Consumer Info", "Stream Sync", "Reply", "Total Messages")
			for i := range clusterRows {
				table.AddRow(clusterRows[i]...)
			}
			fmt.Println(table.Render())
		}

		if len(genRows) > 10 {
			genRows = genRows[1:]
		}
		genRows = append(genRows, []any{c.requests.Comma(), c.jsAPI.Comma(), c.jsAck.Comma(), c.systemMsg.Comma(), c.msgs.Comma(), c.size.IBytes(), c.genC.Comma()})

		table := newTableWriter("General Traffic")
		table.AddHeaders("Requests", "JS API", "JS ACK", "System", "Rest", "Total Bytes", "Total Messages")
		for i := range genRows {
			table.AddRow(genRows[i]...)
		}
		fmt.Println(table.Render())
	}

	return nil
}

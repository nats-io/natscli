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
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/api/server/zmonitor"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/asciigraph"
	iu "github.com/nats-io/natscli/internal/util"
	terminal "golang.org/x/term"
)

type SrvGraphCmd struct {
	id string
	js bool
}

func configureServerGraphCommand(srv *fisk.CmdClause) {
	c := &SrvGraphCmd{}

	graph := srv.Command("graph", "Show graphs for a single server").Action(c.graph)
	graph.Arg("server", "Server ID or Name to inspect").StringVar(&c.id)
	graph.Flag("jetstream", "Draw JetStream statistics").Short('j').UnNegatableBoolVar(&c.js)
}

func (c *SrvGraphCmd) graph(_ *fisk.ParseContext) error {
	if !c.js {
		return c.graphServer()
	}

	return c.graphJetStream()
}

func (c *SrvGraphCmd) graphWrapper(graphs int, h func(width int, height int, vz *zmonitor.VarzV1) ([]string, error)) error {
	if !iu.IsTerminal() {
		return fmt.Errorf("can only graph data on an interactive terminal")
	}

	width, height, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("failed to get terminal dimensions: %w", err)
	}

	minHeight := graphs*5 + 2 // 3 graph lines, the ruler, the heading and overall heading plus newline

	if width < 20 || height < minHeight {
		return fmt.Errorf("please increase terminal dimensions")
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	subj := fmt.Sprintf("$SYS.REQ.SERVER.%s.VARZ", c.id)
	body := []byte("{}")

	if len(c.id) != 56 || strings.ToUpper(c.id) != c.id {
		subj = "$SYS.REQ.SERVER.PING.VARZ"
		opts := server.VarzEventOptions{EventFilterOptions: server.EventFilterOptions{Name: c.id, ExactMatch: true}}
		body, err = json.Marshal(opts)
		if err != nil {
			return err
		}
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	vz, err := c.getVz(nc, subj, body)
	if err != nil {
		return err
	}

	_, err = h(width, height, vz)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			width, height, err = terminal.GetSize(int(os.Stdout.Fd()))
			if err != nil {
				height = 40
				width = 80
			}
			if width > 15 {
				width -= 11
			}
			if height > 10 {
				height -= graphs + 1 // make space for the main heading and gaps in the graphs etc
			}

			if width < 20 || height < minHeight {
				return fmt.Errorf("please increase terminal dimensions")
			}

			vz, err = c.getVz(nc, subj, body)
			if err != nil {
				return err
			}

			iu.ClearScreen()

			plots, err := h(width, height, vz)
			if err != nil {
				return err
			}

			for _, plot := range plots {
				fmt.Println(plot)
				fmt.Println()
			}

		case <-ctx.Done():
			iu.ClearScreen()
			return nil
		}
	}
}

func (c *SrvGraphCmd) graphJetStream() error {
	var memUsed, cpuUsed, fileUsed, haAssets []float64
	var apiRates, pending []float64
	var lastApi float64
	lastStateTs := time.Now()
	first := true

	return c.graphWrapper(6, func(width int, height int, vz *zmonitor.VarzV1) ([]string, error) {
		fmt.Printf("JetStream Statistics for %s\n", c.id)
		fmt.Println()

		if first {
			if vz.JetStream.Stats != nil {
				lastApi = float64(vz.JetStream.Stats.API.Total)
			}
			memUsed = make([]float64, width)
			cpuUsed = make([]float64, width)
			fileUsed = make([]float64, width)
			haAssets = make([]float64, width)
			apiRates = make([]float64, width)
			pending = make([]float64, width)
			first = false
			return nil, nil
		}

		if vz.JetStream.Stats != nil {
			memUsed = c.resizeData(memUsed, width, float64(vz.JetStream.Stats.Memory)/1024/1024/1024)
			cpuUsed = c.resizeData(cpuUsed, width, vz.CPU/float64(vz.Cores))
			fileUsed = c.resizeData(fileUsed, width, float64(vz.JetStream.Stats.Store)/1024/1024/1024)
			haAssets = c.resizeData(haAssets, width, float64(vz.JetStream.Stats.HAAssets))

			apiRate := (float64(vz.JetStream.Stats.API.Total) - lastApi) / time.Since(lastStateTs).Seconds()
			if apiRate < 0 {
				apiRate = 0
			}
			apiRates = c.resizeData(apiRates, width, apiRate)

			lastApi = float64(vz.JetStream.Stats.API.Total)
		}

		if vz.JetStream.Meta != nil {
			pending = c.resizeData(pending, width, float64(vz.JetStream.Meta.Pending))
		}

		lastStateTs = time.Now()

		cpuPlot := asciigraph.Plot(cpuUsed,
			asciigraph.Caption(fmt.Sprintf("CPU %% Used (normalized for %d cores)", vz.Cores)),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(f))

		memPlot := asciigraph.Plot(memUsed,
			asciigraph.Caption("Memory Storage in GB"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.ValueFormatter(fiBytesFloat2Int))

		filePlot := asciigraph.Plot(fileUsed,
			asciigraph.Caption("File Storage in GB"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.ValueFormatter(fiBytesFloat2Int))

		assetsPlot := asciigraph.Plot(haAssets,
			asciigraph.Caption("HA Assets"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(fFloat2Int))

		apiRatesPlot := asciigraph.Plot(apiRates,
			asciigraph.Caption("API Requests / second"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(f))

		pendingPlot := asciigraph.Plot(pending,
			asciigraph.Caption("Pending API Requests"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(fFloat2Int))

		return []string{cpuPlot, assetsPlot, apiRatesPlot, pendingPlot, filePlot, memPlot}, nil
	})
}

func (c *SrvGraphCmd) graphServer() error {
	var cpuUsed, memUsed, connections, subscriptions []float64
	var messagesRate, bytesRate []float64
	var lastMessages, lastByes float64
	lastStateTs := time.Now()
	first := true

	return c.graphWrapper(6, func(width int, height int, vz *zmonitor.VarzV1) ([]string, error) {
		fmt.Printf("JetStream Statistics for %s\n", c.id)
		fmt.Println()

		if first {
			lastMessages = float64(vz.InMsgs + vz.OutMsgs)
			lastByes = float64(vz.InBytes + vz.OutBytes)
			cpuUsed = make([]float64, width)
			memUsed = make([]float64, width)
			connections = make([]float64, width)
			subscriptions = make([]float64, width)
			messagesRate = make([]float64, width)
			bytesRate = make([]float64, width)
			first = false
			return nil, nil
		}

		cpuUsed = c.resizeData(cpuUsed, width, vz.CPU/float64(vz.Cores))
		memUsed = c.resizeData(memUsed, width, float64(vz.Mem)/1024/1024)
		connections = c.resizeData(connections, width, float64(vz.Connections))
		subscriptions = c.resizeData(subscriptions, width, float64(vz.Subscriptions))

		messagesRate = c.resizeData(messagesRate, width, calculateRate(float64(vz.InMsgs+vz.OutMsgs), lastMessages, time.Since(lastStateTs)))
		bytesRate = c.resizeData(bytesRate, width, calculateRate(float64(vz.InBytes+vz.OutBytes), lastByes, time.Since(lastStateTs)))

		lastMessages = float64(vz.InMsgs + vz.OutMsgs)
		lastByes = float64(vz.InBytes + vz.OutBytes)
		lastStateTs = time.Now()

		cpuPlot := asciigraph.Plot(cpuUsed,
			asciigraph.Caption(fmt.Sprintf("CPU %% Used (normalized for %d cores)", vz.Cores)),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(f))

		memPlot := asciigraph.Plot(memUsed,
			asciigraph.Caption("Memory Used in MB"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.ValueFormatter(fiBytesFloat2Int))

		connectionsPlot := asciigraph.Plot(connections,
			asciigraph.Caption("Connections"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(fFloat2Int))

		subscriptionsPlot := asciigraph.Plot(subscriptions,
			asciigraph.Caption("Subscriptions"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(fFloat2Int))

		messagesPlot := asciigraph.Plot(messagesRate,
			asciigraph.Caption("Messages In+Out / second"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(f))

		bytesPlot := asciigraph.Plot(bytesRate,
			asciigraph.Caption("Bytes In+Out / second"),
			asciigraph.Height(height/6-2),
			asciigraph.Width(width),
			asciigraph.Precision(0),
			asciigraph.ValueFormatter(fiBytesFloat2Int))

		return []string{cpuPlot, memPlot, connectionsPlot, subscriptionsPlot, messagesPlot, bytesPlot}, nil
	})
}

func (c *SrvGraphCmd) getVz(nc *nats.Conn, subj string, body []byte) (*zmonitor.VarzV1, error) {
	resp, err := nc.Request(subj, body, opts().Timeout)
	if err != nil {
		return nil, fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}

	reqresp := map[string]json.RawMessage{}
	err = json.Unmarshal(resp.Data, &reqresp)
	if err != nil {
		return nil, err
	}

	errresp, ok := reqresp["error"]
	if ok {
		return nil, fmt.Errorf("invalid response received: %#v", errresp)
	}

	data, ok := reqresp["data"]
	if !ok {
		return nil, fmt.Errorf("no data received in response: %#v", reqresp)
	}

	varz := &zmonitor.VarzV1{}
	err = json.Unmarshal(data, varz)
	if err != nil {
		return nil, err
	}

	return varz, nil
}

func (c *SrvGraphCmd) resizeData(data []float64, width int, val float64) []float64 {
	data = append(data, val)

	if width <= 0 {
		return data
	}

	length := len(data)

	if length > width {
		return data[length-width:]
	}

	return data
}

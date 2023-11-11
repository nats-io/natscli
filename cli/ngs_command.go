package cli

import (
	"fmt"
	"strings"

	"github.com/choria-io/fisk"
)

type ngsCmd struct {
	rttCmd rttCmd
}

func configureNgsCommand(app commandHost) {
	c := &ngsCmd{
		rttCmd: rttCmd{},
	}

	ngs := app.Command("ngs", "NGS helpers")
	ngs.HelpLong("WARNING: This command is experimental")

	rtt := ngs.Command("rtt", "Displays RTT for all NGS endpoints").Action(c.rttHandler)
	rtt.Arg("iterations", "How many round trips to do when testing").Default("5").IntVar(&c.rttCmd.iterations)
}

func init() {
	registerCommand("ngs", 0, configureNgsCommand)
}

var (
	endpoints = []string{
		"tls://aws.cloud.ngs.global",
		"tls://az.cloud.ngs.global",
		"tls://gcp.cloud.ngs.global",
		"tls://asia.geo.ngs.global",
		"tls://eu.geo.ngs.global",
		"tls://us.geo.ngs.global",
		"tls://west.us.geo.ngs.global",
		"tls://west.us.geo.ngs.global",
	}
)

func (c *ngsCmd) rttHandler(_ *fisk.ParseContext) error {
	servers := strings.Join(endpoints, ",")
	fmt.Println("servers", servers)

	table := newTableWriter("Cloud Provider Endpoints")
	table.AddHeaders("Endpoint", "RTT")

	targets, err := c.rttCmd.targets(servers)
	if err != nil {
		return err
	}

	err = c.rttCmd.performTest(targets)

	if err != nil {
		return err
	}

	for _, t := range targets {
		rtts := ""
		f := fmt.Sprintf("%%%ds: %%v\n", c.rttCmd.calcIndent(targets, 3))
		for _, rtt := range t.Results {
			rtts += fmt.Sprintf(f, rtt.Address, rtt.RTT)
		}
		table.AddRow(t.URL, rtts)
		// if i != len(targets)-1 {
		// 	table.AddSeparator()
		// }
	}

	fmt.Println(table.Render())
	return nil
}

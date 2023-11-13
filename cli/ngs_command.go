package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats.go"
)

type ngsCmd struct {
	rttCmd rttCmd

	cloudAWS bool
	cloudAZ  bool
	cloudGCP bool
	cloudAll bool

	regionalAsia   bool
	regionalEU     bool
	regionalUS     bool
	regionalUSWest bool
	regionalUSEast bool
	regionalAll    bool

	global bool
	all    bool
}

func configureNgsCommand(app commandHost) {
	c := &ngsCmd{
		rttCmd: rttCmd{},
	}

	ngs := app.Command("ngs", "NGS helpers")
	ngs.HelpLong("WARNING: This command is experimental")

	rtt := ngs.Command("rtt", "Displays RTT for all NGS endpoints. See: https://docs.synadia.com/ngs/resources/connection-endpoints").Action(c.rttHandler)
	rtt.Arg("iterations", "How many round trips to do when testing each NGS endpoint").Default("3").IntVar(&c.rttCmd.iterations)
	rtt.Flag("global", fmt.Sprintf("query default (global) endpoint: %s", globalEp)).Short('g').UnNegatableBoolVar(&c.global)
	rtt.Flag("all-clouds", "query all Cloud Endpoints (AWS, AZ, GCP)").Short('c').UnNegatableBoolVar(&c.cloudAll)
	rtt.Flag("aws", fmt.Sprintf("query AWS Cloud Endpoint: %s", cloudAWSEp)).UnNegatableBoolVar(&c.cloudAWS)
	rtt.Flag("az", fmt.Sprintf("query AZ Cloud Endpoint: %s", cloudAZEp)).UnNegatableBoolVar(&c.cloudAZ)
	rtt.Flag("gpc", fmt.Sprintf("query GCP Cloud Endpoint: %s", cloudGCPEp)).UnNegatableBoolVar(&c.cloudGCP)
	rtt.Flag("all-regions", "query all Regional Endpoints (Asia, EU, US)").Short('r').UnNegatableBoolVar(&c.regionalAll)
	rtt.Flag("asia", fmt.Sprintf("query regional endpoint in Asia: %s", regionalAsia)).UnNegatableBoolVar(&c.regionalAsia)
	rtt.Flag("eu", fmt.Sprintf("query regional endpoint in EU: %s", regionalEU)).UnNegatableBoolVar(&c.regionalEU)
	rtt.Flag("us", fmt.Sprintf("query regional endpoint in US: %s", regionalUS)).UnNegatableBoolVar(&c.regionalUS)
	rtt.Flag("us-east", fmt.Sprintf("query regional endpoint in US East: %s", regionalUSEast)).UnNegatableBoolVar(&c.regionalUSEast)
	rtt.Flag("us-west", fmt.Sprintf("query regional endpoint in US West: %s", regionalUSWest)).UnNegatableBoolVar(&c.regionalUSWest)
	rtt.Flag("all", "query all available endpoints").Short('a').UnNegatableBoolVar(&c.all)
}

func init() {
	registerCommand("ngs", 0, configureNgsCommand)
}

const (
	globalEp       = "tls://connect.ngs.global"
	cloudAWSEp     = "tls://aws.cloud.ngs.global"
	cloudAZEp      = "tls://az.cloud.ngs.global"
	cloudGCPEp     = "tls://gcp.cloud.ngs.global"
	regionalAsia   = "tls://asia.geo.ngs.global"
	regionalEU     = "tls://eu.geo.ngs.global"
	regionalUS     = "tls://us.geo.ngs.global"
	regionalUSWest = "tls://west.us.geo.ngs.global"
	regionalUSEast = "tls://east.us.geo.ngs.global"
)

var (
	cloudEp = []string{
		cloudAWSEp,
		cloudAZEp,
		cloudGCPEp,
	}
	reginalEp = []string{
		regionalAsia,
		regionalEU,
		regionalUS,
		regionalUSWest,
		regionalUSEast,
	}
	allEp = []string{
		globalEp,
		cloudAWSEp,
		cloudAZEp,
		cloudGCPEp,
		regionalAsia,
		regionalEU,
		regionalUS,
		regionalUSWest,
		regionalUSEast,
	}
)

func (c *ngsCmd) rttHandler(_ *fisk.ParseContext) error {
	servers := c.resolveServers()

	targets, err := c.rttCmd.targets(servers)
	if err != nil {
		return err
	}

	err = c.rttCmd.performTest(targets)
	if err != nil {
		if errors.Is(err, nats.ErrAuthorization) {
			fmt.Println("NGS server configuration needs to be specified (using context or flags).")
		}
		return err
	}

	table := newTableWriter("NGS Endpoints")
	table.AddHeaders("Endpoint", "RTT")

	for _, t := range targets {
		rtts := ""
		f := fmt.Sprintf("%%%ds: %%v\n", c.rttCmd.calcIndent(targets, 3))
		for _, rtt := range t.Results {
			rtts += fmt.Sprintf(f, rtt.Address, rtt.RTT)
		}
		table.AddRow(t.URL, rtts)
		table.AddSeparator()
	}

	fmt.Println(table.Render())
	return nil
}

func (c *ngsCmd) resolveServers() string {
	fmtRes := func(res []string) string {
		return strings.Join(res, ",")
	}

	if c.all {
		return fmtRes(allEp)
	}

	servers := []string{}
	if c.global {
		servers = append(servers, globalEp)
	}

	if c.cloudAll {
		servers = append(servers, cloudEp...)
	} else {
		if c.cloudAWS {
			servers = append(servers, cloudAWSEp)
		}
		if c.cloudAZ {
			servers = append(servers, cloudAZEp)
		}
		if c.cloudGCP {
			servers = append(servers, cloudGCPEp)
		}
	}

	if c.regionalAll {
		servers = append(servers, reginalEp...)
	} else {
		if c.regionalAsia {
			servers = append(servers, regionalAsia)
		}
		if c.regionalEU {
			servers = append(servers, regionalEU)
		}
		if c.regionalUS {
			servers = append(servers, regionalUS)
		}
		if c.regionalUSEast {
			servers = append(servers, regionalUSEast)
		}
		if c.regionalUSWest {
			servers = append(servers, regionalUSWest)
		}
	}

	if len(servers) == 0 {
		servers = append((servers), globalEp)
	}

	return fmtRes(servers)
}

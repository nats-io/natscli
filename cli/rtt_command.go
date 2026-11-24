// Copyright 2020-2025 The NATS Authors
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
	"net"
	"net/url"
	"strings"
	"time"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/nats-io/nats.go"

	"github.com/choria-io/fisk"
)

type rttCmd struct {
	iterations int
	json       bool
}

type rttResult struct {
	Time    time.Time     `json:"time"`
	Address string        `json:"address"`
	RTT     time.Duration `json:"rtt"`
	URL     string        `json:"url"`
	failed  bool
	dial    string
}

type rttTarget struct {
	URL      string       `json:"url"`
	Results  []*rttResult `json:"results"`
	hostPort string
	scheme   string
}

func configureRTTCommand(app commandHost) {
	c := &rttCmd{}

	rtt := app.Command("rtt", "Compute round-trip time to NATS server").Action(c.rtt)
	rtt.Tag("scope:user", "impact:rw")
	rtt.Arg("iterations", "How many round trips to do when testing").Default("5").IntVar(&c.iterations)
	rtt.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
}

func init() {
	registerCommand("rtt", 13, configureRTTCommand)
}

func (c *rttCmd) rtt(_ *fisk.ParseContext) error {
	targets, err := c.targets()
	if err != nil {
		return err
	}

	err = c.performTest(targets)
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(targets)

		return nil
	}

	format := fmt.Sprintf("%%%ds: %%v\n", c.calcIndent(targets, 3))

	for _, t := range targets {
		fmt.Printf("%s:\n\n", t.URL)

		for _, r := range t.Results {
			if r.failed {
				fmt.Printf(format, r.Address, "failed")
			} else {
				fmt.Printf(format, r.Address, f(r.RTT))
			}
		}

		fmt.Println()
	}

	return nil
}

func (c *rttCmd) calcIndent(targets []*rttTarget, prefix int) int {
	i := prefix

	for _, t := range targets {
		for _, r := range t.Results {
			p := len(r.Address) + prefix

			if p > i {
				i = p
			}
		}
	}

	return i
}

func (c *rttCmd) performTest(targets []*rttTarget) (err error) {
	for _, target := range targets {
		for _, r := range target.Results {
			copts := natsOpts()
			server := r.Address

			if target.hostPort != "" {
				server = fmt.Sprintf("%s://%s", target.scheme, target.hostPort)
				copts = append(copts, c.pinnedDialOption(target.hostPort, r.dial))
			}

			r.Time = time.Now()
			r.URL, r.RTT, err = c.calcRTT(server, copts)
			if err != nil {
				r.failed = true
				log.Printf("Failed to compute rtt for %s: %v", r.Address, err)
				continue
			}

			if r.URL == server {
				r.URL = r.Address
			}
		}
	}

	return nil
}

type rttPinnedDialer struct {
	hostPort string
	dial     string
	timeout  time.Duration
	next     nats.CustomDialer
}

func (d *rttPinnedDialer) Dial(network, address string) (net.Conn, error) {
	if address == d.hostPort {
		address = d.dial
	}

	if d.next != nil {
		return d.next.Dial(network, address)
	}

	return (&net.Dialer{Timeout: d.timeout}).Dial(network, address)
}

// pinnedDialOption connects to hostPort by dialing a single resolved address, so the URL
// keeps the host name for TLS verification while each address is measured separately
func (c *rttCmd) pinnedDialOption(hostPort string, dial string) nats.Option {
	return func(o *nats.Options) error {
		o.SkipHostLookup = true
		o.CustomDialer = &rttPinnedDialer{hostPort: hostPort, dial: dial, timeout: o.Timeout, next: o.CustomDialer}

		return nil
	}
}

func (c *rttCmd) calcRTT(server string, copts []nats.Option) (string, time.Duration, error) {
	opts().Conn = nil

	if opts().Trace {
		log.Printf(">>> Connecting to %s\n", server)
	}

	nc, err := newNatsConn(server, copts...)
	if err != nil {
		return "", 0, err
	}
	defer nc.Close()

	time.Sleep(25 * time.Millisecond)

	var totalTime time.Duration

	if opts().Trace {
		fmt.Printf("RTT iterations for server: %s\n", server)
	}
	for i := 1; i <= c.iterations; i++ {
		rtt, err := nc.RTT()
		if err != nil {
			return "", 0, fmt.Errorf("rtt failed: %v", err)
		}

		totalTime += rtt
		if opts().Trace {
			fmt.Printf("#%d:\trtt=%s\n", i, f(rtt))
			if i == c.iterations {
				fmt.Println()
			}
		}
	}

	return nc.ConnectedUrl(), totalTime / time.Duration(c.iterations), nil
}

func (c *rttCmd) targets() (targets []*rttTarget, err error) {
	servers := ""
	if opts().Conn != nil {
		servers = strings.Join(opts().Conn.DiscoveredServers(), ",")
	} else if opts().Config != nil {
		servers = opts().Config.ServerURL()
	} else {
		return nil, fmt.Errorf("cannot find a server list to test")
	}

	for _, s := range strings.Split(servers, ",") {
		if !strings.Contains(s, "://") {
			s = fmt.Sprintf("nats://%s", s)
		}

		u, err := url.Parse(s)
		if err != nil {
			return targets, err
		}

		port := u.Port()
		if port == "" {
			port = "4222"
		}

		targets = append(targets, &rttTarget{URL: u.String(), Results: []*rttResult{}})
		target := targets[len(targets)-1]

		// its a ip just add it
		if net.ParseIP(u.Hostname()) != nil {
			target.Results = append(target.Results, &rttResult{Address: fmt.Sprintf("%s://%s", u.Scheme, net.JoinHostPort(u.Hostname(), port))})
			continue
		}

		// else look it up and add all its addresses
		addrs, _ := net.LookupHost(u.Hostname())
		if len(addrs) == 0 {
			target.Results = append(target.Results, &rttResult{Address: fmt.Sprintf("%s://%s", u.Scheme, net.JoinHostPort(u.Hostname(), port))})
			continue
		}

		target.hostPort = net.JoinHostPort(u.Hostname(), port)
		target.scheme = u.Scheme

		for _, a := range addrs {
			dial := net.JoinHostPort(a, port)
			target.Results = append(target.Results, &rttResult{Address: fmt.Sprintf("%s://%s", u.Scheme, dial), dial: dial})
		}
	}

	return targets, nil
}

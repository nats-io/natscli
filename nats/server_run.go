package main

import (
	"context"
	"net/url"

	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SrvRunCmd struct {
	port       int
	debug      bool
	verbose    bool
	extendDemo bool
}

func configureServerRunCommand(srv *kingpin.CmdClause) {
	c := &SrvRunCmd{
		port: 4222,
	}

	run := srv.Command("run", "Runs a local development NATS server").Hidden().Action(c.runAction)
	run.Flag("extend-demo", "Extends the NATS demo network").BoolVar(&c.extendDemo)
	run.Flag("port", "Sets the local listening port").Default("4222").IntVar(&c.port)
	run.Flag("verbose", "Log verbosely").BoolVar(&c.verbose)
	run.Flag("debug", "Log in debug mode").BoolVar(&c.debug)
}

func (c *SrvRunCmd) runAction(_ *kingpin.ParseContext) error {
	opts := &server.Options{
		Debug: c.debug,
		Port:  c.port,
		Trace: c.verbose,
	}

	if c.extendDemo {
		demo, err := url.Parse("nats://demo.nats.io:7422")
		if err != nil {
			return err
		}

		opts.LeafNode.Remotes = []*server.RemoteLeafOpts{
			{URLs: []*url.URL{demo}},
		}
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	srv.ConfigureLogger()

	srv.Start()

	<-context.Background().Done()

	return nil
}

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
	"github.com/nats-io/jsm.go/api"
	gatherer "github.com/nats-io/jsm.go/audit/gather"

	"github.com/choria-io/fisk"
)

type auditGatherCmd struct {
	progress bool
	config   *gatherer.Configuration
}

func configureAuditGatherCommand(app *fisk.CmdClause) {
	c := &auditGatherCmd{
		config: gatherer.NewCaptureConfiguration(),
	}

	gather := app.Command("gather", "capture a variety of data from a deployment into an archive file").Alias("capture").Alias("cap").Action(c.gather)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.config.TargetPath)
	gather.Flag("progress", "Display progress messages during gathering").Default("true").BoolVar(&c.progress)
	gather.Flag("server-endpoints", "Capture monitoring endpoints for each server").Default("true").BoolVar(&c.config.Include.ServerEndpoints)
	gather.Flag("server-profiles", "Capture profiles for each server").Default("true").BoolVar(&c.config.Include.ServerProfiles)
	gather.Flag("account-endpoints", "Capture monitoring endpoints for each account").Default("true").BoolVar(&c.config.Include.AccountEndpoints)
	gather.Flag("streams", "Capture state of each stream").Default("true").BoolVar(&c.config.Include.Streams)
	gather.Flag("consumers", "Capture state of each stream consumers").Default("true").BoolVar(&c.config.Include.Consumers)
	gather.Flag("details", "Capture detailed server information from the audit").Default("true").BoolVar(&c.config.Detailed)
}

func (c *auditGatherCmd) gather(_ *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	switch {
	case opts().Trace:
		c.config.LogLevel = api.TraceLevel
	case c.progress:
		c.config.LogLevel = api.InfoLevel
	default:
		c.config.LogLevel = api.ErrorLevel
	}

	c.config.Timeout = opts().Timeout

	return gatherer.Gather(nc, c.config)
}

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
	"fmt"
	"os"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/registry"
	"github.com/nats-io/nats.go"
)

type schemaRegistryCmd struct {
	nc     *nats.Conn
	prefix string
}

func configureSchemaRegistryCommand(schema *fisk.CmdClause) {
	c := &schemaRegistryCmd{}

	reg := schema.Command("registry", "Starts a Schema Registry Service").Hidden().Action(c.registryAction)
	reg.Flag("prefix", "Subject prefix to host the service on").Default("nats.schema.registry").StringVar(&c.prefix)
}

func (c *schemaRegistryCmd) registryAction(_ *fisk.ParseContext) error {
	var err error
	c.nc, _, err = prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	reg, err := registry.New(c.nc, c.prefix, new(SchemaValidator), nil)
	if err != nil {
		return err
	}

	cols := newColumns("NATS CLI Schema Registry waiting for requests on %s", c.nc.ConnectedUrlRedacted())
	cols.AddSectionTitle("Listening Subjects")
	cols.AddRow(fmt.Sprintf("%s.validate", c.prefix), "Schema Validation")
	cols.AddRow(fmt.Sprintf("%s.lookup", c.prefix), "Schema Lookup")
	cols.AddRow(fmt.Sprintf("%s.jetstream", c.prefix), "JetStream Proxy")
	cols.AddSectionTitle("Requests Log")
	cols.Frender(os.Stdout)

	err = reg.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}

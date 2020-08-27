// Copyright 2020 The NATS Authors
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

package main

import (
	"fmt"

	"github.com/ghodss/yaml"
	"github.com/nats-io/jsm.go/api"
	"gopkg.in/alecthomas/kingpin.v2"
)

type schemaShowCmd struct {
	schema string
	yaml   bool
}

func configureSchemaShowCommand(schema *kingpin.CmdClause) {
	c := &schemaShowCmd{}
	show := schema.Command("show", "Show the contents of a schema").Action(c.show)
	show.Arg("schema", "Schema ID to show").Required().StringVar(&c.schema)
	show.Flag("yaml", "Produce YAML format output").BoolVar(&c.yaml)
}

func (c *schemaShowCmd) show(_ *kingpin.ParseContext) error {
	schema, err := api.Schema(c.schema)
	if err != nil {
		return fmt.Errorf("could not load schame %q: %s", c.schema, err)
	}

	if c.yaml {
		schema, err = yaml.JSONToYAML(schema)
		if err != nil {
			return fmt.Errorf("could not reformat schema as YAML: %s", err)
		}
	}

	fmt.Println(string(schema))

	return nil
}

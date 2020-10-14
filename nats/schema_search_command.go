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
	"strings"

	"github.com/nats-io/jsm.go/api"
	"gopkg.in/alecthomas/kingpin.v2"
)

type schemaSearchCmd struct {
	filter string
	json   bool
}

func configureSchemaSearchCommand(schema *kingpin.CmdClause) {
	c := &schemaSearchCmd{}
	search := schema.Command("search", "Search schemas using a pattern").Alias("find").Alias("list").Alias("ls").Action(c.search)
	search.Arg("pattern", "Regular expression to search for").Default(".").StringVar(&c.filter)
	search.Flag("json", "Produce JSON format output").BoolVar(&c.json)
}

func (c *schemaSearchCmd) search(_ *kingpin.ParseContext) error {
	found, err := api.SchemaSearch(c.filter)
	if err != nil {
		return fmt.Errorf("search failed: %s", err)
	}

	if c.json {
		printJSON(found)
		return nil
	}

	if len(found) == 0 {
		fmt.Printf("No schemas matched %q\n", c.filter)
		return nil
	}

	fmt.Printf("Matched Schemas:\n\n  %s\n", strings.Join(found, "\n  "))

	return nil
}

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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

type schemaValidateCmd struct {
	schema string
	file   string
	json   bool
}

func configureSchemaValidateCommand(schema *kingpin.CmdClause) {
	c := &schemaValidateCmd{}
	validate := schema.Command("validate", "Validates a JSON file against a schema").Alias("check").Action(c.validate)
	validate.Arg("schema", "Schema ID to validate against").Required().StringVar(&c.schema)
	validate.Arg("file", "JSON data to validate").Required().StringVar(&c.file)
	validate.Flag("json", "Produce JSON format output").BoolVar(&c.json)

}

func (c *schemaValidateCmd) validate(_ *kingpin.ParseContext) error {
	file, err := ioutil.ReadFile(c.file)
	if err != nil {
		return err
	}

	var data interface{}
	err = json.Unmarshal(file, &data)
	if err != nil {
		return fmt.Errorf("could not parse JSON data in %q: %s", c.file, err)
	}

	ok, errs := new(SchemaValidator).ValidateStruct(data, c.schema)
	if c.json {
		if errs == nil {
			errs = []string{}
		}
		printJSON(errs)
		return nil
	}

	if ok {
		fmt.Printf("%s validates against %s\n", c.file, c.schema)
		return nil
	}

	fmt.Printf("Validation errors in %s:\n\n", c.file)
	fmt.Printf("  %s\n", strings.Join(errs, "\n  "))

	return nil
}

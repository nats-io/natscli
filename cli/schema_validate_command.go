// Copyright 2020-2022 The NATS Authors
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
	"io"
	"os"
	"strings"

	"github.com/choria-io/fisk"
)

type schemaValidateCmd struct {
	schema string
	file   string
	json   bool
}

func configureSchemaValidateCommand(schema *fisk.CmdClause) {
	c := &schemaValidateCmd{}

	validate := schema.Command("validate", "Validates a JSON file against a schema").Alias("check").Action(c.validate)
	validate.Arg("schema", "Schema ID to validate against").Required().StringVar(&c.schema)
	validate.Arg("file", "JSON data to validate (- for stdin)").Required().StringVar(&c.file)
	validate.Flag("json", "Produce JSON format output").UnNegatableBoolVar(&c.json)
}

func (c *schemaValidateCmd) validate(_ *fisk.ParseContext) error {
	var f io.ReadCloser
	var err error

	if c.file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(c.file)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	file, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	var data any
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

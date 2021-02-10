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
	"gopkg.in/alecthomas/kingpin.v2"
)

func configureSchemaCommand(app *kingpin.Application) {
	schema := app.Command("schema", "Schema tools")

	cheats["schemas"] = `# To see all available schemas using regular expressions
nats schema search 'response|request'

# To view a specific schema
nats schema show io.nats.jetstream.api.v1.stream_msg_get_request --yaml

# To validate a JSON input against a specific schema
nats schema validate io.nats.jetstream.api.v1.stream_msg_get_request request.json
`

	configureSchemaSearchCommand(schema)
	configureSchemaShowCommand(schema)
	configureSchemaValidateCommand(schema)
}

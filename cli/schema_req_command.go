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

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/api"
)

type schemaReqCmd struct {
	subject string
	body    string
	schema  string
	dump    bool
}

func configureSchemaReqCommand(schema *fisk.CmdClause) {
	c := &schemaReqCmd{}

	req := schema.Command("request", "Request and validate data from a NATS service").Alias("req").Action(c.requestAction)
	req.Arg("subject", "The subject to send a request to").Required().StringVar(&c.subject)
	req.Arg("body", "The body to send").Default(`{}`).StringVar(&c.body)
	req.Flag("schema", "The schema identifier to validate against").StringVar(&c.schema)
	req.Flag("show", "Show the received data").UnNegatableBoolVar(&c.dump)
}

func (c *schemaReqCmd) requestAction(_ *fisk.ParseContext) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	res, err := nc.Request(c.subject, []byte(c.body), opts.Timeout)
	if err != nil {
		return err
	}

	if c.dump {
		var d any
		err := json.Unmarshal(res.Data, &d)
		if err != nil {
			return err
		}
		j, err := json.MarshalIndent(d, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(j))
		fmt.Println()
	}

	schemaType, msg, err := api.ParseMessage(res.Data)
	if err != nil {
		return err
	}

	if c.schema != "" && schemaType != c.schema {
		return fmt.Errorf("invalid message type %s", schemaType)
	}

	ok, errs := validator().ValidateStruct(msg, schemaType)
	if !ok {
		fmt.Printf("Message did not pass validation against %s\n\n", schemaType)
		for _, err := range errs {
			fmt.Printf("   %s\n", err)
		}

		return nil
	}

	fmt.Printf("Response is a valid %s message\n", schemaType)

	return nil
}

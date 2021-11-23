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

package cli

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/jsm.go/api"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type SchemaValidator struct{}

func (v SchemaValidator) ValidateStruct(data interface{}, schemaType string) (ok bool, errs []string) {
	s, err := api.Schema(schemaType)
	if err != nil {
		return false, []string{fmt.Sprintf("unknown schema type %s", schemaType)}
	}
	sch, err := jsonschema.CompileString("schema.json", string(s))
	if err != nil {
		return false, []string{fmt.Sprintf("could not load schema %s: %s", s, err)}
	}

	// it only accepts basic primitives so we have to specifically convert to interface{}
	var d interface{}
	dj, err := json.Marshal(data)
	if err != nil {
		return false, []string{fmt.Sprintf("could not serialize data: %s", err)}
	}
	err = json.Unmarshal(dj, &d)
	if err != nil {
		return false, []string{fmt.Sprintf("could not de-serialize data: %s", err)}
	}

	err = sch.Validate(d)
	if err != nil {
		if verr, ok := err.(*jsonschema.ValidationError); ok {
			for _, e := range verr.BasicOutput().Errors {
				if e.KeywordLocation == "" || e.Error == "oneOf failed" || e.Error == "allOf failed" {
					continue
				}

				if e.InstanceLocation == "" {
					errs = append(errs, e.Error)
				} else {
					errs = append(errs, fmt.Sprintf("%s: %s", e.InstanceLocation, e.Error))
				}
			}
			return false, errs
		} else {
			return false, []string{fmt.Sprintf("could not validate: %s", err)}
		}
	}

	return true, nil
}

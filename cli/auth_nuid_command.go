// Copyright 2026 The NATS Authors
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

	"github.com/choria-io/fisk"
	"github.com/nats-io/nuid"
)

type authNuidCommand struct{}

func configureAuthNuidCommand(auth commandHost) {
	c := &authNuidCommand{}

	nu := auth.Command("nuid", "Create NUIDs")

	nuGen := nu.Command("gen", "Generates NUIDs").Action(c.genAction)
	nuGen.Tag("impact:ro")
}

func (c *authNuidCommand) genAction(_ *fisk.ParseContext) error {
	fmt.Println(nuid.Next())
	return nil
}

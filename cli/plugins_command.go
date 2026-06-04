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
	"fmt"
	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/plugins"
)

type pluginsCmd struct {
	name    string
	command string
	force   bool
}

func configurePluginCommand(app commandHost) {
	c := &pluginsCmd{}

	cmd := app.Command("plugins", "Manage plugins").Hidden()

	register := cmd.Commandf("register", "Registers a new plugin").Action(c.registerAction)
	register.Arg("name", "The top level name to register the command as").Required().StringVar(&c.name)
	register.Arg("command", "The command the provides the plugins").Required().ExistingFileVar(&c.command)
	register.Flag("force", "Overwrite existing plugins").UnNegatableBoolVar(&c.force)
}

func init() {
	registerCommand("plugins", 18, configurePluginCommand)
}

func (c *pluginsCmd) registerAction(_ *fisk.ParseContext) error {
	fmt.Println("WARNING: Plugins support is experimental and not officially supported")
	fmt.Println()

	err := plugins.Register(c.name, c.command, c.force)
	if err != nil {
		return err
	}

	fmt.Printf("Plugin %s using command %s was added or updated\n", c.name, c.command)

	return nil
}

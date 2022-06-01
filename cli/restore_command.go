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
	"fmt"

	"github.com/alecthomas/kingpin"
)

type restoreCmd struct {
	backupDir    string
	file         string
	updateStream bool
}

func configureRestoreCommand(app commandHost) {
	c := &restoreCmd{}

	restore := app.Command("restore", "Restores a backup of JetStream configuration").Action(c.restoreAction)
	restore.Arg("directory", "Directory to read backup from").StringVar(&c.backupDir)
	restore.Flag("file", "File to read backup from").StringVar(&c.file)
	restore.Flag("update-streams", "Update existing stream configuration").BoolVar(&c.updateStream)
}

func init() {
	registerCommand("restore", 12, configureRestoreCommand)
}

func (c *restoreCmd) restoreAction(_ *kingpin.ParseContext) error {
	fmt.Println("Please note this method of backup does not backup stream data")
	fmt.Println()
	fmt.Println("We now have the ability to backup a single stream data or all streams in")
	fmt.Println("an account, please see the 'nats stream backup' and 'nats account backup'")
	fmt.Println("commands, there are also matching restore commands.")
	fmt.Println()
	fmt.Println("This command is now deprecated and will be removed in September 2022")
	fmt.Println()

	if c.file == "" && c.backupDir == "" {
		return fmt.Errorf("a file or directory is required")
	}

	if c.file != "" && c.backupDir != "" {
		return fmt.Errorf("both file and directory can not be supplied")
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	if c.backupDir != "" {
		return mgr.RestoreJetStreamConfiguration(c.backupDir, c.updateStream)
	}

	return mgr.RestoreJetStreamConfigurationFile(c.file, c.updateStream)
}

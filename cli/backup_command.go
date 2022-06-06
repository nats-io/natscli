// Copyright 2019-2020 The NATS Authors
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
)

type backupCmd struct {
	outDir string
	data   bool
}

func configureBackupCommand(app commandHost) {
	c := &backupCmd{}

	backup := app.Command("backup", "JetStream configuration backup utility").Action(c.backupAction)
	backup.Arg("output", "Directory to write backup to").Required().StringVar(&c.outDir)
	backup.Flag("data", "Include data while performing backups").BoolVar(&c.data)
}

func init() {
	registerCommand("backup", 1, configureBackupCommand)
}

func (c *backupCmd) backupAction(_ *fisk.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	fmt.Println("Please note this method of backup does not backup stream data")
	fmt.Println()
	fmt.Println("We now have the ability to backup a single stream data or all streams in")
	fmt.Println("an account, please see the 'nats stream backup' and 'nats account backup'")
	fmt.Println("commands, there are also matching restore commands.")
	fmt.Println()
	fmt.Println("This command is now deprecated and will be removed in September 2022")
	fmt.Println()

	return mgr.BackupJetStreamConfiguration(c.outDir, c.data)
}

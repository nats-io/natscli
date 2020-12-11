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

package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type backupCmd struct {
	outDir string
	data   bool
}

func configureBackupCommand(app *kingpin.Application) {
	c := &backupCmd{}

	backup := app.Command("backup", "JetStream configuration backup utility").Action(c.backupAction)
	backup.Arg("output", "Directory to write backup to").Required().StringVar(&c.outDir)
	backup.Flag("data", "Include data while performing backups").BoolVar(&c.data)
}

func (c *backupCmd) backupAction(_ *kingpin.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	return mgr.BackupJetStreamConfiguration(c.outDir, c.data)
}

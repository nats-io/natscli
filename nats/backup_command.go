package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go"
)

type backupCmd struct {
	outDir string
}

func configureBackupCommand(app *kingpin.Application) {
	c := &backupCmd{}

	backup := app.Command("backup", "JetStream configuration backup utility").Action(c.backupAction)
	backup.Arg("output", "Directory to write backup to").Required().StringVar(&c.outDir)
}

func (c *backupCmd) backupAction(_ *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	return jsm.BackupJetStreamConfiguration(c.outDir)
}

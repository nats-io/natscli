package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jetstream/internal/jsch"
)

type backupCmd struct {
	outDir string
}

func configureBackupCommand(app *kingpin.Application) {
	c := &backupCmd{}

	backup := app.Command("backup", "Creates a backup of JetStream configuration").Action(c.backupAction)
	backup.Arg("output", "Directory to write backup to").Required().StringVar(&c.outDir)
}

func (c *backupCmd) backupAction(_ *kingpin.ParseContext) error {
	_, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	return jsch.BackupJetStreamConfiguration(c.outDir)
}

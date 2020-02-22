package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jetstream/internal/jsch"
)

type restoreCmd struct {
	backupDir string
	file      string
}

func configureRestoreCommand(app *kingpin.Application) {
	c := &restoreCmd{}

	backup := app.Command("restore", "Restores a backup of JetStream configuration").Action(c.restoreAction)
	backup.Arg("directory", "Directory to read backup from").StringVar(&c.backupDir)
	backup.Flag("file", "File to read backup from").StringVar(&c.file)
}

func (c *restoreCmd) restoreAction(_ *kingpin.ParseContext) error {
	if c.file == "" && c.backupDir == "" {
		return fmt.Errorf("a file or directory is required")
	}

	if c.file != "" && c.backupDir != "" {
		return fmt.Errorf("both file and directory can not be supplied")
	}

	_, err := prepareHelper(servers, natsOpts()...)
	if err != nil {
		return err
	}

	if c.backupDir != "" {
		return jsch.RestoreJetStreamConfiguration(c.backupDir, false)
	}

	return jsch.RestoreJetStreamConfigurationFile(c.file, false)
}

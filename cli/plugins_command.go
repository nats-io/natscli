package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
	"github.com/mprimi/natscli/plugins"
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

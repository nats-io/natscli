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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fatih/color"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go/natscontext"
)

type ctxCommand struct {
	json           bool
	activate       bool
	description    string
	name           string
	nsc            string
	force          bool
	validateErrors int
}

func configureCtxCommand(app *kingpin.Application) {
	c := ctxCommand{}

	context := app.Command("context", "Manage nats configuration contexts").Alias("ctx")

	save := context.Command("save", "Update or create a context").Alias("add").Alias("create").Action(c.createCommand)
	save.Arg("name", "The context name to act on").Required().StringVar(&c.name)
	save.Flag("description", "Set a friendly description for this context").StringVar(&c.description)
	save.Flag("select", "Select the saved context as the default one").BoolVar(&c.activate)
	save.Flag("nsc", "URL to a nsc user, eg. nsc://<operator>/<account>/<user>").StringVar(&c.nsc)

	edit := context.Command("edit", "Edit a context in your EDITOR").Alias("vi").Action(c.editCommand)
	edit.Arg("name", "The context name to edit").Required().StringVar(&c.name)

	context.Command("ls", "List known contexts").Alias("list").Alias("l").Action(c.listCommand)

	rm := context.Command("rm", "Remove a context").Alias("remove").Action(c.removeCommand)
	rm.Arg("name", "The context name to remove").Required().StringVar(&c.name)
	rm.Flag("force", "Force remove without prompting").Short('f').BoolVar(&c.force)

	pick := context.Command("select", "Select the default context").Alias("switch").Alias("set").Action(c.selectCommand)
	pick.Arg("name", "The context name to select").StringVar(&c.name)

	show := context.Command("show", "Show the current or named context").Action(c.showCommand)
	show.Arg("name", "The context name to show").StringVar(&c.name)
	show.Flag("json", "Show the context in JSON format").Short('j').BoolVar(&c.json)
	show.Flag("connect", "Attempts to connect to NATS using the context while validating").BoolVar(&c.activate)

	validate := context.Command("validate", "Validate one or all contexts").Action(c.validateCommand)
	validate.Arg("name", "Validate a specific context, validates all when not supplied").StringVar(&c.name)
	validate.Flag("connect", "Attempts to connect to NATS using the context while validating").BoolVar(&c.activate)

	cheats["contexts"] = `# Create or update
nats context add development --server nats.dev.example.net:4222 [other standard connection properties]
nats context add ngs --description "NGS Connection in Orders Account" --nsc nsc://acme/orders/new
nats context edit development [standard connection properties]

# View contexts
nats context ls
nats context show development --json

# Validate all connections are valid and that connections can be established
nats context validate --connect

# Select a new default context
nats context select

# Connecting using a context
nats pub --context development subject body
`
}

func (c *ctxCommand) hasOverrides() bool {
	return len(c.overrideVars()) != 0
}

func (c *ctxCommand) overrideVars() []string {
	var list []string
	for _, v := range overrideEnvVars {
		if os.Getenv(v) != "" {
			list = append(list, v)
		}
	}

	return list
}
func (c *ctxCommand) validateCommand(pc *kingpin.ParseContext) error {
	var contexts []string
	if c.name == "" {
		contexts = natscontext.KnownContexts()
	} else {
		contexts = append(contexts, c.name)
	}

	for _, name := range contexts {
		c.name = name
		err := c.showCommand(pc)
		if err != nil {
			fmt.Printf("Could not load %s: %s\n\n", name, color.RedString(err.Error()))
		}

		fmt.Println()
	}

	if c.validateErrors > 0 {
		return fmt.Errorf("validation failed")
	}

	return nil
}

func (c *ctxCommand) editCommand(pc *kingpin.ParseContext) error {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		return fmt.Errorf("set EDITOR environment variable to your chosen editor")
	}

	if !natscontext.IsKnown(c.name) {
		return fmt.Errorf("unknown context %q", c.name)
	}

	path, err := natscontext.ContextPath(c.name)
	if err != nil {
		return err
	}

	// we load and save here so that any fields added to the context
	// structure after this context was initially created would also
	// appear in the editor as empty fields
	ctx, err := natscontext.New(c.name, true)
	if err != nil {
		return err
	}
	err = ctx.Save(c.name)
	if err != nil {
		return err
	}

	cmd := exec.Command(editor, path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	return c.showCommand(pc)
}

func (c *ctxCommand) listCommand(_ *kingpin.ParseContext) error {
	known := natscontext.KnownContexts()
	current := natscontext.SelectedContext()
	if len(known) == 0 {
		fmt.Println("No known contexts")
		return nil
	}

	fmt.Printf("Known contexts:\n\n")
	for _, name := range known {
		cfg, _ := natscontext.New(name, true)

		if name == current {
			name = name + "*"
		}

		if cfg != nil && cfg.Description() != "" {
			fmt.Printf("   %-20s%s\n", name, cfg.Description())
		} else {
			fmt.Printf("   %-20s\n", name)
		}
	}

	fmt.Println()

	return nil
}

func (c *ctxCommand) showCommand(_ *kingpin.ParseContext) error {
	if c.name == "" {
		c.name = natscontext.SelectedContext()
	}

	if c.name == "" {
		return fmt.Errorf("no default context and no name supplied")
	}

	cfg, err := natscontext.New(c.name, true)
	if err != nil {
		return err
	}

	if c.json {
		printJSON(cfg)
		return nil
	}

	checkFile := func(file string) string {
		if file == "" {
			return ""
		}

		ok, err := fileAccessible(file)
		if !ok || err != nil {
			c.validateErrors++
			return color.RedString("ERROR")
		}

		return color.GreenString("OK")
	}

	fmt.Printf("NATS Configuration Context %q\n\n", c.name)
	c.showIfNotEmpty("      Description: %s\n", cfg.Description())
	c.showIfNotEmpty("      Server URLs: %s\n", cfg.ServerURL())
	c.showIfNotEmpty("         Username: %s\n", cfg.User())
	c.showIfNotEmpty("         Password: *********\n", cfg.Password())
	c.showIfNotEmpty("            Token: %s\n", cfg.Token())
	c.showIfNotEmpty("      Credentials: %s (%s)\n", cfg.Creds(), checkFile(cfg.Creds()))
	c.showIfNotEmpty("             NKey: %s (%s)\n", cfg.NKey(), checkFile(cfg.NKey()))
	c.showIfNotEmpty("      Certificate: %s (%s)\n", cfg.Certificate(), checkFile(cfg.Certificate()))
	c.showIfNotEmpty("              Key: %s (%s)\n", cfg.Key(), checkFile(cfg.Key()))
	c.showIfNotEmpty("               CA: %s (%s)\n", cfg.CA(), checkFile(cfg.CA()))
	c.showIfNotEmpty("       NSC Lookup: %s\n", cfg.NscURL())
	c.showIfNotEmpty("    JS API Prefix: %s\n", cfg.JSAPIPrefix())
	c.showIfNotEmpty("  JS Event Prefix: %s\n", cfg.JSEventPrefix())
	c.showIfNotEmpty("        JS Domain: %s\n", cfg.JSDomain())
	c.showIfNotEmpty("             Path: %s\n", cfg.Path())

	checkConn := func() error {
		opts, err := cfg.NATSOptions()
		opts = append(opts, nats.MaxReconnects(1))
		if err != nil {
			return err
		}
		nc, err := nats.Connect(cfg.ServerURL(), opts...)
		if err != nil {
			return err
		}
		nc.Close()

		return nil
	}

	if c.activate {
		err = checkConn()
		if err != nil {
			c.validateErrors++
			fmt.Printf("       Connection: %s\n", color.RedString(err.Error()))
		} else {
			fmt.Printf("       Connection: %s\n", color.GreenString("OK"))
		}
	}

	fmt.Println()

	if c.hasOverrides() {
		fmt.Printf("%s: Shell environment overrides in place using %v", color.HiRedString("WARNING"), strings.Join(c.overrideVars(), ", "))
		fmt.Println()
	}

	return nil
}
func (c *ctxCommand) createCommand(pc *kingpin.ParseContext) error {
	lname := ""
	load := false

	switch {
	case natscontext.IsKnown(c.name):
		lname = c.name
		load = true
	case cfgCtx != "":
		lname = cfgCtx
		load = true
	}

	config, err := natscontext.New(lname, load,
		natscontext.WithServerURL(servers),
		natscontext.WithUser(username),
		natscontext.WithPassword(password),
		natscontext.WithToken(username),
		natscontext.WithCreds(creds),
		natscontext.WithNKey(nkey),
		natscontext.WithCertificate(tlsCert),
		natscontext.WithKey(tlsKey),
		natscontext.WithCA(tlsCA),
		natscontext.WithDescription(c.description),
		natscontext.WithNscUrl(c.nsc),
		natscontext.WithJSAPIPrefix(jsApiPrefix),
		natscontext.WithJSEventPrefix(jsEventPrefix),
		natscontext.WithJSDomain(jsDomain),
	)
	if err != nil {
		return err
	}

	err = config.Save(c.name)
	if err != nil {
		return err
	}

	if c.activate {
		return c.selectCommand(pc)
	}

	return c.showCommand(pc)
}

func (c *ctxCommand) removeCommand(_ *kingpin.ParseContext) error {
	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete context %q", c.name), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %s", err)
		}

		if !ok {
			return nil
		}
	}

	return natscontext.DeleteContext(c.name)
}

func (c *ctxCommand) selectCommand(pc *kingpin.ParseContext) error {
	known := natscontext.KnownContexts()

	if len(known) == 0 {
		return fmt.Errorf("no context defined")
	}

	if c.name == "" {
		err := survey.AskOne(&survey.Select{
			Message:  "Select a Context",
			Options:  known,
			PageSize: selectPageSize(len(known)),
		}, &c.name)
		if err != nil {
			return err
		}
	}

	if c.name == "" {
		return fmt.Errorf("please select a context to activate")
	}

	err := natscontext.SelectContext(c.name)
	if err != nil {
		return err
	}

	return c.showCommand(pc)
}

func (c *ctxCommand) showIfNotEmpty(format string, val string, arg ...interface{}) {
	if val == "" {
		return
	}

	if !strings.Contains(format, "%") {
		fmt.Print(format)
		return
	}

	fmt.Printf(format, append([]interface{}{interface{}(val)}, arg...)...)
}

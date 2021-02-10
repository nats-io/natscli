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
	"log"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"
)

type cheatCmd struct {
	sections bool
	section  string
	save     string
}

func configureCheatCommand(app *kingpin.Application) {
	c := &cheatCmd{}
	help := `Cheatsheets for the nats CLI

These cheatsheets are in a format compatible with the popular https://github.com/cheat/cheat
command.
`
	cmd := app.Command("cheat", help).Action(c.cheat)
	cmd.Arg("section", "Restrict help to a specific section").StringVar(&c.section)
	cmd.Flag("sections", "Show section names").BoolVar(&c.sections)
	cmd.Flag("save", "Save cheatsheets to a directory that should not exist").StringVar(&c.save)
	cheats["cheats"] = `# show a specific section of cheats
nats cheat pub

# list available sections
nats cheat --sections

# Save cheats to files in the format expected by 'cheats'
rm -rf .config/cheat/cheatsheets/personal/nats
nats cheat --save .config/cheat/cheatsheets/personal/nats
cheat nats/sub
`
}

func (c *cheatCmd) saveCheats() error {
	_, err := os.Stat(c.save)
	if !os.IsNotExist(err) {
		return fmt.Errorf("target directory %s already exist", c.save)
	}

	err = os.MkdirAll(c.save, 0755)
	if err != nil {
		return err
	}

	for s, cheat := range cheats {
		dest := filepath.Join(c.save, s)
		f, err := os.Create(dest)
		if err != nil {
			return err
		}

		fmt.Fprint(f, "---\ntags: [nats]\n---\n\n")
		fmt.Fprintln(f, cheat)
		f.Close()
		log.Printf("Wrote %s\n", dest)
	}

	return nil
}

func (c *cheatCmd) cheat(_ *kingpin.ParseContext) error {
	if c.save != "" {
		return c.saveCheats()
	}

	switch {
	case c.section != "":
		s, ok := cheats[c.section]
		if !ok {
			fmt.Printf("Unknown section %s\n", c.section)
			fmt.Println()
			c.listSections()
			os.Exit(1)
		}

		fmt.Println(s)

	case c.sections:
		c.listSections()

	default:
		fmt.Println(cheats["cheats"])
		for s, c := range cheats {
			if s == "cheats" {
				continue
			}

			fmt.Println(c)
		}
	}

	return nil
}

func (c *cheatCmd) listSections() {
	fmt.Println("Known sections:")
	fmt.Println()

	var sections []string
	for s := range cheats {
		sections = append(sections, s)
	}
	sort.Strings(sections)

	for _, s := range sections {
		fmt.Println("   " + s)
	}
}

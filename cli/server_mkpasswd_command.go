// Copyright 2020-2025 The NATS Authors
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
	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	iu "github.com/nats-io/natscli/internal/util"
	"golang.org/x/crypto/bcrypt"
)

type SrvPasswdCmd struct {
	pass     string
	cost     uint
	generate bool
	force    bool
}

func configureServerPasswdCommand(srv *fisk.CmdClause) {
	c := &SrvPasswdCmd{}

	passwd := srv.Command("passwd", "Creates encrypted passwords for use in NATS Server").Alias("mkpasswd").Alias("pass").Alias("password").Action(c.mkpasswd)
	passwd.Flag("pass", "The password to encrypt (PASSWORD)").Short('p').Envar("PASSWORD").StringVar(&c.pass)
	passwd.Flag("cost", "The cost to use in the bcrypt argument").Short('c').Default("11").UintVar(&c.cost)
	passwd.Flag("generate", "Generates a secure passphrase and encrypt it").Short('g').UnNegatableBoolVar(&c.generate)
	passwd.Flag("force", "Do not verify the password rule").Short('f').UnNegatableBoolVar(&c.force)
}

func (c *SrvPasswdCmd) mkpasswd(_ *fisk.ParseContext) error {
	if int(c.cost) < bcrypt.MinCost || int(c.cost) > bcrypt.MaxCost {
		return fmt.Errorf("bcrypt cost should be between %d and %d", bcrypt.MinCost, bcrypt.MaxCost)
	}

	var err error

	if c.pass == "" && c.generate {
		c.pass = iu.RandomPassword(22)
		fmt.Printf("Generated password: %s\n", c.pass)
	} else if c.pass == "" && !c.generate {
		c.pass, err = c.askPassword()
		if err != nil {
			return err
		}
	}

	if !c.force && len(c.pass) < 22 {
		return fmt.Errorf("password should be at least 22 characters long")
	}

	cb, err := bcrypt.GenerateFromPassword([]byte(c.pass), int(c.cost))
	if err != nil {
		return fmt.Errorf("error producing bcrypt hash: %w", err)
	}

	if c.generate {
		fmt.Printf("       bcrypt hash: %s\n", string(cb))
	} else {
		fmt.Println(string(cb))
	}

	return nil
}

func (c *SrvPasswdCmd) askPassword() (string, error) {
	bp1 := ""
	bp2 := ""

	err := iu.AskOne(&survey.Password{Message: "Enter password", Help: "Enter a password string that's minimum 22 characters long"}, &bp1)
	if err != nil {
		return "", fmt.Errorf("could not read password: %w", err)
	}
	fmt.Println()
	err = iu.AskOne(&survey.Password{Message: "Re-enter password", Help: "Enter the same password again"}, &bp2)
	if err != nil {
		return "", fmt.Errorf("could not read password: %w", err)
	}

	fmt.Println()

	if bp1 != bp2 {
		return "", fmt.Errorf("entered and re-entered passwords do not match")
	}

	return bp1, nil
}

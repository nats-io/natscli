// Copyright 2023-2024 The NATS Authors
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
	"errors"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	au "github.com/nats-io/natscli/internal/auth"
	iu "github.com/nats-io/natscli/internal/util"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

func configureAuthCommand(app commandHost) {
	auth := app.Command("auth", "NATS Decentralized Authentication")

	// todo:
	//  - Improve maintaining pub/sub permissions for a user, perhaps allow interactive edits of yaml?

	auth.HelpLong("WARNING: This is experimental and subject to massive change, do not use yet")

	configureAuthOperatorCommand(auth)
	configureAuthAccountCommand(auth)
	configureAuthUserCommand(auth)
	configureAuthNkeyCommand(auth)
}

func init() {
	registerCommand("auth", 0, configureAuthCommand)
}

func selectOperator(operatorName string, pick bool, useSelected bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, err := au.GetAuthBuilder()
	if err != nil {
		return nil, nil, err
	}

	operators := auth.Operators().List()

	// if we have the selected operator file we put that as the name if no name were given
	if operatorName == "" && useSelected {
		cfg, err := loadConfig()
		if err == nil {
			operatorName = cfg.SelectedOperator
		}
	}

	if operatorName == "" || !au.IsAuthItemKnown(operators, operatorName) {
		if !pick {
			return nil, nil, fmt.Errorf("unknown operator: %v", operatorName)
		}

		if len(operators) == 1 {
			return auth, operators[0], nil
		}

		if !iu.IsTerminal() {
			return nil, nil, fmt.Errorf("cannot pick an Operator without a terminal and no operator name supplied")
		}

		names := au.SortedAuthNames(auth.Operators().List())
		if len(names) == 0 {
			return nil, nil, fmt.Errorf("no operators found")
		}

		err = askOne(&survey.Select{
			Message:  "Select an Operator",
			Options:  names,
			PageSize: selectPageSize(len(names)),
		}, &operatorName)
		if err != nil {
			return nil, nil, err
		}
	}

	op, err := auth.Operators().Get(operatorName)
	if op == nil || errors.Is(err, ab.ErrNotFound) {
		return nil, nil, fmt.Errorf("unknown operator: %v", operatorName)
	} else if err != nil {
		return nil, nil, err
	}

	return auth, op, nil
}

func selectOperatorAccount(operatorName string, accountName string, pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, operator, err := selectOperator(operatorName, pick, true)
	if err != nil {
		return nil, nil, nil, err
	}

	if accountName == "" || !au.IsAuthItemKnown(operator.Accounts().List(), accountName) {
		if !pick {
			return nil, nil, nil, fmt.Errorf("unknown Account: %v", accountName)
		}

		if !iu.IsTerminal() {
			return nil, nil, nil, fmt.Errorf("cannot pick an Account without a terminal and no Account name supplied")
		}

		names := au.SortedAuthNames(operator.Accounts().List())
		err = askOne(&survey.Select{
			Message:  "Select an Account",
			Options:  names,
			PageSize: selectPageSize(len(names)),
		}, &accountName)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	acct, err := operator.Accounts().Get(accountName)
	if acct == nil || errors.Is(err, ab.ErrNotFound) {
		return nil, nil, nil, fmt.Errorf("unknown Account: %v", accountName)
	} else if err != nil {
		return nil, nil, nil, err
	}

	return auth, operator, acct, nil
}

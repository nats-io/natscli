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
	"github.com/synadia-io/jwt-auth-builder.go/providers/nsc"
	"path/filepath"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/natscli/columns"
	au "github.com/nats-io/natscli/internal/auth"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

func configureAuthCommand(app commandHost) {
	auth := app.Command("auth", "NATS Decentralized Authentication")

	// todo:
	//  - store role name, currently its the pub key not name
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

func getAuthBuilder() (*ab.AuthImpl, error) {
	storeDir, err := nscStore()
	if err != nil {
		return nil, err
	}

	return ab.NewAuth(nsc.NewNscProvider(filepath.Join(storeDir, "stores"), filepath.Join(storeDir, "keys")))
}

func selectOperator(operatorName string, pick bool, useSelected bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, err := getAuthBuilder()
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

		if !isTerminal() {
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

		if !isTerminal() {
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

func renderUserLimits(limits ab.UserLimits, cols *columns.Writer) error {
	cols.AddRowIfNotEmpty("Locale", limits.Locale())
	cols.AddRow("Bearer Token", limits.BearerToken())

	cols.AddSectionTitle("Limits")

	cols.AddRowUnlimited("Max Payload", limits.MaxPayload(), -1)
	cols.AddRowUnlimited("Max Data", limits.MaxData(), -1)
	cols.AddRowUnlimited("Max Subscriptions", limits.MaxSubscriptions(), -1)
	cols.AddRowIfNotEmpty("Connection Types", strings.Join(limits.ConnectionTypes().Types(), ", "))
	cols.AddRowIfNotEmpty("Connection Sources", strings.Join(limits.ConnectionSources().Sources(), ", "))

	ctimes := limits.ConnectionTimes().List()
	if len(ctimes) > 0 {
		ranges := []string{}
		for _, tr := range ctimes {
			ranges = append(ranges, fmt.Sprintf("%s to %s", tr.Start, tr.End))
		}
		cols.AddStringsAsValue("Connection Times", ranges)
	}

	cols.AddSectionTitle("Permissions")
	cols.Indent(2)
	cols.AddSectionTitle("Publish")
	if len(limits.PubPermissions().Allow()) > 0 || len(limits.PubPermissions().Deny()) > 0 {
		if len(limits.PubPermissions().Allow()) > 0 {
			cols.AddStringsAsValue("Allow", limits.PubPermissions().Allow())
		}
		if len(limits.PubPermissions().Deny()) > 0 {
			cols.AddStringsAsValue("Deny", limits.PubPermissions().Deny())
		}
	} else {
		cols.Println("No permissions defined")
	}

	cols.AddSectionTitle("Subscribe")
	if len(limits.SubPermissions().Allow()) > 0 || len(limits.SubPermissions().Deny()) > 0 {
		if len(limits.SubPermissions().Allow()) > 0 {
			cols.AddStringsAsValue("Allow", limits.SubPermissions().Allow())
		}

		if len(limits.SubPermissions().Deny()) > 0 {
			cols.AddStringsAsValue("Deny", limits.SubPermissions().Deny())
		}
	} else {
		cols.Println("No permissions defined")
	}

	cols.Indent(0)

	return nil
}

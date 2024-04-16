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
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

type authUserCommand struct {
	userName        string
	accountName     string
	operatorName    string
	defaults        bool
	signingKey      string
	userLocale      string
	bearerAllowed   bool
	maxPayload      int64
	maxData         int64
	maxSubs         int64
	maxPayloadIsSet bool
	maxSubsIsSet    bool
	pubAllow        []string
	pubDeny         []string
	subDeny         []string
	subAllow        []string
	listNames       bool
	force           bool
	credFile        string
	expire          time.Duration
	revoke          bool
}

func configureAuthUserCommand(auth commandHost) {
	c := &authUserCommand{}

	user := auth.Command("user", "Manage Account Users").Alias("u").Alias("usr").Alias("users")

	addCreateFlags := func(f *fisk.CmdClause, edit bool) {
		f.Flag("locale", "Sets the locale for the user connection").StringVar(&c.userLocale)
		f.Flag("bearer", "Enables the use of bearer tokens").BoolVar(&c.bearerAllowed)
		f.Flag("payload", "Maximum payload size to allow").IsSetByUser(&c.maxPayloadIsSet).Default("1048576").Int64Var(&c.maxPayload)
		f.Flag("subscriptions", "Maximum subscription count to allow").IsSetByUser(&c.maxSubsIsSet).Default("-1").Int64Var(&c.maxSubs)
		f.Flag("pub-allow", "Allow publishing to a subject").StringsVar(&c.pubAllow)
		f.Flag("pub-deny", "Deny publishing to a subject").StringsVar(&c.pubDeny)
		f.Flag("sub-allow", "Allow subscribing to a subject").StringsVar(&c.subAllow)
		f.Flag("sub-deny", "Deny subscribing to a subject").StringsVar(&c.subDeny)
		f.Flag("data", "Maximum message data size to allow").Default("-1").Int64Var(&c.maxData)
	}

	add := user.Command("add", "Adds a new User").Action(c.addAction)
	add.Arg("name", "Unique name for this User").Required().StringVar(&c.userName)
	add.Flag("key", "The public key to use when signing the user").StringVar(&c.signingKey)
	add.Flag("operator", "Operator to add the user to").StringVar(&c.operatorName)
	add.Flag("account", "Account to add the user to").StringVar(&c.accountName)
	addCreateFlags(add, false)
	add.Flag("force", "Overwrite existing files").Short('f').UnNegatableBoolVar(&c.force)
	add.Flag("credential", "Writes credentials to a file").StringVar(&c.credFile)
	add.Flag("defaults", "Accept default values without prompting").UnNegatableBoolVar(&c.defaults)

	info := user.Command("info", "Show User information").Alias("i").Alias("show").Alias("view").Action(c.infoAction)
	info.Arg("name", "Unique name for this User").StringVar(&c.userName)
	info.Flag("operator", "Operator holding the Account").StringVar(&c.operatorName)
	info.Flag("account", "Account to query").StringVar(&c.accountName)

	edit := user.Command("edit", "Edits User settings").Alias("update").Action(c.editAction)
	edit.Arg("name", "Unique name for this User").StringVar(&c.userName)
	edit.Flag("operator", "Operator holding the Account").StringVar(&c.operatorName)
	edit.Flag("account", "Account to query").StringVar(&c.accountName)
	addCreateFlags(edit, true)

	ls := user.Command("ls", "List users").Action(c.lsAction)
	ls.Flag("operator", "Operator holding the Account").StringVar(&c.operatorName)
	ls.Flag("account", "Account to query").StringVar(&c.accountName)
	ls.Flag("names", "Show just the Account names").UnNegatableBoolVar(&c.listNames)

	rm := user.Command("rm", "Removes an user").Action(c.rmAction)
	rm.Arg("name", "Unique name for this User").StringVar(&c.userName)
	rm.Flag("operator", "Operator holding the Account").StringVar(&c.operatorName)
	rm.Flag("account", "Account to query").StringVar(&c.accountName)
	rm.Flag("revoke", "Also revokes the user before deleting it").UnNegatableBoolVar(&c.revoke)
	rm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	cred := user.Command("credential", "Creates a credential file for a user").Alias("cred").Alias("creds").Action(c.credAction)
	cred.Arg("file", "The file to create").Required().StringVar(&c.credFile)
	cred.Arg("name", "Unique name for this User").StringVar(&c.userName)
	cred.Flag("expire", "Duration till expiry").DurationVar(&c.expire)
	cred.Flag("operator", "Operator holding the Account").StringVar(&c.operatorName)
	cred.Flag("account", "Account to query").StringVar(&c.accountName)
	cred.Flag("force", "Overwrite existing files").Short('f').UnNegatableBoolVar(&c.force)
}

func (c *authUserCommand) editAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, _ := acct.Users().Get(c.userName)
	if user == nil {
		return fmt.Errorf("user not found")
	}

	err = c.updateUser(user)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) credAction(_ *fisk.ParseContext) error {
	if !c.force && fileExists(c.credFile) {
		return fmt.Errorf("file %s already exist", c.credFile)
	}

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, _ := acct.Users().Get(c.userName)
	if user == nil {
		return fmt.Errorf("user not found")
	}

	err = c.writeCred(user, c.credFile, c.force)
	if err != nil {
		return err
	}

	fmt.Printf("Wrote credential for %s to %s\n", user.Name(), c.credFile)

	return nil
}

func (c *authUserCommand) rmAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the User %s", c.userName), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	user, err := acct.Users().Get(c.userName)
	if errors.Is(err, ab.ErrNotFound) {
		return fmt.Errorf("user does not exist")
	} else if err != nil {
		return err
	}

	if c.revoke {
		err = acct.Revocations().Add(user.Subject(), time.Now())
		if err != nil {
			return fmt.Errorf("revocation failed: %v", err)
		}
	}

	err = acct.Users().Delete(c.userName)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("Removed user %s\n", c.userName)

	return nil
}
func (c *authUserCommand) lsAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	users := acct.Users().List()
	if len(users) == 0 {
		fmt.Println("No users found")
		return nil
	}

	if c.listNames {
		for _, u := range users {
			fmt.Println(u.Name())
		}
		return nil
	}

	table := newTableWriter(fmt.Sprintf("Users in account %s", acct.Name()))
	table.AddHeaders("Name", "Subject", "Scoped", "Sub Perms", "Pub Perms", "Max Subscriptions")
	for _, user := range users {
		limits := ab.UserLimits(user)
		if user.IsScoped() {
			scope, err := acct.ScopedSigningKeys().GetScope(user.Issuer())
			if errors.Is(err, ab.ErrNotFound) {
				table.AddRow(user.Name(), user.Subject(), user.IsScoped(), "", "", "")
				continue
			} else if err != nil {
				return err
			}
			limits = scope
		}

		var hasPub, hasSub, maxSubs string

		if len(limits.PubPermissions().Deny()) > 0 || len(limits.PubPermissions().Allow()) > 0 {
			hasPub = "✓"
		}

		if len(limits.SubPermissions().Deny()) > 0 || len(limits.SubPermissions().Allow()) > 0 {
			hasSub = "✓"
		}

		maxSubs = strconv.Itoa(int(user.MaxSubscriptions()))
		if user.MaxSubscriptions() == -1 {
			maxSubs = "Unlimited"
		}

		table.AddRow(user.Name(), user.Subject(), user.IsScoped(), hasPub, hasSub, maxSubs)
	}

	fmt.Println(table.Render())

	return nil
}

func (c *authUserCommand) pickUser(acct ab.Account) error {
	users := acct.Users().List()
	if len(users) == 0 {
		return fmt.Errorf("no users found in %s", acct.Name())
	}

	var names []string
	for _, u := range users {
		names = append(names, u.Name())
	}
	sort.Strings(names)

	err := askOne(&survey.Select{
		Message:  "Select a User",
		Options:  names,
		PageSize: selectPageSize(len(names)),
	}, &c.userName)
	if err != nil {
		return err
	}

	return nil
}

func (c *authUserCommand) infoAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.userName == "" {
		err = c.pickUser(acct)
		if err != nil {
			return err
		}
	}

	user, err := acct.Users().Get(c.userName)
	if user == nil || err != nil {
		return fmt.Errorf("user %s not found", c.userName)
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) addAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := selectOperatorAccount(c.operatorName, c.accountName, true)
	if err != nil {
		return err
	}

	if c.signingKey == "" {
		c.signingKey = acct.Subject()
	}

	user, err := acct.Users().Get(c.userName)
	switch {
	case user != nil:
		return fmt.Errorf("user %s already exist", c.userName)
	case errors.Is(err, ab.ErrNotFound):
	case err != nil:
		return err
	}

	user, err = acct.Users().Add(c.userName, c.signingKey)
	if err != nil {
		return err
	}

	if !c.defaults {
		if !c.maxPayloadIsSet {
			c.maxPayload, err = askOneInt("Maximum Payload", "-1", "The maximum message size the user can send")
			if err != nil {
				return err
			}
		}

		if !c.maxSubsIsSet {
			c.maxSubs, err = askOneInt("Maximum Subscriptions", "-1", "The maximum number of subscriptions the user can make")
			if err != nil {
				return err
			}
		}
	}

	err = c.updateUser(user)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	if c.credFile != "" {
		err = c.writeCred(user, c.credFile, c.force)
		if err != nil {
			return err
		}
	}

	user, err = acct.Users().Get(c.userName)
	if user == nil || err != nil {
		return fmt.Errorf("user not found")
	}

	return c.fShowUser(os.Stdout, user, acct)
}

func (c *authUserCommand) updateUser(user ab.User) error {
	if user.IsScoped() {
		return nil
	}

	limits := user.(*ab.UserData).UserPermissionLimits()
	limits.Locale = c.userLocale
	limits.BearerToken = c.bearerAllowed
	limits.Payload = c.maxPayload
	limits.Data = c.maxData
	limits.Subs = c.maxSubs

	// TODO: should allow adding/removing not just setting
	if len(c.pubAllow) > 0 {
		limits.Pub.Allow = c.pubAllow
	}
	if len(c.pubDeny) > 0 {
		limits.Pub.Deny = c.pubDeny
	}
	if len(c.subAllow) > 0 {
		limits.Sub.Allow = c.subAllow
	}
	if len(c.subDeny) > 0 {
		limits.Sub.Deny = c.subDeny
	}

	return user.(*ab.UserData).SetUserPermissionLimits(limits)
}

func (c *authUserCommand) fShowUser(w io.Writer, user ab.User, acct ab.Account) error {
	out, err := c.showUser(user, acct)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authUserCommand) showUser(user ab.User, acct ab.Account) (string, error) {
	cols := newColumns("User %s (%s)", user.Name(), user.Subject())
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Account", fmt.Sprintf("%s (%s)", acct.Name(), user.IssuerAccount()))
	cols.AddRow("Issuer", user.Issuer())
	cols.AddRow("Scoped", user.IsScoped())

	limits := ab.UserLimits(user)

	if user.IsScoped() {
		scope, err := acct.ScopedSigningKeys().GetScope(user.Issuer())
		if err != nil {
			return "", fmt.Errorf("could not find signing scope %s", user.Issuer())
		}
		limits = scope
	}

	err := renderUserLimits(limits, cols)
	if err != nil {
		return "", err
	}

	return cols.Render()
}

func (c *authUserCommand) selectAccount(pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, oper, acct, err := selectOperatorAccount(c.operatorName, c.accountName, pick)
	if err != nil {
		return nil, nil, nil, err
	}

	c.operatorName = oper.Name()
	c.accountName = acct.Name()

	return auth, oper, acct, nil
}

func (c *authUserCommand) writeCred(user ab.User, credFile string, force bool) error {
	if !force && fileExists(credFile) {
		return fmt.Errorf("file %s already exist", credFile)
	}

	cred, err := user.Creds(c.expire)
	if err != nil {
		return err
	}

	return os.WriteFile(c.credFile, cred, 0700)
}

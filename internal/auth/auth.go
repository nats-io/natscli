// Copyright 2024 The NATS Authors
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

package auth

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	ab "github.com/synadia-io/jwt-auth-builder.go"
	"github.com/synadia-io/jwt-auth-builder.go/providers/nsc"
)

type listWithNames interface {
	Name() string
}

type OperatorLimitsManager interface {
	OperatorLimits() jwt.OperatorLimits
	SetOperatorLimits(limits jwt.OperatorLimits) error
}

type UserLimitsManager interface {
	UserPermissionLimits() jwt.UserPermissionLimits
	SetUserPermissionLimits(limits jwt.UserPermissionLimits) error
}

func GetAuthBuilder() (*ab.AuthImpl, error) {
	storeDir, err := NscStore()
	if err != nil {
		return nil, err
	}

	return ab.NewAuth(nsc.NewNscProvider(filepath.Join(storeDir, "stores"), filepath.Join(storeDir, "keys")))
}

func NscStore() (string, error) {
	parent, err := iu.XdgShareHome()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(parent, "nats", "nsc")
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return "", err
	}

	return dir, nil
}

func UpdateTags(tags ab.Tags, add []string, rm []string) error {
	if len(add) > 0 {
		err := tags.Add(add...)
		if err != nil {
			return err
		}
	}
	if len(rm) > 0 {
		for _, tag := range rm {
			_, err := tags.Remove(tag)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func SortedAuthNames[list listWithNames](items []list) []string {
	var res []string
	for _, i := range items {
		res = append(res, i.Name())
	}

	sort.Strings(res)
	return res
}

func IsAuthItemKnown[list listWithNames](items []list, name string) bool {
	for _, op := range items {
		if op.Name() == name {
			return true
		}
	}

	return false
}

func SelectOperator(operatorName string, pick bool, useSelected bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, err := GetAuthBuilder()
	if err != nil {
		return nil, nil, err
	}

	operators := auth.Operators().List()

	// if we have the selected operator file we put that as the name if no name were given
	if operatorName == "" && useSelected {
		cfg, err := iu.LoadConfig()
		if err == nil {
			operatorName = cfg.SelectedOperator
		}
	}

	if operatorName == "" || !IsAuthItemKnown(operators, operatorName) {
		if !pick {
			return nil, nil, fmt.Errorf("unknown operator: %v", operatorName)
		}

		if len(operators) == 1 {
			return auth, operators[0], nil
		}

		if !iu.IsTerminal() {
			return nil, nil, fmt.Errorf("cannot pick an Operator without a terminal and no operator name supplied")
		}

		names := SortedAuthNames(auth.Operators().List())
		if len(names) == 0 {
			return nil, nil, fmt.Errorf("no operators found")
		}

		err = iu.AskOne(&survey.Select{
			Message:  "Select an Operator",
			Options:  names,
			PageSize: iu.SelectPageSize(len(names)),
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

func SelectOperatorAccount(operatorName string, accountName string, pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, operator, err := SelectOperator(operatorName, pick, true)
	if err != nil {
		return nil, nil, nil, err
	}

	if accountName == "" || !IsAuthItemKnown(operator.Accounts().List(), accountName) {
		if !pick {
			return nil, nil, nil, fmt.Errorf("unknown Account: %v", accountName)
		}

		if !iu.IsTerminal() {
			return nil, nil, nil, fmt.Errorf("cannot pick an Account without a terminal and no Account name supplied")
		}

		names := SortedAuthNames(operator.Accounts().List())
		if len(names) == 1 {
			accountName = names[0]
		} else {
			err = iu.AskOne(&survey.Select{
				Message:  "Select an Account",
				Options:  names,
				PageSize: iu.SelectPageSize(len(names)),
			}, &accountName)
			if err != nil {
				return nil, nil, nil, err
			}
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

func SelectAccount(op ab.Operator, choice string, prompt string) (ab.Account, error) {
	accts := op.Accounts().List()
	if len(accts) == 0 {
		return nil, fmt.Errorf("no accounts found")
	}

	sort.SliceStable(accts, func(i, j int) bool {
		return accts[i].Name() < accts[j].Name()
	})

	if choice != "" {
		// look on name
		acct, _ := op.Accounts().Get(choice)
		if acct != nil {
			return acct, nil
		}

		// look on subject
		for _, acct := range accts {
			if acct.Subject() == choice {
				return acct, nil
			}
		}
	}

	var subjects []string
	for _, acct := range accts {
		subjects = append(subjects, acct.Subject())
	}

	// not found now make lists
	answ := 0

	if prompt == "" {
		prompt = "Select an Account"
	}

	err := survey.AskOne(&survey.Select{
		Message: prompt,
		Options: subjects,
		Description: func(value string, index int) string {
			return accts[index].Name()
		},
	}, &answ)
	if err != nil {
		return nil, err
	}

	return accts[answ], nil
}

func SelectSigningKey(acct ab.Account, choice string, scopedOnly bool) (ab.ScopeLimits, string, error) {
	if choice != "" {
		// choice is a role and we have just one key for that role
		scopes, _ := acct.ScopedSigningKeys().GetScopeByRole(choice)
		if len(scopes) == 1 {
			return scopes[0], scopes[0].Key(), nil
		}

		// its a public key so we try that
		scope, _ := acct.ScopedSigningKeys().GetScope(choice)
		if scope != nil {
			return scope, choice, nil
		}
	}

	sks := acct.ScopedSigningKeys().List()
	if len(sks) == 0 {
		return nil, "", fmt.Errorf("no signing keys found")
	}

	type k struct {
		scope       ab.ScopeLimits
		key         string
		description string
	}

	var choices []k
	var scopes []string

	for _, sk := range sks {
		scope, _ := acct.ScopedSigningKeys().GetScope(sk)

		if scope == nil && scopedOnly {
			continue
		}

		var description string
		key := sk

		if scope != nil {
			// if they gave us a key and we have it just use that
			if scope.Key() == choice {
				return scope, scope.Key(), nil
			}

			key = scope.Key()

			if scope.Description() == "" {
				description = fmt.Sprintf("Scope %s", scope.Role())
			} else {
				description = fmt.Sprintf("Scope %s (%s)", scope.Role(), scope.Description())
			}
		}

		scopes = append(scopes, sk)
		choices = append(choices, k{
			scope:       scope,
			key:         key,
			description: description,
		})
	}

	answ := 0

	err := survey.AskOne(&survey.Select{
		Message: "Select Signing Key",
		Options: scopes,
		Description: func(value string, index int) string {
			return choices[index].description
		},
	}, &answ)
	if err != nil {
		return nil, "", err
	}

	return choices[answ].scope, choices[answ].key, nil
}

func RenderUserLimits(limits ab.UserLimits, cols *columns.Writer) error {
	cols.AddRowIfNotEmpty("Locale", limits.Locale())
	cols.AddRow("Bearer Token", limits.BearerToken())

	cols.AddSectionTitle("Limits")

	cols.AddRowUnlimited("Max Payload", limits.MaxPayload(), -1)
	cols.AddRowUnlimited("Max Data", limits.MaxData(), -1)
	cols.AddRowUnlimited("Max Subscriptions", limits.MaxSubscriptions(), -1)
	cols.AddRowIfNotEmpty("Connection Types", strings.Join(limits.ConnectionTypes().Types(), ", "))
	cols.AddRowIfNotEmpty("Connection Sources", strings.Join(limits.ConnectionSources().Sources(), ", "))

	ranges := []string{}
	for _, tr := range limits.ConnectionTimes().List() {
		ranges = append(ranges, fmt.Sprintf("%s to %s", tr.Start, tr.End))
	}
	cols.AddStringsAsValue("Connection Times", ranges)

	cols.AddSectionTitle("Permissions")
	cols.Indent(2)
	RenderPermission(cols, "Publish", limits.PubPermissions().Allow(), limits.PubPermissions().Deny())
	RenderPermission(cols, "Subscribe", limits.SubPermissions().Allow(), limits.SubPermissions().Deny())
	cols.Indent(0)

	return nil
}

func RenderPermission(cols *columns.Writer, title string, allow []string, deny []string) {
	cols.AddSectionTitle("%s", title)
	if len(allow) == 0 && len(deny) == 0 {
		cols.Println("No permissions defined")
		return
	}

	cols.AddStringsAsValue("Allow", allow)
	cols.AddStringsAsValue("Deny", deny)
}

// RenderOperator renders an operator, claims is set for an operator decoded from a JWT. Such an
// operator has no accounts loaded, so the account count is left out and the system account is
// shown by its key.
func RenderOperator(operator ab.Operator, claims *jwt.OperatorClaims, cols *columns.Writer) error {
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", operator.Name())
	cols.AddRow("Subject", operator.Subject())
	if tags, _ := operator.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}
	cols.AddRowIf("Service URL(s)", operator.OperatorServiceURLs(), len(operator.OperatorServiceURLs()) > 0)
	cols.AddRowIfNotEmpty("Account Server", operator.AccountServerURL())

	switch {
	case claims != nil && claims.SystemAccount != "":
		cols.AddRow("System Account", claims.SystemAccount)
	case claims != nil:
		cols.AddRow("System Account", "not set")
	default:
		cols.AddRow("Accounts", len(operator.Accounts().List()))
		sa, err := operator.SystemAccount()
		if err == nil && sa != nil {
			cols.AddRowf("System Account", "%s (%s)", sa.Name(), sa.Subject())
		} else {
			cols.AddRow("System Account", "not set")
		}
	}

	cols.AddStringsAsValue("Signing Keys", operator.SigningKeys().List())

	return nil
}

// RenderAccount renders an account, operator is nil when the account is not in the store. claims is
// set for an account decoded from a JWT, which does not carry its users, so the user count is left out.
func RenderAccount(acct ab.Account, operator ab.Operator, claims *jwt.AccountClaims, cols *columns.Writer) error {
	limits := acct.Limits()
	js := limits.JetStream()
	serviceExports := len(acct.Exports().Services().List())
	streamExports := len(acct.Exports().Streams().List())
	serviceImports := len(acct.Imports().Services().List())
	streamImports := len(acct.Imports().Streams().List())

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", acct.Name())
	cols.AddRow("Issuer", acct.Issuer())
	if operator != nil {
		cols.AddRow("Operator", operator.Name())
		sa, err := operator.SystemAccount()
		cols.AddRow("System Account", err == nil && sa != nil && sa.Subject() == acct.Subject())
	}
	if tags, _ := acct.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}
	cols.AddRow("JetStream", js.IsJetStreamEnabled())
	cols.AddRowIf("Expiry", time.Unix(acct.Expiry(), 0), acct.Expiry() > 0)
	if claims == nil {
		cols.AddRow("Users", len(acct.Users().List()))
	}
	cols.AddRow("Revocations", len(acct.Revocations().List()))
	cols.AddRow("Service Exports", serviceExports)
	cols.AddRow("Stream Exports", streamExports)
	cols.AddRow("Service Imports", serviceImports)
	cols.AddRow("Stream Imports", streamImports)

	cols.AddSectionTitle("Limits")
	cols.AddRow("Bearer Tokens Allowed", !limits.DisallowBearerTokens())
	cols.AddRowUnlimited("Subscriptions", limits.MaxSubscriptions(), -1)
	cols.AddRowUnlimited("Connections", limits.MaxConnections(), -1)
	cols.AddRowUnlimitedIf("Maximum Payload", humanize.IBytes(uint64(limits.MaxPayload())), limits.MaxPayload() <= 0)
	if limits.MaxData() > 0 {
		cols.AddRow("Data", limits.MaxData()) // only showing when set as afaik its a ngs thing
	}
	cols.AddRowUnlimited("Leafnodes", limits.MaxLeafNodeConnections(), -1)
	cols.AddRowUnlimited("Imports", limits.MaxImports(), -1)
	cols.AddRowUnlimited("Exports", limits.MaxExports(), -1)

	if js.IsJetStreamEnabled() {
		cols.Indent(2)
		cols.AddSectionTitle("JetStream Settings")

		traffic := acct.ClusterTraffic()
		if traffic == "" {
			traffic = "system"
		}
		cols.AddRow("Cluster Traffic", traffic)

		cols.Indent(4)
		for _, tc := range validTiers(acct) {
			tier, _ := js.Get(tc)
			if tier == nil {
				continue
			}

			if tc == 0 {
				cols.AddSectionTitle("Account Default Limits")
			} else {
				cols.AddSectionTitle("Tier %d", tc)
			}

			if unlimited, _ := tier.IsUnlimited(); unlimited {
				cols.Indent(6)
				cols.Println("Unlimited")
				cols.Indent(4)
				continue
			}

			maxAck, _ := tier.MaxAckPending()
			maxMem, _ := tier.MaxMemoryStorage()
			maxMemStream, _ := tier.MaxMemoryStreamSize()
			maxConns, _ := tier.MaxConsumers()
			maxDisk, _ := tier.MaxDiskStorage()
			maxDiskStream, _ := tier.MaxDiskStreamSize()
			streams, _ := tier.MaxStreams()
			streamSizeRequired, _ := tier.MaxStreamSizeRequired()

			cols.AddRowUnlimitedIf("Max Ack Pending", maxAck, maxAck <= 0)
			cols.AddRowUnlimited("Maximum Streams", streams, -1)
			cols.AddRowUnlimited("Max Consumers Per Stream", maxConns, -1)
			cols.AddRow("Max Stream Size Required", streamSizeRequired)
			cols.AddRowUnlimitedIf("Max File Storage", humanize.IBytes(uint64(maxDisk)), maxDisk < 0)
			cols.AddRowIf("Max File Storage Stream Size", humanize.IBytes(uint64(maxDiskStream)), maxDiskStream > 0)
			cols.AddRowUnlimitedIf("Max Memory Storage", humanize.IBytes(uint64(maxMem)), maxMem < 0)
			cols.AddRowIf("Max Memory Storage Stream Size", humanize.IBytes(uint64(maxMemStream)), maxMemStream > 0)
		}

		cols.Indent(0)
	}

	return nil
}

func validTiers(acct ab.Account) []int8 {
	tiers := []int8{}
	for i := int8(0); i <= 5; i++ {
		tier, _ := acct.Limits().JetStream().Get(i)
		if tier != nil {
			tiers = append(tiers, i)
		}
	}

	if len(tiers) > 1 {
		tiers = tiers[1:]
	}

	return tiers
}

// RenderUser renders a user, acct is nil for a user decoded from a JWT. Without the account the
// signing key scope is unknown, a user issued by a signing key with empty permissions is scoped
// as the server rejects scoped users that carry permissions of their own.
func RenderUser(user ab.User, acct ab.Account, cols *columns.Writer) error {
	var scoped bool
	if acct != nil {
		scoped = user.IsScoped()
	} else if ud, ok := user.(*ab.UserData); ok {
		scoped = user.Issuer() != user.IssuerAccount() && ud.Claim.HasEmptyPermissions()
	}

	cols.AddSectionTitle("Configuration")
	if acct != nil {
		cols.AddRow("Account", fmt.Sprintf("%s (%s)", acct.Name(), user.IssuerAccount()))
	} else {
		cols.AddRow("Account", user.IssuerAccount())
	}
	cols.AddRow("Issuer", user.Issuer())
	cols.AddRow("Scoped", scoped)
	if tags, _ := user.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}

	if scoped && acct == nil {
		cols.AddSectionTitle("Limits")
		cols.Println("Set by the signing key scope in the account JWT")
		return nil
	}

	limits := ab.UserLimits(user)
	if scoped {
		scope, err := acct.ScopedSigningKeys().GetScope(user.Issuer())
		if err != nil {
			return fmt.Errorf("could not find signing scope %s", user.Issuer())
		}
		limits = scope
	}

	return RenderUserLimits(limits, cols)
}

package auth

import (
	_ "embed"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	ab "github.com/synadia-io/jwt-auth-builder.go"
	"github.com/synadia-io/jwt-auth-builder.go/providers/nsc"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed resolver_form.yaml
var ResolverForm []byte

//go:embed resolver_template.txt
var ResolverTemplate string

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

func SelectSigningKey(acct ab.Account, choice string) (ab.ScopeLimits, error) {
	if choice != "" {
		// choice is a role and we have just one key for that role
		scopes, _ := acct.ScopedSigningKeys().GetScopeByRole(choice)
		if len(scopes) == 1 {
			return scopes[0], nil
		}

		// its a public key so we try that
		scope, _ := acct.ScopedSigningKeys().GetScope(choice)
		if scope != nil {
			return scope, nil
		}
	}

	sks := acct.ScopedSigningKeys().List()
	if len(sks) == 0 {
		return nil, fmt.Errorf("no signing keys found")
	}

	type k struct {
		scope       ab.ScopeLimits
		description string
	}
	var choices []k

	for _, sk := range sks {
		scope, _ := acct.ScopedSigningKeys().GetScope(sk)

		// if they gave us a key and we have it just use that
		if scope.Key() == choice {
			return scope, nil
		}

		var description string
		if scope.Description() == "" {
			description = scope.Role()
		} else {
			description = fmt.Sprintf("%s %s", scope.Role(), scope.Description())
		}

		choices = append(choices, k{
			scope:       scope,
			description: description,
		})
	}

	answ := 0

	err := survey.AskOne(&survey.Select{
		Message: "Select Signing Key",
		Options: sks,
		Description: func(value string, index int) string {
			return choices[index].description
		},
	}, &answ)
	if err != nil {
		return nil, err
	}

	return choices[answ].scope, nil
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

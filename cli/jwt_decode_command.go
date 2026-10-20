// Copyright 2026 The NATS Authors
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
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
)

type jwtDecodeCmd struct {
	jwt  string
	json bool
}

func configureJwtDecodeCommand(jwtCmd commandHost) {
	c := &jwtDecodeCmd{}

	decode := jwtCmd.Command("decode", "Decode JWT claims").Alias("de").Alias("d").Action(c.decodeAction)
	decode.Tag("impact:ro")
	decode.Arg("jwt", "JWT string or file holding a JWT").Required().StringVar(&c.jwt)
	decode.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
}

func (c *jwtDecodeCmd) decodeAction(_ *fisk.ParseContext) error {
	token, err := c.loadToken()
	if err != nil {
		return err
	}

	claims, err := jwt.Decode(token)
	if err != nil {
		return err
	}

	return c.fShowClaims(os.Stdout, claims)
}

func (c *jwtDecodeCmd) loadToken() (string, error) {
	input := strings.TrimSpace(c.jwt)

	_, err := os.Stat(input)
	if err != nil {
		switch {
		case c.looksLikeJWT(input):
			return input, nil
		case errors.Is(err, os.ErrNotExist):
			return "", fmt.Errorf("%q is neither a file nor a JWT", input)
		default:
			return "", err
		}
	}

	contents, err := os.ReadFile(input)
	if err != nil {
		return "", err
	}

	token, err := jwt.ParseDecoratedJWT(contents)
	if err != nil {
		return "", err
	}

	token = strings.TrimSpace(token)
	if !c.looksLikeJWT(token) {
		return "", fmt.Errorf("%s does not contain a JWT", input)
	}

	return token, nil
}

// a JWT header is base64 encoded JSON, so it always starts with eyJ, the encoding of {"
func (c *jwtDecodeCmd) looksLikeJWT(s string) bool {
	return strings.HasPrefix(s, "eyJ") && strings.Count(s, ".") == 2
}

func (c *jwtDecodeCmd) fShowClaims(w io.Writer, claims jwt.Claims) error {
	out, err := c.showClaims(claims)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *jwtDecodeCmd) showClaims(claims jwt.Claims) (string, error) {
	if c.json {
		return iu.ToJSON(claims)
	}

	var cols *columns.Writer
	switch claims := claims.(type) {
	case *jwt.UserClaims:
		cols = c.showUser(claims)
	case *jwt.AccountClaims:
		cols = c.showAccount(claims)
	case *jwt.OperatorClaims:
		cols = c.showOperator(claims)
	case *jwt.ActivationClaims:
		cols = c.showActivation(claims)
	case *jwt.AuthorizationRequestClaims:
		cols = c.showAuthRequest(claims)
	case *jwt.AuthorizationResponseClaims:
		cols = c.showAuthResponse(claims)
	default:
		return "", fmt.Errorf("unsupported JWT claim type %q", claims.ClaimType())
	}

	var vr jwt.ValidationResults
	claims.Validate(&vr)
	if len(vr.Issues) > 0 {
		cols.AddSectionTitle("Validation Issues")
		for _, issue := range vr.Issues {
			cols.Println(issue.Description)
		}
	}

	return cols.Render()
}

func (c *jwtDecodeCmd) addClaimsData(cols *columns.Writer, d *jwt.ClaimsData) {
	cols.AddRow("Id", d.ID)
	cols.AddRowIfNotEmpty("Audience", d.Audience)
	cols.AddRow("Issuer", d.Issuer)
	cols.AddRow("Subject", d.Subject)
	cols.AddRow("Issued At", time.Unix(d.IssuedAt, 0))
	if d.Expires > 0 {
		cols.AddRow("Expires", time.Unix(d.Expires, 0))
	} else {
		cols.AddRow("Expires", "never")
	}
	cols.AddRowIf("Not Before", time.Unix(d.NotBefore, 0), d.NotBefore > 0)
}

func (c *jwtDecodeCmd) addPermission(cols *columns.Writer, p jwt.Permission) {
	if len(p.Allow) == 0 && len(p.Deny) == 0 {
		cols.Println("No permissions defined")
		return
	}
	if len(p.Allow) > 0 {
		cols.AddStringsAsValue("Allow", p.Allow)
	}
	if len(p.Deny) > 0 {
		cols.AddStringsAsValue("Deny", p.Deny)
	}
}

func (c *jwtDecodeCmd) showUser(uc *jwt.UserClaims) *columns.Writer {
	cols := newColumnsf("User %s (%s)", uc.Name, uc.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, uc.Claims())
	cols.AddRowIfNotEmpty("Locale", uc.Locale)
	cols.AddRow("Bearer Token", uc.BearerToken)

	cols.AddSectionTitle("Limits")

	cols.AddRowUnlimited("Max Payload", uc.NatsLimits.Payload, -1)
	cols.AddRowUnlimited("Max Data", uc.Data, -1)
	cols.AddRowUnlimited("Max Subscriptions", uc.NatsLimits.Subs, -1)
	cols.AddRowIfNotEmpty("Connection Types", strings.Join(uc.AllowedConnectionTypes, ","))
	ctimes := uc.Times
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
	c.addPermission(cols, uc.Pub)
	cols.AddSectionTitle("Subscribe")
	c.addPermission(cols, uc.Sub)
	cols.Indent(0)

	return cols
}

func (c *jwtDecodeCmd) showOperator(oc *jwt.OperatorClaims) *columns.Writer {
	cols := newColumnsf("Operator %s (%s)", oc.Name, oc.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, oc.Claims())
	cols.AddRowIfNotEmpty("System Account", oc.SystemAccount)
	cols.AddRowIfNotEmpty("Account Server URL", oc.AccountServerURL)
	cols.AddRowIfNotEmpty("Assert Server Version", oc.AssertServerVersion)
	cols.AddRow("Strict Signing Key Usage", oc.StrictSigningKeyUsage)
	if len(oc.OperatorServiceURLs) > 0 {
		cols.AddStringsAsValue("Operator Service URLs", oc.OperatorServiceURLs)
	}
	if len(oc.SigningKeys) > 0 {
		cols.AddStringsAsValue("Signing Keys", oc.SigningKeys)
	}
	if len(oc.Tags) > 0 {
		cols.AddRow("Tags", strings.Join(oc.Tags, ", "))
	}

	return cols
}

func (c *jwtDecodeCmd) showAccount(ac *jwt.AccountClaims) *columns.Writer {
	cols := newColumnsf("Account %s (%s)", ac.Name, ac.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, ac.Claims())
	cols.AddRowIfNotEmpty("Description", ac.Description)
	cols.AddRowIfNotEmpty("Info URL", ac.InfoURL)
	if len(ac.Tags) > 0 {
		cols.AddRow("Tags", strings.Join(ac.Tags, ", "))
	}

	cols.AddSectionTitle("Limits")
	cols.AddRowUnlimited("Max Connections", ac.Limits.Conn, -1)
	cols.AddRowUnlimited("Max Leafnodes", ac.Limits.LeafNodeConn, -1)
	cols.AddRowUnlimited("Max Subscriptions", ac.Limits.Subs, -1)
	cols.AddRowUnlimited("Max Payload", ac.Limits.Payload, -1)
	cols.AddRowUnlimited("Max Data", ac.Limits.Data, -1)
	cols.AddRowUnlimited("Max Imports", ac.Limits.Imports, -1)
	cols.AddRowUnlimited("Max Exports", ac.Limits.Exports, -1)
	cols.AddRow("Wildcard Exports", ac.Limits.WildcardExports)
	cols.AddRow("Disallow Bearer", ac.Limits.DisallowBearer)

	cols.AddSectionTitle("JetStream Limits")
	cols.AddRowUnlimited("Max Memory Storage", ac.Limits.MemoryStorage, -1)
	cols.AddRowUnlimited("Max Disk Storage", ac.Limits.DiskStorage, -1)
	cols.AddRowUnlimited("Max Streams", ac.Limits.Streams, -1)
	cols.AddRowUnlimited("Max Consumers", ac.Limits.Consumer, -1)

	cols.AddSectionTitle("Default Permissions")
	cols.Indent(2)

	cols.AddSectionTitle("Publish")
	c.addPermission(cols, ac.DefaultPermissions.Pub)
	cols.AddSectionTitle("Subscribe")
	c.addPermission(cols, ac.DefaultPermissions.Sub)
	cols.Indent(0)

	cols.AddSectionTitle("Summary")
	cols.AddRow("Imports", len(ac.Imports))
	cols.AddRow("Exports", len(ac.Exports))
	cols.AddRow("Signing Keys", len(ac.SigningKeys))
	cols.AddRow("Revocations", len(ac.Revocations))
	cols.AddRow("Mappings", len(ac.Mappings))

	if len(ac.Exports) > 0 {
		exports := []string{}
		for _, e := range ac.Exports {
			exports = append(exports, fmt.Sprintf("%s (%s)", e.Subject, e.Type))
		}
		cols.AddStringsAsValue("Exported Subjects", exports)
	}
	if len(ac.Imports) > 0 {
		imports := []string{}
		for _, i := range ac.Imports {
			imports = append(imports, fmt.Sprintf("%s from %s (%s)", i.Subject, i.Account, i.Type))
		}
		cols.AddStringsAsValue("Imported Subjects", imports)
	}

	if ac.Authorization.AuthUsers != nil {
		cols.AddSectionTitle("External Authorization")
		cols.AddStringsAsValue("Auth Users", ac.Authorization.AuthUsers)
		if len(ac.Authorization.AllowedAccounts) > 0 {
			cols.AddStringsAsValue("Allowed Accounts", ac.Authorization.AllowedAccounts)
		}
		cols.AddRowIfNotEmpty("XKey", ac.Authorization.XKey)
	}

	return cols
}

func (c *jwtDecodeCmd) showActivation(a *jwt.ActivationClaims) *columns.Writer {
	cols := newColumnsf("Activation %s (%s)", a.Name, a.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, a.Claims())
	cols.AddRow("Import Subject", a.ImportSubject)
	cols.AddRow("Import Type", a.ImportType)
	cols.AddRowIfNotEmpty("Issuer Account", a.IssuerAccount)
	if len(a.Tags) > 0 {
		cols.AddRow("Tags", strings.Join(a.Tags, ", "))
	}

	return cols
}

func (c *jwtDecodeCmd) showAuthRequest(ar *jwt.AuthorizationRequestClaims) *columns.Writer {
	cols := newColumnsf("Authorization Request %s (%s)", ar.Name, ar.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, ar.Claims())
	cols.AddRow("User NKey", ar.UserNkey)
	cols.AddRowIfNotEmpty("Request Nonce", ar.RequestNonce)

	cols.AddSectionTitle("Server")
	cols.AddRowIfNotEmpty("Name", ar.Server.Name)
	cols.AddRowIfNotEmpty("Id", ar.Server.ID)
	cols.AddRowIfNotEmpty("Host", ar.Server.Host)
	cols.AddRowIfNotEmpty("Cluster", ar.Server.Cluster)
	cols.AddRowIfNotEmpty("Version", ar.Server.Version)

	cols.AddSectionTitle("Client")
	cols.AddRow("Id", ar.ClientInformation.ID)
	cols.AddRowIfNotEmpty("Name", ar.ClientInformation.Name)
	cols.AddRowIfNotEmpty("Host", ar.ClientInformation.Host)
	cols.AddRowIfNotEmpty("User", ar.ClientInformation.User)
	cols.AddRowIfNotEmpty("Kind", ar.ClientInformation.Kind)
	cols.AddRowIfNotEmpty("Type", ar.ClientInformation.Type)

	cols.AddSectionTitle("Connect Options")
	cols.AddRowIfNotEmpty("Username", ar.ConnectOptions.Username)
	cols.AddRowIfNotEmpty("Language", ar.ConnectOptions.Lang)
	cols.AddRowIfNotEmpty("Client Version", ar.ConnectOptions.Version)
	cols.AddRow("Protocol", ar.ConnectOptions.Protocol)

	if ar.TLS != nil {
		cols.AddSectionTitle("TLS")
		cols.AddRowIfNotEmpty("Version", ar.TLS.Version)
		cols.AddRowIfNotEmpty("Cipher", ar.TLS.Cipher)
	}

	return cols
}

func (c *jwtDecodeCmd) showAuthResponse(ar *jwt.AuthorizationResponseClaims) *columns.Writer {
	cols := newColumnsf("Authorization Response %s (%s)", ar.Name, ar.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, ar.Claims())
	cols.AddRowIfNotEmpty("Issuer Account", ar.IssuerAccount)
	if ar.Error != "" {
		cols.AddRow("Error", ar.Error)
	}
	cols.AddRow("User JWT Issued", ar.Jwt != "")

	return cols
}

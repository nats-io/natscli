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
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/columns"
	au "github.com/nats-io/natscli/internal/auth"
	iu "github.com/nats-io/natscli/internal/util"
	ab "github.com/synadia-io/jwt-auth-builder.go"
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
	var err error
	switch claims := claims.(type) {
	case *jwt.UserClaims:
		cols = c.newClaimsColumns("User", claims.Claims())
		err = c.showUser(claims, cols)
	case *jwt.AccountClaims:
		cols = c.newClaimsColumns("Account", claims.Claims())
		err = c.showAccount(claims, cols)
	case *jwt.OperatorClaims:
		cols = c.newClaimsColumns("Operator", claims.Claims())
		err = c.showOperator(claims, cols)
	case *jwt.ActivationClaims:
		cols = c.newClaimsColumns("Activation", claims.Claims())
		c.showActivation(claims, cols)
	case *jwt.AuthorizationRequestClaims:
		cols = c.newClaimsColumns("Authorization Request", claims.Claims())
		c.showAuthRequest(claims, cols)
	case *jwt.AuthorizationResponseClaims:
		cols = c.newClaimsColumns("Authorization Response", claims.Claims())
		c.showAuthResponse(claims, cols)
	default:
		return "", fmt.Errorf("unsupported JWT claim type %q", claims.ClaimType())
	}
	if err != nil {
		return "", err
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

func (c *jwtDecodeCmd) newClaimsColumns(kind string, d *jwt.ClaimsData) *columns.Writer {
	cols := newColumnsf("%s %s (%s)", kind, d.Name, d.Subject)

	cols.AddSectionTitle("Token")
	cols.AddRow("Id", d.ID)
	cols.AddRowIfNotEmpty("Audience", d.Audience)
	cols.AddRow("Issued At", time.Unix(d.IssuedAt, 0))
	if d.Expires > 0 {
		cols.AddRow("Expires", time.Unix(d.Expires, 0))
	} else {
		cols.AddRow("Expires", "never")
	}
	cols.AddRowIf("Not Before", time.Unix(d.NotBefore, 0), d.NotBefore > 0)

	return cols
}

func (c *jwtDecodeCmd) showUser(uc *jwt.UserClaims, cols *columns.Writer) error {
	err := au.RenderUser(&ab.UserData{Claim: uc, BaseData: ab.BaseData{EntityName: uc.Name}}, nil, cols)
	if err != nil {
		return err
	}

	if uc.ProxyRequired || uc.Resp != nil {
		cols.AddSectionTitle("Additional Settings")
		cols.AddRowIf("Proxy Required", true, uc.ProxyRequired)
		if uc.Resp != nil {
			cols.AddRow("Allow Responses", c.responsePermission(uc.Resp))
		}
	}

	return nil
}

// the server replaces zero response limits with its defaults, negative values are unlimited
func (c *jwtDecodeCmd) responsePermission(resp *jwt.ResponsePermission) string {
	msgs := fmt.Sprint(resp.MaxMsgs)
	switch {
	case resp.MaxMsgs < 0:
		msgs = "unlimited"
	case resp.MaxMsgs == 0:
		msgs = fmt.Sprint(server.DEFAULT_ALLOW_RESPONSE_MAX_MSGS)
	}

	expires := resp.Expires.String()
	switch {
	case resp.Expires < 0:
		expires = "never"
	case resp.Expires == 0:
		expires = server.DEFAULT_ALLOW_RESPONSE_EXPIRATION.String()
	}

	return fmt.Sprintf("%s messages, expires %s", msgs, expires)
}

func (c *jwtDecodeCmd) showOperator(oc *jwt.OperatorClaims, cols *columns.Writer) error {
	err := au.RenderOperator(&ab.OperatorData{Claim: oc, BaseData: ab.BaseData{EntityName: oc.Name}}, oc, cols)
	if err != nil {
		return err
	}

	if oc.Issuer != oc.Subject || oc.AssertServerVersion != "" || oc.StrictSigningKeyUsage {
		cols.AddSectionTitle("Additional Settings")
		cols.AddRowIf("Issuer", oc.Issuer, oc.Issuer != oc.Subject)
		cols.AddRowIfNotEmpty("Assert Server Version", oc.AssertServerVersion)
		cols.AddRowIf("Strict Signing Key Usage", true, oc.StrictSigningKeyUsage)
	}

	return nil
}

func (c *jwtDecodeCmd) showAccount(ac *jwt.AccountClaims, cols *columns.Writer) error {
	err := au.RenderAccount(&ab.AccountData{Claim: ac, BaseData: ab.BaseData{EntityName: ac.Name}}, nil, ac, cols)
	if err != nil {
		return err
	}

	signingKeys := []string{}
	for key, scope := range ac.SigningKeys {
		us, ok := scope.(*jwt.UserScope)
		switch {
		case !ok:
			signingKeys = append(signingKeys, key)
		case us.Role != "":
			signingKeys = append(signingKeys, fmt.Sprintf("%s (scoped: %s)", key, us.Role))
		default:
			signingKeys = append(signingKeys, fmt.Sprintf("%s (scoped)", key))
		}
	}

	mappings := []string{}
	for source, targets := range ac.Mappings {
		for _, target := range targets {
			mapping := fmt.Sprintf("%s -> %s", source, target.Subject)
			if target.GetWeight() != 100 {
				mapping += fmt.Sprintf(" (%d%%)", target.GetWeight())
			}
			if target.Cluster != "" {
				mapping += fmt.Sprintf(" in cluster %s", target.Cluster)
			}
			mappings = append(mappings, mapping)
		}
	}

	if ac.Description != "" || ac.InfoURL != "" || !ac.Limits.WildcardExports || len(signingKeys)+len(mappings) > 0 {
		cols.AddSectionTitle("Additional Settings")
		cols.AddRowIfNotEmpty("Description", ac.Description)
		cols.AddRowIfNotEmpty("Info URL", ac.InfoURL)
		cols.AddRowIf("Wildcard Exports", false, !ac.Limits.WildcardExports)
		cols.AddStringsAsValue("Signing Keys", signingKeys)
		cols.AddStringsAsValue("Mappings", mappings)
	}

	pub := ac.DefaultPermissions.Pub
	sub := ac.DefaultPermissions.Sub
	if len(pub.Allow)+len(pub.Deny)+len(sub.Allow)+len(sub.Deny) > 0 {
		cols.AddSectionTitle("Default Permissions")
		cols.Indent(2)
		au.RenderPermission(cols, "Publish", pub.Allow, pub.Deny)
		au.RenderPermission(cols, "Subscribe", sub.Allow, sub.Deny)
		cols.Indent(0)
	}

	if len(ac.Exports)+len(ac.Imports) > 0 {
		exports := []string{}
		for _, e := range ac.Exports {
			if e.TokenReq {
				exports = append(exports, fmt.Sprintf("%s (%s, token required)", e.Subject, e.Type))
			} else {
				exports = append(exports, fmt.Sprintf("%s (%s)", e.Subject, e.Type))
			}
		}
		imports := []string{}
		for _, i := range ac.Imports {
			imports = append(imports, fmt.Sprintf("%s from %s (%s)", i.Subject, i.Account, i.Type))
		}

		cols.AddSectionTitle("Exports and Imports")
		cols.AddStringsAsValue("Exported Subjects", exports)
		cols.AddStringsAsValue("Imported Subjects", imports)
	}

	if len(ac.Authorization.AuthUsers) > 0 {
		cols.AddSectionTitle("External Authorization")
		cols.AddStringsAsValue("Auth Users", ac.Authorization.AuthUsers)
		cols.AddStringsAsValue("Allowed Accounts", ac.Authorization.AllowedAccounts)
		cols.AddRowIfNotEmpty("XKey", ac.Authorization.XKey)
	}

	return nil
}

func (c *jwtDecodeCmd) showActivation(a *jwt.ActivationClaims, cols *columns.Writer) {
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Issuer", a.Issuer)
	cols.AddRowIfNotEmpty("Issuer Account", a.IssuerAccount)
	cols.AddRow("Subject", a.Subject)
	cols.AddRow("Import Subject", a.ImportSubject)
	cols.AddRow("Import Type", a.ImportType)
	cols.AddStringsAsValue("Tags", a.Tags)
}

func (c *jwtDecodeCmd) showAuthRequest(ar *jwt.AuthorizationRequestClaims, cols *columns.Writer) {
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Issuer", ar.Issuer)
	cols.AddRow("Subject", ar.Subject)
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
}

func (c *jwtDecodeCmd) showAuthResponse(ar *jwt.AuthorizationResponseClaims, cols *columns.Writer) {
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Issuer", ar.Issuer)
	cols.AddRowIfNotEmpty("Issuer Account", ar.IssuerAccount)
	cols.AddRow("Subject", ar.Subject)
	cols.AddRowIfNotEmpty("Error", ar.Error)
	cols.AddRow("User JWT Issued", ar.Jwt != "")
}

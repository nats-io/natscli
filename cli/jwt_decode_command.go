package cli

import (
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
	//contains jwtstring or location
	jwt   string
	json  bool
	token string
}

func configureJwtDecodeCommand(jwtCmd commandHost) {
	c := &jwtDecodeCmd{}

	decode := jwtCmd.Command("decode", "Decode Jwt Claims").Alias("de").Alias("d").Action(c.decodeAction)
	decode.Tag("impact:ro")
	decode.Arg("jwt", "Jwt string or location").Required().StringVar(&c.jwt)
	decode.Flag("json", "Produce json output").Short('j').UnNegatableBoolVar(&c.json)
}
func (c *jwtDecodeCmd) loadToken() error {

	if _, err := os.Stat(c.jwt); err == nil {
		return c.extractToken()
	}

	if strings.Count(c.jwt, ".") == 2 {
		c.token = c.jwt
		return nil
	}

	return fmt.Errorf("%q is neither a file nor a JWT", c.jwt)
}

func (c *jwtDecodeCmd) decodeAction(_ *fisk.ParseContext) error {
	if c.jwt != "" {
		c.jwt = strings.TrimSpace(c.jwt)
		c.loadToken()
	}

	err := c.decodeClaimsAndDisplay(os.Stdout)
	if err != nil {
		return err
	}

	return nil
}

func (c *jwtDecodeCmd) extractToken() error {
	bytes, err := os.ReadFile(c.jwt)
	if err != nil {
		return err
	}

	c.token = strings.TrimSpace(string(bytes))
	if token, err := jwt.ParseDecoratedJWT(bytes); err == nil {
		c.token = token
	}

	return nil
}

func (c *jwtDecodeCmd) decodeClaimsAndDisplay(w io.Writer) error {
	gc, err := jwt.DecodeGeneric(c.token)
	if err != nil {
		return err
	}

	var vr jwt.ValidationResults
	switch gc.ClaimType() {
	case jwt.UserClaim:
		claims, err := jwt.DecodeUserClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.UserClaim, *claims)
		if err != nil {
			return err
		}
	case jwt.AccountClaim:
		claims, err := jwt.DecodeAccountClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.AccountClaim, *claims)
		if err != nil {
			return err
		}
	case jwt.OperatorClaim:
		claims, err := jwt.DecodeOperatorClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.OperatorClaim, *claims)
		if err != nil {
			return err
		}
	case jwt.ActivationClaim:
		claims, err := jwt.DecodeActivationClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.ActivationClaim, *claims)
		if err != nil {
			return err
		}
	case jwt.AuthorizationRequestClaim:
		claims, err := jwt.DecodeAuthorizationRequestClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.AuthorizationRequestClaim, *claims)
		if err != nil {
			return err
		}
	case jwt.AuthorizationResponseClaim:
		claims, err := jwt.DecodeAuthorizationResponseClaims(c.token)
		if err != nil {
			return err
		}
		claims.Validate(&vr)
		err = c.fShowAny(w, jwt.AuthorizationResponseClaim, *claims)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Jwt claims type not recognized")
	}

	if vr.IsBlocking(true) {
		return fmt.Errorf("Jwt validation failed")
	}

	return nil
}

func (c *jwtDecodeCmd) fShowAny(w io.Writer, t jwt.ClaimType, claims any) error {
	var out string
	var err error
	switch t {
	case jwt.UserClaim:
		out, err = c.showUser(claims.(jwt.UserClaims))
	case jwt.AccountClaim:
		out, err = c.showAccount(claims.(jwt.AccountClaims))
	case jwt.OperatorClaim:
		out, err = c.showOperator(claims.(jwt.OperatorClaims))
	case jwt.ActivationClaim:
		out, err = c.showActivation(claims.(jwt.ActivationClaims))
	case jwt.AuthorizationRequestClaim:
		out, err = c.showAuthRequest(claims.(jwt.AuthorizationRequestClaims))
	case jwt.AuthorizationResponseClaim:
		out, err = c.showAuthResponse(claims.(jwt.AuthorizationResponseClaims))
	default:
		return fmt.Errorf("claim type not supported")
	}
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *jwtDecodeCmd) fmtTime(ts int64, zero string) string {
	if ts == 0 {
		return zero
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

func (c *jwtDecodeCmd) addClaimsData(cols *columns.Writer, d *jwt.ClaimsData) {
	cols.AddRow("Id", d.ID)
	cols.AddRowIfNotEmpty("Audience", d.Audience)
	cols.AddRow("Issuer", d.Issuer)
	cols.AddRow("Subject", d.Subject)
	cols.AddRow("Issued At", c.fmtTime(d.IssuedAt, "Unknown"))
	cols.AddRow("Expires", c.fmtTime(d.Expires, "Never"))
	cols.AddRow("Not Before", c.fmtTime(d.NotBefore, "Not set"))
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

func (c *jwtDecodeCmd) showUser(uc jwt.UserClaims) (string, error) {
	if c.json {
		return iu.ToJSON(uc)
	}

	cols := newColumnsf("User %s (%s)", uc.Name, uc.Subject)
	cols.AddSectionTitle("Configuration")

	c.addClaimsData(cols, uc.Claims())
	cols.AddRow("Name", uc.Name)
	cols.AddRow("Subject", uc.Subject)
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
	if len(uc.Pub.Allow) > 0 || len(uc.Pub.Deny) > 0 {
		if len(uc.Pub.Allow) > 0 {
			cols.AddStringsAsValue("Allow", uc.Pub.Allow)
		}
		if len(uc.Pub.Deny) > 0 {
			cols.AddStringsAsValue("Deny", uc.Pub.Deny)
		}
	} else {
		cols.Println("No permissions defined")
	}

	cols.AddSectionTitle("Subscribe")
	if len(uc.Sub.Allow) > 0 || len(uc.Sub.Deny) > 0 {
		if len(uc.Sub.Allow) > 0 {
			cols.AddStringsAsValue("Allow", uc.Sub.Allow)
		}
		if len(uc.Sub.Deny) > 0 {
			cols.AddStringsAsValue("Deny", uc.Sub.Deny)
		}
	} else {
		cols.Println("No permissions defined")
	}

	cols.Indent(0)

	return cols.Render()
}

func (c *jwtDecodeCmd) showOperator(oc jwt.OperatorClaims) (string, error) {
	if c.json {
		return iu.ToJSON(oc)
	}

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

	return cols.Render()
}

func (c *jwtDecodeCmd) showAccount(ac jwt.AccountClaims) (string, error) {
	if c.json {
		return iu.ToJSON(ac)
	}

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

	return cols.Render()
}

func (c *jwtDecodeCmd) showActivation(a jwt.ActivationClaims) (string, error) {
	if c.json {
		return iu.ToJSON(a)
	}

	cols := newColumnsf("Activation %s (%s)", a.Name, a.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, a.Claims())
	cols.AddRow("Import Subject", a.ImportSubject)
	cols.AddRow("Import Type", a.ImportType)
	cols.AddRowIfNotEmpty("Issuer Account", a.IssuerAccount)
	if len(a.Tags) > 0 {
		cols.AddRow("Tags", strings.Join(a.Tags, ", "))
	}

	return cols.Render()
}

func (c *jwtDecodeCmd) showAuthRequest(ar jwt.AuthorizationRequestClaims) (string, error) {
	if c.json {
		return iu.ToJSON(ar)
	}

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

	return cols.Render()
}

func (c *jwtDecodeCmd) showAuthResponse(ar jwt.AuthorizationResponseClaims) (string, error) {
	if c.json {
		return iu.ToJSON(ar)
	}

	cols := newColumnsf("Authorization Response %s (%s)", ar.Name, ar.Subject)
	cols.AddSectionTitle("Configuration")
	c.addClaimsData(cols, ar.Claims())
	cols.AddRowIfNotEmpty("Issuer Account", ar.IssuerAccount)
	if ar.Error != "" {
		cols.AddRow("Error", ar.Error)
	}
	cols.AddRow("User JWT Issued", ar.Jwt != "")

	return cols.Render()
}

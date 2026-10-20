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

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func TestJwtDecode(t *testing.T) {
	mustKey := func(create func() (nkeys.KeyPair, error)) (nkeys.KeyPair, string) {
		t.Helper()
		kp, err := create()
		checkErr(t, err, "creating nkey: %v", err)
		pub, err := kp.PublicKey()
		checkErr(t, err, "reading public key: %v", err)
		return kp, pub
	}

	mustEncode := func(c interface {
		Encode(nkeys.KeyPair) (string, error)
	}, signer nkeys.KeyPair) string {
		t.Helper()
		tok, err := c.Encode(signer)
		checkErr(t, err, "encoding JWT: %v", err)
		return tok
	}

	decodePayload := func(t *testing.T, token string) map[string]any {
		t.Helper()
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Fatalf("expected 3 JWT segments, got %d", len(parts))
		}
		raw, err := base64.RawURLEncoding.DecodeString(parts[1])
		checkErr(t, err, "decoding JWT payload: %v", err)
		var claims map[string]any
		err = json.Unmarshal(raw, &claims)
		checkErr(t, err, "unmarshalling JWT payload: %v", err)
		return claims
	}

	exp := time.Now().Add(24 * time.Hour).Unix()

	opKP, opPub := mustKey(nkeys.CreateOperator)
	accKP, accPub := mustKey(nkeys.CreateAccount)
	_, targetAccPub := mustKey(nkeys.CreateAccount)
	usrKP, usrPub := mustKey(nkeys.CreateUser)
	srvKP, srvPub := mustKey(nkeys.CreateServer)

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "O"
	oc.SystemAccount = accPub
	operatorJwt := mustEncode(oc, opKP)

	skKP, skPub := mustKey(nkeys.CreateAccount)

	ac := jwt.NewAccountClaims(accPub)
	ac.Name = "A"
	ac.Exports.Add(&jwt.Export{Subject: "svc.>", Type: jwt.Service, TokenReq: true})
	ac.Limits.WildcardExports = false
	scope := jwt.NewUserScope()
	scope.Key = skPub
	scope.Role = "service"
	ac.SigningKeys.AddScopedSigner(scope)
	accountJwt := mustEncode(ac, opKP)

	uc := jwt.NewUserClaims(usrPub)
	uc.Name = "U"
	uc.Pub.Allow.Add("orders.>")
	uc.Sub.Allow.Add("_INBOX.>")
	uc.ProxyRequired = true
	uc.Resp = &jwt.ResponsePermission{MaxMsgs: 5, Expires: time.Minute}
	userJwt := mustEncode(uc, accKP)

	act := jwt.NewActivationClaims(targetAccPub)
	act.Name = "CrossAccountServiceToken"
	act.ImportSubject = "orders.validate"
	act.ImportType = jwt.Service
	act.Expires = exp
	activationJwt := mustEncode(act, accKP)

	req := jwt.NewAuthorizationRequestClaims(accPub)
	req.Name = "NATS-Server-Auth-Request"
	req.Server = jwt.ServerID{Name: "test-server", Host: "127.0.0.1", ID: srvPub}
	req.UserNkey = usrPub
	req.ClientInformation = jwt.ClientInformation{Host: "192.168.1.100", Name: "alice-client-connection", Kind: "Client", Type: "nats"}
	req.ConnectOptions = jwt.ConnectOptions{Username: "alice", Protocol: 1}
	req.TLS = &jwt.ClientTLS{Version: "1.3", Cipher: "TLS_AES_256_GCM_SHA384"}
	req.Expires = exp
	authRequestJwt := mustEncode(req, srvKP)

	resp := jwt.NewAuthorizationResponseClaims(usrPub)
	resp.Name = "Auth-Service-Response"
	resp.Audience = srvPub
	resp.Jwt = userJwt
	resp.Expires = exp
	authResponseJwt := mustEncode(resp, accKP)

	t.Run("--json", func(t *testing.T) {
		cases := []struct {
			name     string
			jwt      string
			wantType string
			wantName string
			wantSub  string
		}{
			{"operator", operatorJwt, "operator", "O", opPub},
			{"account", accountJwt, "account", "A", accPub},
			{"user", userJwt, "user", "U", usrPub},
			{"activation", activationJwt, "activation", "CrossAccountServiceToken", targetAccPub},
			{"auth_request", authRequestJwt, "authorization_request", "NATS-Server-Auth-Request", accPub},
			{"auth_response", authResponseJwt, "authorization_response", "Auth-Service-Response", usrPub},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				want := decodePayload(t, tc.jwt)
				output := runNatsCli(t, fmt.Sprintf("jwt d %s --json", tc.jwt))

				var got map[string]any
				err := json.Unmarshal(output, &got)
				checkErr(t, err, "output is not valid JSON: %v\noutput: %s", err, output)

				natsClaims, ok := got["nats"].(map[string]any)
				if !ok {
					t.Fatalf("missing or invalid \"nats\" claim in output: %s", output)
				}
				if natsClaims["type"] != tc.wantType {
					t.Errorf("nats.type = %v, want %q", natsClaims["type"], tc.wantType)
				}
				if got["name"] != tc.wantName {
					t.Errorf("name = %v, want %q", got["name"], tc.wantName)
				}
				if got["sub"] != tc.wantSub {
					t.Errorf("sub = %v, want %q", got["sub"], tc.wantSub)
				}
				if v, _ := natsClaims["version"].(float64); v != 2 {
					t.Errorf("nats.version = %v, want 2", natsClaims["version"])
				}

				err = expectMatchJSON(t, string(output), want)
				if err != nil {
					gotJSON, _ := json.MarshalIndent(got, "", "  ")
					wantJSON, _ := json.MarshalIndent(want, "", "  ")
					t.Errorf("decoded claims differ\n--- got ---\n%s\n--- want ---\n%s error: %s", gotJSON, wantJSON, err.Error())
				}
			})
		}
	})

	t.Run("creds file", func(t *testing.T) {
		seed, err := usrKP.Seed()
		checkErr(t, err, "reading seed: %v", err)
		creds, err := jwt.FormatUserConfig(userJwt, seed)
		checkErr(t, err, "formatting creds: %v", err)
		credsFile := filepath.Join(t.TempDir(), "user.creds")
		err = os.WriteFile(credsFile, creds, 0600)
		checkErr(t, err, "writing creds: %v", err)

		output := string(runNatsCli(t, fmt.Sprintf("jwt decode '%s'", credsFile)))

		if !expectMatchRegex(t, fmt.Sprintf(`User U \(%s\)`, usrPub), output) {
			t.Errorf("user heading not found in output: %s", output)
		}
		if strings.Contains(output, string(seed)) {
			t.Errorf("seed found in output: %s", output)
		}
	})

	t.Run("operator", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", operatorJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Configuration": map[string]any{"System Account": "^" + accPub + "$"},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}
	})

	opSKKP, opSKPub := mustKey(nkeys.CreateOperator)
	signed := jwt.NewOperatorClaims(opPub)
	signed.Name = "O"
	signedJwt := mustEncode(signed, opSKKP)

	t.Run("operator signed by another key", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", signedJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Additional Settings": map[string]any{"Issuer": "^" + opSKPub + "$"},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}
	})

	t.Run("user", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", userJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Additional Settings": map[string]any{
				"Proxy Required":  "^true$",
				"Allow Responses": "^5 messages, expires 1m0s$",
			},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}
	})

	t.Run("account", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", accountJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Exports and Imports": map[string]any{"Exported Subjects": `^svc\.> \(service, token required\)$`},
			"Additional Settings": map[string]any{
				"Signing Keys":     "^" + skPub + ` \(scoped: service\)$`,
				"Wildcard Exports": "^false$",
			},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}

		var got struct {
			Configuration map[string]any
		}
		err = json.Unmarshal([]byte(output), &got)
		checkErr(t, err, "output is not valid JSON: %v\noutput: %s", err, output)
		if _, ok := got.Configuration["Users"]; ok {
			t.Errorf("user count found in output: %s", output)
		}
	})

	mixed := jwt.NewAccountClaims(accPub)
	mixed.Name = "mixed"
	mixed.Limits.JetStreamLimits = jwt.JetStreamLimits{DiskStorage: -1, MemoryStorage: 1024 * 1024, Streams: 10, Consumer: -1, MaxAckPending: -1}
	mixedJwt := mustEncode(mixed, opKP)

	t.Run("mixed JetStream limits", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", mixedJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Account Default Limits": map[string]any{
				"Max File Storage":   "^unlimited$",
				"Max Memory Storage": `^1\.0 MiB$`,
				"Max Ack Pending":    "^unlimited$",
			},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}
	})

	scoped := jwt.NewUserClaims(usrPub)
	scoped.Name = "scoped"
	scoped.IssuerAccount = accPub
	scoped.UserPermissionLimits = jwt.UserPermissionLimits{}
	scopedJwt := mustEncode(scoped, skKP)

	t.Run("scoped user", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", scopedJwt))

		err := expectMatchJSON(t, output, map[string]any{
			"Configuration": map[string]any{"Scoped": "^true$"},
			"Limits":        map[string]any{"header": []any{"^Set by the signing key scope in the account JWT$"}},
		})
		if err != nil {
			t.Errorf("%v\noutput: %s", err, output)
		}
	})

	expired := jwt.NewUserClaims(usrPub)
	expired.Name = "expired"
	expired.Expires = time.Now().Add(-time.Hour).Unix()
	expiredJwt := mustEncode(expired, accKP)

	t.Run("expired", func(t *testing.T) {
		output := string(runNatsCli(t, "jwt decode", expiredJwt))

		if !expectMatchLine(t, output, "Validation Issues") {
			t.Errorf("validation issues section not found in output: %s", output)
		}
		if !expectMatchLine(t, output, "claim is expired") {
			t.Errorf("expiry issue not found in output: %s", output)
		}
	})

	t.Run("expired --json", func(t *testing.T) {
		output := runNatsCli(t, "jwt decode --json", expiredJwt)

		var got map[string]any
		err := json.Unmarshal(output, &got)
		checkErr(t, err, "output is not valid JSON: %v\noutput: %s", err, output)

		err = expectMatchJSON(t, string(output), decodePayload(t, expiredJwt))
		if err != nil {
			t.Errorf("decoded claims differ: %v\noutput: %s", err, output)
		}
	})

	t.Run("not a JWT", func(t *testing.T) {
		for _, input := range []string{"notajwt", "alice.user.creds"} {
			err := runNatsCliWithError(t, "jwt decode", input)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q is neither a file nor a JWT", input)) {
				t.Errorf("unexpected error for %s: %v", input, err)
			}
		}
	})

	t.Run("file without a JWT", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "notajwt.txt")
		err := os.WriteFile(file, []byte("hello world\n"), 0600)
		checkErr(t, err, "writing file: %v", err)

		err = runNatsCliWithError(t, fmt.Sprintf("jwt decode '%s'", file))
		if err == nil || !strings.Contains(err.Error(), file+" does not contain a JWT") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

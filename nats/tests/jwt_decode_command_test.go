package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func decodeJWTPayload(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT segments, got %d", len(parts))
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decoding JWT payload: %v", err)
	}

	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshalling JWT payload: %v", err)
	}
	return claims
}

type jwtEncoder interface {
	Encode(nkeys.KeyPair) (string, error)
}

func mustEncode(t *testing.T, c jwtEncoder, signer nkeys.KeyPair) string {
	t.Helper()
	tok, err := c.Encode(signer)
	if err != nil {
		t.Fatalf("encoding JWT: %v", err)
	}
	return tok
}

func mustKey(t *testing.T, create func() (nkeys.KeyPair, error)) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := create()
	if err != nil {
		t.Fatalf("creating nkey: %v", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatalf("reading public key: %v", err)
	}
	return kp, pub
}

func TestJwtDecodeCmdWithJsonFlag(t *testing.T) {
	exp := time.Now().Add(24 * time.Hour).Unix()

	opKP, opPub := mustKey(t, nkeys.CreateOperator)
	accKP, accPub := mustKey(t, nkeys.CreateAccount)
	_, targetAccPub := mustKey(t, nkeys.CreateAccount)
	_, usrPub := mustKey(t, nkeys.CreateUser)
	srvKP, srvPub := mustKey(t, nkeys.CreateServer)

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "O"
	operatorJwt := mustEncode(t, oc, opKP)

	ac := jwt.NewAccountClaims(accPub)
	ac.Name = "A"
	accountJwt := mustEncode(t, ac, opKP)

	uc := jwt.NewUserClaims(usrPub)
	uc.Name = "U"
	uc.Pub.Allow.Add("orders.>")
	uc.Sub.Allow.Add("_INBOX.>")
	userJwt := mustEncode(t, uc, accKP)

	act := jwt.NewActivationClaims(targetAccPub)
	act.Name = "CrossAccountServiceToken"
	act.ImportSubject = "orders.validate"
	act.ImportType = jwt.Service
	act.Expires = exp
	activationJwt := mustEncode(t, act, accKP)

	req := jwt.NewAuthorizationRequestClaims(accPub)
	req.Name = "NATS-Server-Auth-Request"
	req.Server = jwt.ServerID{Name: "test-server", Host: "127.0.0.1", ID: srvPub}
	req.UserNkey = usrPub
	req.ClientInformation = jwt.ClientInformation{Host: "192.168.1.100", Name: "alice-client-connection", Kind: "Client", Type: "nats"}
	req.ConnectOptions = jwt.ConnectOptions{Username: "alice", Protocol: 1}
	req.TLS = &jwt.ClientTLS{Version: "1.3", Cipher: "TLS_AES_256_GCM_SHA384"}
	req.Expires = exp
	authRequestJwt := mustEncode(t, req, srvKP)

	resp := jwt.NewAuthorizationResponseClaims(usrPub)
	resp.Name = "Auth-Service-Response"
	resp.Audience = srvPub
	resp.Jwt = userJwt
	resp.Expires = exp
	authResponseJwt := mustEncode(t, resp, accKP)

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
			want := decodeJWTPayload(t, tc.jwt)

			if tc.name == "activation" {
				want["nats"].(map[string]any)["kind"] = 2
			}
			output := runNatsCli(t, fmt.Sprintf("jwt d %s --json", tc.jwt))

			var got map[string]any
			if err := json.Unmarshal(output, &got); err != nil {
				t.Fatalf("output is not valid JSON: %v\noutput: %s", err, output)
			}

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

			err := expectMatchJSON(t, string(output), want)
			if err != nil {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(want, "", "  ")
				t.Errorf("decoded claims differ\n--- got ---\n%s\n--- want ---\n%s error: %s", gotJSON, wantJSON, err.Error())
			}
		})
	}
}

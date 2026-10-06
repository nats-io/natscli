package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jwt/v2"
)

type decodeJwtCmd struct {
	jsonify bool
	data    headerAndClaims
}

type headerAndClaims struct {
	Header   jwt.Header        `json:"header"`
	Claims   jwt.GenericClaims `json:"claims"`
	IssuedAt time.Time         `json:"issuedAt,omitzero"`
}

func getJwtFromCreds(path string) (string, error) {

	bytes, err := os.ReadFile(path)

	if err != nil {
		return "", err
	}

	creds := string(bytes)
	credsSeparated := strings.Split(creds, "-----BEGIN NATS USER JWT-----")

	if len(credsSeparated) < 2 {
		return "", fmt.Errorf("did not find jwt on the specified path ")
	}

	creds = credsSeparated[1]
	credsSeparated = strings.Split(creds, "------END NATS USER JWT------")

	if len(credsSeparated) < 2 {
		return "", fmt.Errorf("did not find jwt on the specified path ")
	}
	creds = credsSeparated[0]

	return creds, nil

}

func decodeJwt(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func (c *decodeJwtCmd) DecodeCreds(jwtStr string) error {

	s := strings.Split(strings.TrimSpace(jwtStr), ".")

	if len(s) < 2 {
		return fmt.Errorf("jwt does not have minimum 2 chunks")
	}

	header := s[0]

	bytes, err := decodeJwt(header)

	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &c.data.Header)

	if err != nil {
		return err
	}

	claims := s[1]

	bytes, err = decodeJwt(claims)

	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &c.data.Claims)

	if err != nil {
		return err
	}

	if c.data.Claims.IssuedAt != 0 {
		c.data.IssuedAt = time.Unix(c.data.Claims.IssuedAt, 0).UTC()
	}

	return nil
}

func (c *decodeJwtCmd) decode(_ *fisk.ParseContext) error {

	credsPath := opts().Creds

	if credsPath == "" {
		return fmt.Errorf("no creds file provided, use --creds")
	}

	rawJwt, err := getJwtFromCreds(credsPath)

	if err != nil {
		return err
	}

	err = c.DecodeCreds(rawJwt)

	if err != nil {
		return fmt.Errorf("failed decoding creds jwt is malformed , %s", err.Error())
	}

	//combine both jsons into one
	jsonBytes, err := json.MarshalIndent(c.data, "", " ")

	if err != nil {
		return err
	}

	if c.jsonify {
		_, err := os.Stdout.Write(jsonBytes)

		if err != nil {
			return err
		}

	} else {

		fmt.Printf("header and claims : %s\n", string(jsonBytes))
	}

	return nil
}

func configureJwtDecodeCommand(app commandHost) {
	c := &decodeJwtCmd{
		jsonify: false,
	}

	const jwtHelpLong = `Decodes the user JWT stored in a .creds file and prints its header and claims in JSON

The creds file is taken from --creds or, if not given, from the selected context.
Only the JWT section of the file is read, any NKEY seed in it is never parsed or printed.

The signature is not verified, so don't treat the output as proof the token is genuine.

Examples:

nats jwt
nats jwt --creds user.creds
nats jwt --json | jq .claims.nats.pub
nats jwt --json --creds ./user.creds | jq .claims.nats.pub
`

	decodeJwt := app.Command("jwt", "Decode the JWT in a .creds file").Action(c.decode)
	decodeJwt.Tag("scope:user", "impact:ro")

	decodeJwt.HelpLong(jwtHelpLong)

	addCheat("jwt", decodeJwt)

	decodeJwt.Flag("json", "setting this flag true sends the decoded json bytes to stdout so you can pipe it to tools like jq, by default it's set to false").BoolVar(&c.jsonify)
}

func init() {
	registerCommand("sub", 20, configureJwtDecodeCommand)
}

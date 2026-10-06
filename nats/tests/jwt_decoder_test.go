package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJwtDecoder(t *testing.T) {

	testseed := "SUAB4AJTM7A2QO2C2PBEQ7RES7YZ66BJPZYT2IAXPG5ZR523YVE5T6IPOQ"

	t.Run("malformed jwt input", func(t *testing.T) {
		malformedJwtCreds := `-----BEGIN NATS USER JWT-----
		eyJ0eXAiOiJKV1QiiOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJHQUtYM1dFVkFST0lRSEczUEVPTldBWjZOSFZVTVJNQVhYVkxPSkE0Q0xKUTc2QzZNT0lRIiwiaWF0IjoxNzkxMTg1MzU5LCJpc3MiOiJBRFlHN0tPRVlKSUtNVFNRTkxLSVdWSUpDMkJTNzM1WEhZUlVPUFAzWFBCT0RXQVVIUUJBUVdZRCIsIm5hbWUiOiJvcmRlci1zdmMiLCJzdWIiOiJVQUs3QVhTTUQ1Q1hTS1U3VFJBMklMQVIyV1FDR1dFSVVHTU4yUVpST1BNNlJJUU9LTUE1Rk1SRiIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjoxMDQ4NTc2LCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.c3bbqysIUYQihwTjy3ggKJeErPxIjUZSOysouWRK-VuwPiFKnojTUSbRoUjdGG98OkYKKxmrPirkY-VpHcIEAA
		------END NATS USER JWT------

		************************* IMPORTANT *************************
		NKEY Seed printed below can be used to sign and prove identity.
		NKEYs are sensitive and should be treated as secrets.

		-----BEGIN USER NKEY SEED-----
		SUAB4AJTM7A2QO2C2PBEQ7RES7YZ66BJPZYT2IAXPG5ZR523YVE5T6IPOQ
		------END USER NKEY SEED------

		*************************************************************`

		tmpPath := filepath.Join(t.TempDir(), "test.creds")
		err := os.WriteFile(tmpPath, []byte(malformedJwtCreds), 0644)
		if err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}

		err = runNatsCliWithError(t, fmt.Sprintf(" jwt --creds %s", tmpPath))

		if strings.Contains(err.Error(), testseed) {
			t.Fatal("seeds leaked")
		}

		if !strings.Contains(err.Error(), "jwt is malformed") {
			t.Fatal("malformed jwt does not throw error")
		}

	})

	t.Run("missing creds file", func(t *testing.T) {

		err := runNatsCliWithError(t, "jwt")

		if err == nil {
			t.Fatal("path is missing must throw error")
		}

		if strings.Contains(err.Error(), testseed) {
			t.Fatal("seeds leaked")
		}

	})

	t.Run("decoding jwt", func(t *testing.T) {
		creds := `-----BEGIN NATS USER JWT-----
	eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJHQUtYM1dFVkFST0lRSEczUEVPTldBWjZOSFZVTVJNQVhYVkxPSkE0Q0xKUTc2QzZNT0lRIiwiaWF0IjoxNzkxMTg1MzU5LCJpc3MiOiJBRFlHN0tPRVlKSUtNVFNRTkxLSVdWSUpDMkJTNzM1WEhZUlVPUFAzWFBCT0RXQVVIUUJBUVdZRCIsIm5hbWUiOiJvcmRlci1zdmMiLCJzdWIiOiJVQUs3QVhTTUQ1Q1hTS1U3VFJBMklMQVIyV1FDR1dFSVVHTU4yUVpST1BNNlJJUU9LTUE1Rk1SRiIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjoxMDQ4NTc2LCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.c3bbqysIUYQihwTjy3ggKJeErPxIjUZSOysouWRK-VuwPiFKnojTUSbRoUjdGG98OkYKKxmrPirkY-VpHcIEAA
	------END NATS USER JWT------

	************************* IMPORTANT *************************
	NKEY Seed printed below can be used to sign and prove identity.
	NKEYs are sensitive and should be treated as secrets.

	-----BEGIN USER NKEY SEED-----
	SUAB4AJTM7A2QO2C2PBEQ7RES7YZ66BJPZYT2IAXPG5ZR523YVE5T6IPOQ
	------END USER NKEY SEED------

	*************************************************************
	`

		tmpPath := filepath.Join(t.TempDir(), "test.creds")
		err := os.WriteFile(tmpPath, []byte(creds), 0644)
		if err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}

		output := string(runNatsCli(t, fmt.Sprintf(" jwt --creds %s --json", tmpPath)))

		expected := map[string]any{
			"header": map[string]string{
				"typ": "JWT",
				"alg": "ed25519-nkey",
			},
			"claims": map[string]any{
				"jti":  "GAKX3WEVAROIQHG3PEONWAZ6NHVUMRMAXXVLOJA4CLJQ76C6MOIQ",
				"iat":  float64(1791185359),
				"iss":  "ADYG7KOEYJIKMTSQNLKIWVIJC2BS735XHYRUOPP3XPBODWAUHQBAQWYD",
				"name": "order-svc",
				"sub":  "UAK7AXSMD5CXSKU7TRA2ILAR2WQCGWEIUGMN2QZROPM6RIQOKMA5FMRF",
				"nats": map[string]any{
					"data":    -1,
					"payload": float64(1048576),
					"pub":     map[string]any{},
					"sub":     map[string]any{},
					"subs":    -1,
					"type":    "user",
					"version": 2,
				},
			},
			"issuedAt": "2026-10-05T07:29:19Z",
		}

		// check if the output has seed in it
		if strings.Contains(output, testseed) {
			t.Fatal("seed leadked")
		}

		mapBytes, err := json.MarshalIndent(expected, "", " ")

		if err != nil {
			t.Fatal(err)
		}

		var expectedAny any

		err = json.Unmarshal(mapBytes, &expectedAny)
		if err != nil {
			t.Fatal(err)
		}

		err = expectMatchJSON(t, output, expectedAny)
		if err != nil {
			t.Fatal(err)
		}

	})

	t.Run("no end marker , prevent seed leakage", func(t *testing.T) {
		creds := `-----BEGIN NATS USER JWT-----
	eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJHQUtYM1dFVkFST0lRSEczUEVPTldBWjZOSFZVTVJNQVhYVkxPSkE0Q0xKUTc2QzZNT0lRIiwiaWF0IjoxNzkxMTg1MzU5LCJpc3MiOiJBRFlHN0tPRVlKSUtNVFNRTkxLSVdWSUpDMkJTNzM1WEhZUlVPUFAzWFBCT0RXQVVIUUJBUVdZRCIsIm5hbWUiOiJvcmRlci1zdmMiLCJzdWIiOiJVQUs3QVhTTUQ1Q1hTS1U3VFJBMklMQVIyV1FDR1dFSVVHTU4yUVpST1BNNlJJUU9LTUE1Rk1SRiIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjoxMDQ4NTc2LCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.c3bbqysIUYQihwTjy3ggKJeErPxIjUZSOysouWRK-VuwPiFKnojTUSbRoUjdGG98OkYKKxmrPirkY-VpHcIEAA-

	************************* IMPORTANT *************************
	NKEY Seed printed below can be used to sign and prove identity.
	NKEYs are sensitive and should be treated as secrets.

	-----BEGIN USER NKEY SEED-----
	SUAB4AJTM7A2QO2C2PBEQ7RES7YZ66BJPZYT2IAXPG5ZR523YVE5T6IPOQ
	------END USER NKEY SEED------

	*************************************************************
	`
		tmpPath := filepath.Join(t.TempDir(), "test.creds")
		err := os.WriteFile(tmpPath, []byte(creds), 0644)
		if err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}

		err = runNatsCliWithError(t, fmt.Sprintf(" jwt --creds %s --json", tmpPath))

		if strings.Contains(err.Error(), testseed) {
			t.Fatal("seed leaked")
		}
		if err == nil {
			t.Fatal("leaked credentials without end marker , should throw error")
		}

		fmt.Println(err)

	})

}

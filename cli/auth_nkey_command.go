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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nkeys"
)

type authNKCommand struct {
	keyType        string
	pubOut         bool
	entropySource  string
	outFile        string
	keyFile        string
	dataFile       string
	signFile       string
	counterpartKey string
	useB64         bool
}

func configureAuthNkeyCommand(auth commandHost) {
	c := &authNKCommand{}

	nk := auth.Command("nkey", "Create and Use NKeys").Alias("nk")

	nkGen := nk.Command("gen", "Generates NKeys").Action(c.genAction)
	nkGen.Arg("type", "Type of key to generate").Required().EnumVar(&c.keyType, "user", "account", "server", "cluster", "operator", "curve", "x25519")
	nkGen.Flag("public", "Output the public key").UnNegatableBoolVar(&c.pubOut)
	nkGen.Flag("entropy", "Source of entropy eg. /dev/urandom").ExistingFileVar(&c.entropySource)
	nkGen.Flag("output", "Write the key to a file").StringVar(&c.outFile)

	nkShow := nk.Command("show", "Show the public key").Action(c.showAction)
	nkShow.Arg("key", "File containing NKey to act on").Required().ExistingFileVar(&c.keyFile)

	nkSign := nk.Command("sign", "Signs data using NKeys").Action(c.signAction)
	nkSign.Arg("file", "File to sign").Required().ExistingFileVar(&c.dataFile)
	nkSign.Arg("key", "File containing NKey to sign with").Required().ExistingFileVar(&c.keyFile)

	nkVerify := nk.Command("verify", "Verify signed data").Action(c.verifyAction)
	nkVerify.Arg("file", "File containing the data to check").Required().ExistingFileVar(&c.dataFile)
	nkVerify.Arg("signature", "File containing the signature").Required().ExistingFileVar(&c.signFile)
	nkVerify.Arg("key", "File containing NKey to use for verification").Required().ExistingFileVar(&c.keyFile)

	nkSeal := nk.Command("seal", "Encrypts a file using NKeys").Alias("encrypt").Alias("enc").Action(c.sealAction)
	nkSeal.Arg("file", "File to encrypt").Required().ExistingFileVar(&c.dataFile)
	nkSeal.Arg("key", "File containing NKey to encrypt with").Required().ExistingFileVar(&c.keyFile)
	nkSeal.Arg("recipient", "Public XKey of recipient").Required().StringVar(&c.counterpartKey)
	nkSeal.Flag("output", "Write the encrypted data to a file").StringVar(&c.outFile)
	nkSeal.Flag("b64", "Write base64 encoded data").Default("true").BoolVar(&c.useB64)

	nkOpen := nk.Command("unseal", "Decrypts a file using NKeys").Alias("open").Alias("decrypt").Alias("dec").Action(c.unsealAction)
	nkOpen.Arg("file", "File to decrypt").Required().ExistingFileVar(&c.dataFile)
	nkOpen.Arg("key", "File containing NKey to decrypt with").Required().ExistingFileVar(&c.keyFile)
	nkOpen.Arg("sender", "Public XKey of sender").Required().StringVar(&c.counterpartKey)
	nkOpen.Flag("output", "Write the decrypted data to a file").StringVar(&c.outFile)
	nkOpen.Flag("b64", "Read data in as base64 encoded").Default("true").BoolVar(&c.useB64)
}

func (c *authNKCommand) showAction(_ *fisk.ParseContext) error {
	seed, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		log.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}

	fmt.Println(pub)

	return nil
}

func (c *authNKCommand) genAction(_ *fisk.ParseContext) error {
	prefix, err := c.preForType(c.keyType)
	if err != nil {
		return err
	}

	ef := rand.Reader
	if c.entropySource != "" {
		r, err := os.Open(c.entropySource)
		if err != nil {
			return fmt.Errorf("could not use custom entropy source: %w", err)
		}

		ef = r
	}

	var kp nkeys.KeyPair

	if prefix == nkeys.PrefixByteCurve {
		kp, err = nkeys.CreateCurveKeysWithRand(ef)
	} else {
		kp, err = nkeys.CreatePairWithRand(prefix, ef)
	}
	if err != nil {
		return fmt.Errorf("could not create %q: %w", prefix, err)
	}

	seed, err := kp.Seed()
	if err != nil {
		return err
	}

	out := os.Stdout
	if c.outFile != "" {
		out, err = os.Create(c.outFile)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	_, err = fmt.Fprintln(out, string(seed))
	if err != nil {
		return err
	}

	if c.pubOut {
		pk, err := kp.PublicKey()
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(out, pk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *authNKCommand) preForType(keyType string) (nkeys.PrefixByte, error) {
	switch strings.ToLower(keyType) {
	case "user":
		return nkeys.PrefixByteUser, nil
	case "account":
		return nkeys.PrefixByteAccount, nil
	case "server":
		return nkeys.PrefixByteServer, nil
	case "cluster":
		return nkeys.PrefixByteCluster, nil
	case "operator":
		return nkeys.PrefixByteOperator, nil
	case "curve", "x25519":
		return nkeys.PrefixByteCurve, nil
	default:
		return nkeys.PrefixByte(0), fmt.Errorf("unknown prefix type %q", keyType)
	}
}

func (c *authNKCommand) signAction(_ *fisk.ParseContext) error {
	seed, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	sigBytes, err := kp.Sign(content)
	if err != nil {
		return err
	}

	fmt.Println(base64.RawURLEncoding.EncodeToString(sigBytes))

	return nil
}

func (c *authNKCommand) verifyAction(_ *fisk.ParseContext) error {
	var err error
	var kp nkeys.KeyPair

	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err = nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	sigEnc, err := os.ReadFile(c.signFile)
	if err != nil {
		return err
	}

	sig, err := base64.RawURLEncoding.DecodeString(string(sigEnc))
	if err != nil {
		return err
	}

	if err := kp.Verify(content, sig); err != nil {
		return err
	}

	fmt.Println("Verified OK")

	return nil
}

func (c *authNKCommand) sealAction(_ *fisk.ParseContext) error {
	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err := nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	if !nkeys.IsValidPublicCurveKey(c.counterpartKey) {
		return errors.New("invalid public key provided")
	}

	encryptedData, err := kp.Seal(content, c.counterpartKey)
	if err != nil {
		return err
	}

	if c.useB64 {
		encryptedData = []byte(base64.StdEncoding.EncodeToString(encryptedData))
	}

	if c.outFile == "" {
		fmt.Println(string(encryptedData))
		return nil
	}

	f, err := os.Create(c.outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(encryptedData)
	if err != nil {
		return err
	}

	return nil
}

func (c *authNKCommand) unsealAction(_ *fisk.ParseContext) error {
	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err := nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	if c.useB64 {
		var err error
		content, err = base64.StdEncoding.DecodeString(string(content))
		if err != nil {
			return err
		}
	}

	if !nkeys.IsValidPublicCurveKey(c.counterpartKey) {
		return errors.New("invalid public key provided")
	}

	decryptedData, err := kp.Open(content, c.counterpartKey)
	if err != nil {
		return err
	}

	if c.outFile == "" {
		fmt.Println(string(decryptedData))
		return nil
	}

	f, err := os.Create(c.outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(decryptedData)
	if err != nil {
		return err
	}

	return nil
}

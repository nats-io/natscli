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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nkeys"
)

func TestSealOpen(t *testing.T) {
	// Create two pairs of xkeys
	ef := rand.Reader
	p1, err := nkeys.CreateCurveKeysWithRand(ef)
	if err != nil {
		t.Error("Failed to create key")
		t.FailNow()
	}
	p2, err := nkeys.CreateCurveKeysWithRand(ef)
	if err != nil {
		t.Error("Failed to create key")
		t.FailNow()
	}
	p1_seed, _ := p1.Seed()
	p2_seed, _ := p2.Seed()

	// Setup all the test files
	p1_key, err := os.Create(filepath.Join(os.TempDir(), "p1_seed"))
	if err != nil {
		t.Error("Failed to create test key file")
		t.FailNow()
	}
	defer p1_key.Close()
	defer os.RemoveAll(filepath.Join(os.TempDir(), "p1_seed"))
	err = os.WriteFile(filepath.Join(os.TempDir(), "p1_seed"), p1_seed, 0644)
	if err != nil {
		t.Error("Failed to write test key to file")
		t.FailNow()
	}

	p2_key, err := os.Create(filepath.Join(os.TempDir(), "p2_seed"))
	if err != nil {
		t.Error("Failed to create test key file")
		t.FailNow()
	}
	defer p2_key.Close()
	defer os.RemoveAll(filepath.Join(os.TempDir(), "p2_seed"))
	err = os.WriteFile(filepath.Join(os.TempDir(), "p2_seed"), p2_seed, 0644)
	if err != nil {
		t.Error("Failed to write test key to file")
		t.FailNow()
	}

	message, err := os.Create(filepath.Join(os.TempDir(), "message.txt"))
	if err != nil {
		t.Error("Failed to create test key file")
		t.FailNow()
	}
	defer message.Close()
	defer os.RemoveAll(filepath.Join(os.TempDir(), "message.txt"))
	err = os.WriteFile(filepath.Join(os.TempDir(), "message.txt"), []byte("test"), 0644)
	if err != nil {
		t.Error("Failed to write test key to file")
		t.FailNow()
	}

	// Get both public keys
	p1_pub, _ := p1.PublicKey()
	p2_pub, _ := p2.PublicKey()

	// Setup fisk for Seal Test
	c := &authNKCommand{}
	c.b64out = false
	c.counterpartKey = p2_pub
	c.dataFile = filepath.Join(os.TempDir(), "message.txt")
	c.outFile = filepath.Join(os.TempDir(), "message.enc")
	c.keyFile = filepath.Join(os.TempDir(), "p1_seed")

	err = c.sealAction(nil)
	if err != nil {
		t.Error("Failed to seal message")
		t.FailNow()
	}

	// Setup fisk for Seal Test
	c = &authNKCommand{}
	c.counterpartKey = p1_pub
	c.dataFile = filepath.Join(os.TempDir(), "message.enc")
	c.keyFile = filepath.Join(os.TempDir(), "p2_seed")

	// Redirect stdout to capture decrypted output
	stdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = c.openAction(nil)
	if err != nil {
		t.Error("Failed to open message")
		t.FailNow()
	}

	w.Close()
	out, _ := io.ReadAll(r)
	os.Stdout = stdout

	// Test if decrypted output is correct
	if string(out) != "test\n" {
		t.Fail()
	}
}

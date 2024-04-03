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
	"testing"

	"github.com/nats-io/nkeys"
)

func TestSealOpen(t *testing.T) {
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

	msg := []byte("Hello World")

	p2_pub, err := p2.PublicKey()
	if err != nil {
		t.Error("Failed to get public key")
		t.FailNow()
	}

	enc_data, err := p1.Seal(msg, p2_pub)
	if err != nil || enc_data == nil {
		t.Error("Failed to seal data")
		t.FailNow()
	}

	p1_pub, err := p1.PublicKey()
	if err != nil {
		t.Error("Failed to get public key")
		t.FailNow()
	}
	dec_data, err := p2.Open(enc_data, p1_pub)
	if err != nil {
		t.Error("Failed to open data")
		t.FailNow()
	}

	if string(dec_data) != "Hello World" {
		t.Error("Decrypted data does not match original")
		t.FailNow()
	}
}

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

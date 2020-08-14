package natscontext_test

import (
	"os"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/nats/natscontext"
)

func TestContext(t *testing.T) {
	os.Setenv("XDG_CONFIG_HOME", "testdata")

	known := natscontext.KnownContexts()
	if len(known) != 2 && known[0] != "gotest" && known[1] != "other" {
		t.Fatalf("expected [gotest,other] got %#v", known)
	}

	selected := natscontext.SelectedContext()
	if selected != "gotest" {
		t.Fatalf("Expected gotest got %q", selected)
	}

	err := natscontext.SelectContext("other")
	if err != nil {
		t.Fatalf("could not select context: %s", err)
	}

	selected = natscontext.SelectedContext()
	if selected != "other" {
		t.Fatalf("Expected other got %q", selected)
	}

	err = natscontext.SelectContext("gotest")
	if err != nil {
		t.Fatalf("could not select context: %s", err)
	}

	c, err := natscontext.New("", false)
	if err != nil {
		t.Fatalf("could not create empty context: %s", err)
	}

	err = c.Save("not..valid")
	if err == nil {
		t.Fatalf("expected error loading context, received none")
	}

	err = c.Save("/aaaa")
	if err == nil {
		t.Fatalf("expected error loading context, received none")
	}

	// just take whats there
	config, err := natscontext.New("", true)
	if err != nil {
		t.Fatalf("error loading context: %s", err)
	}
	if config.ServerURL() != "demo.nats.io" {
		t.Fatalf("expected demo.nats got %s", config.ServerURL())
	}

	// support overrides
	config, err = natscontext.New("", true, natscontext.WithServerURL("connect.ngs.global"))
	if err != nil {
		t.Fatalf("error loading context: %s", err)
	}
	if config.ServerURL() != "connect.ngs.global" {
		t.Fatalf("expected ngs got %s", config.ServerURL())
	}

	// support missing config/context
	os.Setenv("XDG_CONFIG_HOME", "/nonexisting")
	config, err = natscontext.New("", true)
	if err != nil {
		t.Fatalf("error loading context: %s", err)
	}
	if config.ServerURL() != nats.DefaultURL {
		t.Fatalf("expected localhost got %s", config.ServerURL())
	}
}

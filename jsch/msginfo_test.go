package jsch_test

import (
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/jsch"
)

func TestParseJSMsgMetadata(t *testing.T) {
	i, err := jsch.ParseJSMsgMetadata(&nats.Msg{Reply: "$JS.ACK.ORDERS.NEW.1.2.3"})
	checkErr(t, err, "msg parse failed")

	if i.Stream() != "ORDERS" {
		t.Fatalf("expected ORDERS got %s", i.Stream())
	}

	if i.Consumer() != "NEW" {
		t.Fatalf("expected NEW got %s", i.Consumer())
	}

	if i.Delivered() != 1 {
		t.Fatalf("expceted 1 got %d", i.Delivered())
	}

	if i.StreamSequence() != 2 {
		t.Fatalf("expceted 2 got %d", i.StreamSequence())
	}

	if i.ConsumerSequence() != 3 {
		t.Fatalf("expceted 3 got %d", i.ConsumerSequence())
	}
}

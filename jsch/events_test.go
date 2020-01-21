package jsch_test

import (
	"reflect"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nats-io/jetstream/jsch"
)

func TestSchemaForEvent(t *testing.T) {
	s, err := jsch.SchemaForEvent([]byte(`{"schema":"io.nats.jetstream.metric.v1.consumer_ack"}`))
	checkErr(t, err, "schema extract failed")

	if s != "io.nats.jetstream.metric.v1.consumer_ack" {
		t.Fatalf("expected io.nats.jetstream.metric.v1.consumer_ack got %s", s)
	}

	s, err = jsch.SchemaForEvent([]byte(`{}`))
	checkErr(t, err, "schema extract failed")

	if s != "io.nats.unknown_event" {
		t.Fatalf("expected io.nats.unknown_event got %s", s)
	}
}

func TestParseEvent(t *testing.T) {
	s, e, err := jsch.ParseEvent([]byte(`{"schema":"io.nats.jetstream.metric.v1.consumer_ack"}`))
	checkErr(t, err, "schema parse failed")

	if s != "io.nats.jetstream.metric.v1.consumer_ack" {
		t.Fatalf("expected io.nats.jetstream.metric.v1.consumer_ack got %s", s)
	}

	_, ok := e.(*server.ConsumerAckMetric)
	if !ok {
		t.Fatalf("expected ConsumerAckMetric got %v", reflect.TypeOf(e))
	}
}

package jsch_test

import (
	"reflect"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nats-io/jetstream/internal/jsch"
)

func TestSchemaForEvent(t *testing.T) {
	s, err := jsch.SchemaTokenForEvent([]byte(`{"schema":"io.nats.jetstream.metric.v1.consumer_ack"}`))
	checkErr(t, err, "schema extract failed")

	if s != "io.nats.jetstream.metric.v1.consumer_ack" {
		t.Fatalf("expected io.nats.jetstream.metric.v1.consumer_ack got %s", s)
	}

	s, err = jsch.SchemaTokenForEvent([]byte(`{}`))
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

func TestSchemaURLForToken(t *testing.T) {
	jsch.SchemasRepo = "https://nats.io/schemas"

	a, u, err := jsch.SchemaURLForToken("io.nats.jetstream.metric.v1.consumer_ack")
	checkErr(t, err, "parse failed")

	if a != "https://nats.io/schemas/jetstream/metric/v1/consumer_ack.json" {
		t.Fatalf("expected https://nats.io/schemas/jetstream/metric/v1/consumer_ack.json got %q", a)
	}

	if u.Host != "nats.io" || u.Scheme != "https" || u.Path != "/schemas/jetstream/metric/v1/consumer_ack.json" {
		t.Fatalf("invalid url: %v", u.String())
	}

	_, _, err = jsch.SchemaURLForToken("jetstream.metric.v1.consumer_ack")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestSchemaURLForEvent(t *testing.T) {
	jsch.SchemasRepo = "https://nats.io/schemas"

	a, u, err := jsch.SchemaURLForEvent([]byte(`{"schema":"io.nats.jetstream.metric.v1.consumer_ack"}`))
	checkErr(t, err, "parse failed")

	if a != "https://nats.io/schemas/jetstream/metric/v1/consumer_ack.json" {
		t.Fatalf("expected . got %q", a)
	}

	if u.Host != "nats.io" || u.Scheme != "https" || u.Path != "/schemas/jetstream/metric/v1/consumer_ack.json" {
		t.Fatalf("invalid url: %v", u.String())
	}
}

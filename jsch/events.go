package jsch

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
)

// SchemasRepo is the repository holding NATS Schemas
var SchemasRepo = "https://nats.io/schemas"

type schemaDetector struct {
	Schema string `json:"schema"`
}

var schemaTypes = map[string]func() interface{}{
	"io.nats.jetstream.metric.v1.consumer_ack":  func() interface{} { return &server.ConsumerAckMetric{} },
	"io.nats.jetstream.advisory.v1.max_deliver": func() interface{} { return &server.ConsumerDeliveryExceededAdvisory{} },
	"io.nats.jetstream.advisory.v1.api_audit":   func() interface{} { return &server.JetStreamAPIAudit{} },
	"io.nats.unknown_event":                     func() interface{} { return &UnknownEvent{} },
}

// UnknownEvent is a type returned when parsing an unknown type of event
type UnknownEvent = map[string]interface{}

// SchemaTokenForEvent retrieves the schema token from an event
func SchemaTokenForEvent(e []byte) (schema string, err error) {
	sd := &schemaDetector{}
	err = json.Unmarshal(e, sd)
	if err != nil {
		return "", err
	}

	if sd.Schema == "" {
		sd.Schema = "io.nats.unknown_event"
	}

	return sd.Schema, nil
}

// SchemaURLForEvent parses event e and determines a http address for the JSON schema describing it
func SchemaURLForEvent(e []byte) (address string, url *url.URL, err error) {
	schema, err := SchemaTokenForEvent(e)
	if err != nil {
		return "", nil, err
	}

	return SchemaURLForToken(schema)
}

// SchemaURLForToken determines the path to the JSON Schema document describing an event given a token like io.nats.jetstream.metric.v1.consumer_ack
func SchemaURLForToken(schema string) (address string, url *url.URL, err error) {
	if !strings.HasPrefix(schema, "io.nats.") {
		return "", nil, fmt.Errorf("unsupported schema %q", schema)
	}

	token := strings.TrimPrefix(schema, "io.nats.")
	address = fmt.Sprintf("%s/%s.json", SchemasRepo, strings.ReplaceAll(token, ".", "/"))
	url, err = url.Parse(address)

	return address, url, err
}

// ParseEvent parses event e and returns event as for example *server.ConsumerAckMetric, all unknown
// event schemas will be of type *UnknownEvent
func ParseEvent(e []byte) (schema string, event interface{}, err error) {
	schema, err = SchemaTokenForEvent(e)
	if err != nil {
		return "", nil, err
	}

	gf, ok := schemaTypes[schema]
	if !ok {
		gf = schemaTypes["io.nats.unknown_event"]
	}

	event = gf()
	err = json.Unmarshal(e, event)

	return schema, event, err
}

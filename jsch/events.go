package jsch

import (
	"encoding/json"

	"github.com/nats-io/nats-server/v2/server"
)

type schemeDetector struct {
	Schema string `json:"schema"`
}

var schemaTypes = map[string]func() interface{}{
	"io.nats.jetstream.metric.v1.consumer_ack":  func() interface{} { return &server.ConsumerAckMetric{} },
	"io.nats.jetstream.advisory.v1.max_deliver": func() interface{} { return &server.ConsumerDeliveryExceededAdvisory{} },
	"io.nats.unknown_event":                     func() interface{} { return &UnknownEvent{} },
}

// UnknownEvent is a type returned when parsing an unknown type of event
type UnknownEvent = map[string]interface{}

// SchemaForEvent retrieves the schema token from an event
func SchemaForEvent(e []byte) (schema string, err error) {
	sd := &schemeDetector{}
	err = json.Unmarshal(e, sd)
	if err != nil {
		return "", err
	}

	if sd.Schema == "" {
		sd.Schema = "io.nats.unknown_event"
	}

	return sd.Schema, nil
}

// ParseEvent parses event e and returns event as for example *server.ConsumerAckMetric, all unknown
// event schemas will be of type *UnknownEvent
func ParseEvent(e []byte) (schema string, event interface{}, err error) {
	schema, err = SchemaForEvent(e)
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

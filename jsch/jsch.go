// Copyright 2020 The NATS Authors
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

// Package jsch provides client helpers for interacting with NATS
// JetStream
//
// This is a helper library for managing and interacting with JetStream
// we are exploring a few options for how such a library will look and
// determine what will go into core NATS clients and what will be left
// as an external library.
//
// This this library is not an official blessed way for interacting with
// JetStream yet but as a set of examples of all the capabilities this
// is valuable and it's for us a starting point to learning what patterns
// work well
package jsch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Timeout used when performing JetStream requests
var Timeout = 5 * time.Second

var nc *nats.Conn

// SetConnection sets the connection used to perform requests
func SetConnection(c *nats.Conn) {
	nc = c
}

// IsJetStreamEnabled determines if JetStream is enabled for the current account
func IsJetStreamEnabled() bool {
	_, err := nc.Request(server.JetStreamEnabled, nil, Timeout)
	return err == nil
}

// IsErrorResponse checks if the message holds a standard JetStream error
func IsErrorResponse(m *nats.Msg) bool {
	return strings.HasPrefix(string(m.Data), server.ErrPrefix)
}

// IsOKResponse checks if the message holds a standard JetStream error
func IsOKResponse(m *nats.Msg) bool {
	return strings.HasPrefix(string(m.Data), server.OK)
}

// IsKnownStream determines if a Stream is known
func IsKnownStream(stream string) (bool, error) {
	streams, err := StreamNames()
	if err != nil {
		return false, err
	}

	for _, s := range streams {
		if s == stream {
			return true, nil
		}
	}

	return false, nil
}

// IsKnownConsumer determines if a Consumer is known for a specific Stream
func IsKnownConsumer(stream string, consumer string) (bool, error) {
	consumers, err := ConsumerNames(stream)
	if err != nil {
		return false, err
	}

	for _, c := range consumers {
		if c == consumer {
			return true, nil
		}
	}

	return false, nil
}

// JetStreamAccountInfo retrieves information about the current account limits and more
func JetStreamAccountInfo() (info server.JetStreamAccountStats, err error) {
	response, err := nc.Request(server.JetStreamInfo, nil, Timeout)
	if err != nil {
		return info, err
	}

	if IsErrorResponse(response) {
		return info, fmt.Errorf("%s", string(response.Data))
	}

	err = json.Unmarshal(response.Data, &info)
	if err != nil {
		return info, err
	}

	return info, nil
}

// StreamNames is a sorted list of all known Streams
func StreamNames() (streams []string, err error) {
	streams = []string{}

	response, err := nc.Request(server.JetStreamListStreams, nil, Timeout)
	if err != nil {
		return streams, err
	}

	if IsErrorResponse(response) {
		return streams, fmt.Errorf("%s", string(response.Data))
	}

	err = json.Unmarshal(response.Data, &streams)
	if err != nil {
		return streams, err
	}

	sort.Strings(streams)

	return streams, nil
}

// ConsumerNames is a sorted list of all known Consumers within a Stream
func ConsumerNames(stream string) (consumers []string, err error) {
	consumers = []string{}

	response, err := nc.Request(fmt.Sprintf(server.JetStreamConsumersT, stream), nil, Timeout)
	if err != nil {
		return consumers, err
	}

	if IsErrorResponse(response) {
		return consumers, fmt.Errorf("%s", string(response.Data))
	}

	err = json.Unmarshal(response.Data, &consumers)
	if err != nil {
		return consumers, err
	}

	sort.Strings(consumers)

	return consumers, nil
}

// EachStream iterates over all known Streams
func EachStream(cb func(*Stream)) (err error) {
	names, err := StreamNames()
	if err != nil {
		return err
	}

	for _, s := range names {
		stream, err := LoadStream(s)
		if err != nil {
			return err
		}

		cb(stream)
	}

	return nil
}

// Subscribe subscribes a consumer, creating it if not present from a template configuration modified by opts.  Stream should exist. See nats.Subscribe
//
// TODO: I dont really think this kind of thing is a good idea, but its awfully verbose without it so I suspect we will need to cater for this
func Subscribe(stream string, consumer string, cb func(*nats.Msg), template server.ConsumerConfig, opts ...ConsumerOption) (*nats.Subscription, error) {
	c, err := LoadOrNewConsumerFromTemplate(stream, consumer, template, opts...)
	if err != nil {
		return nil, err
	}

	return c.Subscribe(cb)
}

// Flush flushes the underlying NATS connection
func Flush() error {
	return nc.Flush()
}

func NATSConn() *nats.Conn {
	return nc
}

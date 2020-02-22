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
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var timeout = 5 * time.Second
var nc *nats.Conn
var mu sync.Mutex

// Connect connects to NATS and configures it to use the connection in future interaction
func Connect(servers string, opts ...nats.Option) (err error) {
	mu.Lock()
	defer mu.Unlock()

	nc, err = nats.Connect(servers, opts...)

	return err
}

// SetTimeout sets the timeout for requests to JetStream
func SetTimeout(t time.Duration) {
	mu.Lock()
	defer mu.Unlock()

	timeout = t
}

// SetConnection sets the connection used to perform requests
func SetConnection(c *nats.Conn) {
	mu.Lock()
	defer mu.Unlock()

	nc = c
}

// IsJetStreamEnabled determines if JetStream is enabled for the current account
func IsJetStreamEnabled() bool {
	_, err := nrequest(server.JetStreamEnabled, nil, timeout)
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

// IsKnownStreamTemplate determines if a StreamTemplate is known
func IsKnownStreamTemplate(template string) (bool, error) {
	templates, err := StreamTemplateNames()
	if err != nil {
		return false, err
	}

	for _, s := range templates {
		if s == template {
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
	response, err := nrequest(server.JetStreamInfo, nil, timeout)
	if err != nil {
		return info, err
	}

	if IsErrorResponse(response) {
		return info, fmt.Errorf(string(response.Data))
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

	response, err := nrequest(server.JetStreamListStreams, nil, timeout)
	if err != nil {
		return streams, err
	}

	if IsErrorResponse(response) {
		return streams, fmt.Errorf(string(response.Data))
	}

	err = json.Unmarshal(response.Data, &streams)
	if err != nil {
		return streams, err
	}

	sort.Strings(streams)

	return streams, nil
}

// StreamTemplateNames is a sorted list of all known StreamTemplates
func StreamTemplateNames() (templates []string, err error) {
	templates = []string{}

	response, err := nrequest(server.JetStreamListTemplates, nil, timeout)
	if err != nil {
		return templates, err
	}

	if IsErrorResponse(response) {
		return templates, fmt.Errorf(string(response.Data))
	}

	err = json.Unmarshal(response.Data, &templates)
	if err != nil {
		return templates, err
	}

	sort.Strings(templates)

	return templates, nil
}

// ConsumerNames is a sorted list of all known Consumers within a Stream
func ConsumerNames(stream string) (consumers []string, err error) {
	consumers = []string{}

	response, err := nrequest(fmt.Sprintf(server.JetStreamConsumersT, stream), nil, timeout)
	if err != nil {
		return consumers, err
	}

	if IsErrorResponse(response) {
		return consumers, fmt.Errorf(string(response.Data))
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

// EachStreamTemplate iterates over all known Stream Templates
func EachStreamTemplate(cb func(*StreamTemplate)) (err error) {
	names, err := StreamTemplateNames()
	if err != nil {
		return err
	}

	for _, t := range names {
		template, err := LoadStreamTemplate(t)
		if err != nil {
			return err
		}

		cb(template)
	}

	return nil
}

// Flush flushes the underlying NATS connection
func Flush() error {
	nc := nconn()

	if nc == nil {
		return fmt.Errorf("nats connection is not set, use SetConnection()")
	}

	return nc.Flush()
}

func nconn() *nats.Conn {
	mu.Lock()
	defer mu.Unlock()

	return nc
}

func nrequest(subj string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	nc := nconn()
	if nc == nil {
		return nil, fmt.Errorf("nats connection is not set, use SetConnection()")
	}

	return nc.Request(subj, data, timeout)
}

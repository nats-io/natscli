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

package jsch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// DefaultStream is a template configuration with StreamPolicy retention and 1 years maximum age. No storage type or subjects are set
var DefaultStream = server.StreamConfig{
	Retention:    server.LimitsPolicy,
	MaxConsumers: -1,
	MaxMsgs:      -1,
	MaxBytes:     -1,
	MaxAge:       24 * 365 * time.Hour,
	MaxMsgSize:   -1,
	Replicas:     1,
	NoAck:        false,
}

// DefaultWorkQueue is a template configuration with WorkQueuePolicy retention and 1 years maximum age. No storage type or subjects are set
var DefaultWorkQueue = server.StreamConfig{
	Retention:    server.WorkQueuePolicy,
	MaxConsumers: -1,
	MaxMsgs:      -1,
	MaxBytes:     -1,
	MaxAge:       24 * 365 * time.Hour,
	MaxMsgSize:   -1,
	Replicas:     1,
	NoAck:        false,
}

// DefaultStreamConfiguration is the configuration that will be used to create new Streams in NewStream
var DefaultStreamConfiguration = DefaultStream

// StreamOption configures a stream
type StreamOption func(o *server.StreamConfig) error

// Stream represents a JetStream Stream
type Stream struct {
	cfg server.StreamConfig
}

// NewStreamFromTemplate creates a new stream based on a supplied template and options
func NewStreamFromTemplate(name string, template server.StreamConfig, opts ...StreamOption) (stream *Stream, err error) {
	cfg, err := NewStreamConfiguration(template, opts...)
	if err != nil {
		return nil, err
	}

	cfg.Name = name

	jreq, err := json.Marshal(&cfg)
	if err != nil {
		return nil, err
	}

	response, err := nc.Request(server.JetStreamCreateStream, jreq, Timeout)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(string(response.Data), server.OK) {
		return nil, fmt.Errorf("%s", string(response.Data))
	}

	return LoadStream(name)
}

// LoadOrNewStreamFromTemplate loads an existing stream or creates a new one matching opts and template
func LoadOrNewStreamFromTemplate(name string, template server.StreamConfig, opts ...StreamOption) (stream *Stream, err error) {
	s, err := LoadStream(name)
	if s == nil || err != nil {
		return NewStreamFromTemplate(name, template, opts...)
	}

	return s, err
}

// NewStream creates a new stream using DefaultStream as a starting template allowing adjustments to be made using options
func NewStream(name string, opts ...StreamOption) (stream *Stream, err error) {
	return NewStreamFromTemplate(name, DefaultStream, opts...)
}

// LoadOrNewStreamFromTemplate loads an existing stream or creates a new one matching opts
func LoadOrNewStream(name string, opts ...StreamOption) (stream *Stream, err error) {
	return LoadOrNewStreamFromTemplate(name, DefaultStream, opts...)
}

// LoadStream loads a stream by name
func LoadStream(name string) (stream *Stream, err error) {
	stream = &Stream{
		cfg: server.StreamConfig{
			Name: name,
		},
	}

	err = loadConfigForStream(stream)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

// NewStreamConfiguration generates a new configuration based on template modified by opts
func NewStreamConfiguration(template server.StreamConfig, opts ...StreamOption) (server.StreamConfig, error) {
	for _, o := range opts {
		err := o(&template)
		if err != nil {
			return template, err
		}
	}

	return template, nil
}

func loadConfigForStream(stream *Stream) (err error) {
	info, err := loadStreamInfo(stream.cfg.Name)
	if err != nil {
		return err
	}

	stream.cfg = info.Config

	return nil
}

func loadStreamInfo(stream string) (info *server.StreamInfo, err error) {
	response, err := nc.Request(server.JetStreamStreamInfo, []byte(stream), Timeout)
	if err != nil {
		return nil, err
	}

	if IsErrorResponse(response) {
		return nil, fmt.Errorf("%s", string(response.Data))
	}

	info = &server.StreamInfo{}
	err = json.Unmarshal(response.Data, info)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func Subjects(s ...string) StreamOption {
	return func(o *server.StreamConfig) error {
		o.Subjects = s
		return nil
	}
}

func LimitsRetention() StreamOption {
	return func(o *server.StreamConfig) error {
		o.Retention = server.LimitsPolicy
		return nil
	}
}

func InterestRetention() StreamOption {
	return func(o *server.StreamConfig) error {
		o.Retention = server.InterestPolicy
		return nil
	}
}

func WorkQueueRetention() StreamOption {
	return func(o *server.StreamConfig) error {
		o.Retention = server.WorkQueuePolicy
		return nil
	}
}

func MaxConsumers(m int) StreamOption {
	return func(o *server.StreamConfig) error {
		o.MaxConsumers = m
		return nil
	}
}

func MaxMessages(m int64) StreamOption {
	return func(o *server.StreamConfig) error {
		o.MaxMsgs = m
		return nil
	}
}

func MaxBytes(m int64) StreamOption {
	return func(o *server.StreamConfig) error {
		o.MaxBytes = m
		return nil
	}
}

func MaxAge(m time.Duration) StreamOption {
	return func(o *server.StreamConfig) error {
		o.MaxAge = m
		return nil
	}
}

func MaxMessageSize(m int32) StreamOption {
	return func(o *server.StreamConfig) error {
		o.MaxMsgSize = m
		return nil
	}
}

func FileStorage() StreamOption {
	return func(o *server.StreamConfig) error {
		o.Storage = server.FileStorage
		return nil
	}
}

func MemoryStorage() StreamOption {
	return func(o *server.StreamConfig) error {
		o.Storage = server.MemoryStorage
		return nil
	}
}

func Replicas(r int) StreamOption {
	return func(o *server.StreamConfig) error {
		o.Replicas = r
		return nil
	}
}

func NoAck() StreamOption {
	return func(o *server.StreamConfig) error {
		o.NoAck = true
		return nil
	}
}

// Reset reloads the Stream configuration from the JetStream server
func (s *Stream) Reset() error {
	return loadConfigForStream(s)
}

// LoadConsumer loads a named consumer related to this Stream
func (s *Stream) LoadConsumer(name string) (*Consumer, error) {
	return LoadConsumer(s.cfg.Name, name)
}

// NewConsumer creates a new consumer in this Stream based on DefaultConsumer
func (s *Stream) NewConsumer(opts ...ConsumerOption) (consumer *Consumer, err error) {
	return NewConsumer(s.Name(), opts...)
}

// LoadOrNewConsumer loads or creates a consumer based on these options
func (s *Stream) LoadOrNewConsumer(name string, opts ...ConsumerOption) (consumer *Consumer, err error) {
	return LoadOrNewConsumer(s.Name(), name, opts...)
}

// NewConsumerFromTemplate creates a new consumer in this Stream based on a supplied template config
func (s *Stream) NewConsumerFromTemplate(name string, template server.ConsumerConfig, opts ...ConsumerOption) (consumer *Consumer, err error) {
	return NewConsumerFromTemplate(s.Name(), template, opts...)
}

// LoadOrNewConsumer loads or creates a consumer based on these options that adjust supplied template
func (s *Stream) LoadOrNewConsumerFromTemplate(name string, template server.ConsumerConfig, opts ...ConsumerOption) (consumer *Consumer, err error) {
	return LoadOrNewConsumerFromTemplate(s.Name(), name, template, opts...)
}

// ConsumerNames is a list of all known consumers for this Stream
func (s *Stream) ConsumerNames() (names []string, err error) {
	response, err := nc.Request(server.JetStreamConsumers, []byte(s.cfg.Name), Timeout)
	if err != nil {
		return names, err
	}

	if IsErrorResponse(response) {
		return names, fmt.Errorf("%s", string(response.Data))
	}

	err = json.Unmarshal(response.Data, &names)
	if err != nil {
		return names, err
	}

	sort.Strings(names)

	return names, nil
}

func (s *Stream) Information() (info *server.StreamInfo, err error) {
	return loadStreamInfo(s.Name())
}

// State retrieves the Stream State
func (s *Stream) State() (stats server.StreamState, err error) {
	info, err := loadStreamInfo(s.Name())
	if err != nil {
		return stats, err
	}

	return info.State, nil
}

// Delete deletes the Stream, after this the Stream object should be disposed
func (s *Stream) Delete() error {
	response, err := nc.Request(server.JetStreamDeleteStream, []byte(s.Name()), Timeout)
	if err != nil {
		return err
	}

	if IsErrorResponse(response) {
		return fmt.Errorf("%s", string(response.Data))
	}

	return nil
}

// Purge deletes all messages from the Stream
func (s *Stream) Purge() error {
	response, err := nc.Request(server.JetStreamPurgeStream, []byte(s.Name()), Timeout)
	if err != nil {
		return err
	}

	if IsErrorResponse(response) {
		return fmt.Errorf("%s", string(response.Data))
	}

	return nil
}

// LoadMessage loads a message from the message set by its sequence number
func (s *Stream) LoadMessage(seq int) (msg server.StoredMsg, err error) {
	response, err := nc.Request(server.JetStreamMsgBySeqPre+"."+s.Name(), []byte(strconv.Itoa(seq)), Timeout)
	if err != nil {
		return server.StoredMsg{}, err
	}

	if IsErrorResponse(response) {
		return server.StoredMsg{}, fmt.Errorf("%s", string(response.Data))
	}

	msg = server.StoredMsg{}
	err = json.Unmarshal(response.Data, &msg)
	if err != nil {
		return server.StoredMsg{}, err
	}

	return msg, nil
}

// DeleteMessage deletes a specific message from the Stream
func (s *Stream) DeleteMessage(seq int) (err error) {
	response, err := nc.Request(server.JetStreamDeleteMsg, []byte(s.Name()+" "+strconv.Itoa(seq)), Timeout)
	if err != nil {
		return err
	}

	if IsErrorResponse(response) {
		return fmt.Errorf("%s", string(response.Data))
	}

	return nil
}

func (s *Stream) Configuration() server.StreamConfig { return s.cfg }
func (s *Stream) Name() string                       { return s.cfg.Name }
func (s *Stream) Subjects() []string                 { return s.cfg.Subjects }
func (s *Stream) Retention() server.RetentionPolicy  { return s.cfg.Retention }
func (s *Stream) MaxConsumers() int                  { return s.cfg.MaxConsumers }
func (s *Stream) MaxMsgs() int64                     { return s.cfg.MaxMsgs }
func (s *Stream) MaxBytes() int64                    { return s.cfg.MaxBytes }
func (s *Stream) MaxAge() time.Duration              { return s.cfg.MaxAge }
func (s *Stream) MaxMsgSize() int32                  { return s.cfg.MaxMsgSize }
func (s *Stream) Storage() server.StorageType        { return s.cfg.Storage }
func (s *Stream) Replicas() int                      { return s.cfg.Replicas }
func (s *Stream) NoAck() bool                        { return s.cfg.NoAck }

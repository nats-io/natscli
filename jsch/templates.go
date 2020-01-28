package jsch

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
)

type StreamTemplate struct {
	cfg     server.StreamTemplateConfig
	streams []string
}

// NewStreamTemplate creates a new template
func NewStreamTemplate(name string, maxStreams uint32, config server.StreamConfig, opts ...StreamOption) (template *StreamTemplate, err error) {
	cfg, err := NewStreamConfiguration(config, opts...)
	if err != nil {
		return nil, err
	}

	tc := server.StreamTemplateConfig{
		Name:       name,
		Config:     &cfg,
		MaxStreams: maxStreams,
	}

	jreq, err := json.Marshal(&tc)
	if err != nil {
		return nil, err
	}

	response, err := nc.Request(fmt.Sprintf(server.JetStreamCreateTemplateT, name), jreq, Timeout)
	if err != nil {
		return nil, err
	}

	if IsErrorResponse(response) {
		return nil, fmt.Errorf("%s", string(response.Data))
	}

	return LoadStreamTemplate(name)
}

// LoadOrNewStreamTemplate loads an existing template, else creates a new one based on config
func LoadOrNewStreamTemplate(name string, maxStreams uint32, config server.StreamConfig, opts ...StreamOption) (template *StreamTemplate, err error) {
	template, err = LoadStreamTemplate(name)
	if template != nil && err == nil {
		return template, nil
	}

	return NewStreamTemplate(name, maxStreams, config, opts...)
}

// LoadStreamTemplate loads a given stream template from JetStream
func LoadStreamTemplate(name string) (template *StreamTemplate, err error) {
	response, err := nc.Request(fmt.Sprintf(server.JetStreamTemplateInfoT, name), nil, Timeout)
	if err != nil {
		return nil, err
	}

	if IsErrorResponse(response) {
		return nil, fmt.Errorf("%s", string(response.Data))
	}

	info := server.StreamTemplateInfo{}
	err = json.Unmarshal(response.Data, &info)
	if err != nil {
		return nil, err
	}

	template = &StreamTemplate{
		cfg:     *info.Config,
		streams: info.Streams,
	}

	return template, nil
}

// Delete deletes the StreamTemplate, after this the StreamTemplate object should be disposed
func (t *StreamTemplate) Delete() error {
	response, err := nc.Request(fmt.Sprintf(server.JetStreamDeleteTemplateT, t.Name()), nil, Timeout)
	if err != nil {
		return err
	}

	if IsErrorResponse(response) {
		return fmt.Errorf("%s", string(response.Data))
	}

	return nil
}

func (t *StreamTemplate) Configuration() server.StreamTemplateConfig { return t.cfg }
func (t *StreamTemplate) StreamConfiguration() server.StreamConfig   { return *t.cfg.Config }
func (t *StreamTemplate) Name() string                               { return t.cfg.Name }
func (t *StreamTemplate) MaxStreams() uint32                         { return t.cfg.MaxStreams }
func (t *StreamTemplate) Streams() []string                          { return t.streams }

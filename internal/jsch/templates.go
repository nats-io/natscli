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

	_, err = nrequest(fmt.Sprintf(server.JetStreamCreateTemplateT, name), jreq, timeout)
	if err != nil {
		return nil, err
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
	template = &StreamTemplate{cfg: server.StreamTemplateConfig{Name: name}}
	err = loadConfigForStreamTemplate(template)
	if err != nil {
		return nil, err
	}

	return template, nil
}

func loadConfigForStreamTemplate(template *StreamTemplate) (err error) {
	response, err := nrequest(fmt.Sprintf(server.JetStreamTemplateInfoT, template.Name()), nil, timeout)
	if err != nil {
		return err
	}

	info := server.StreamTemplateInfo{}
	err = json.Unmarshal(response.Data, &info)
	if err != nil {
		return err
	}

	template.cfg = *info.Config
	template.streams = info.Streams

	return nil
}

// Delete deletes the StreamTemplate, after this the StreamTemplate object should be disposed
func (t *StreamTemplate) Delete() error {
	_, err := nrequest(fmt.Sprintf(server.JetStreamDeleteTemplateT, t.Name()), nil, timeout)
	if err != nil {
		return err
	}

	return nil
}

// Reset reloads the Stream Template configuration and state from the JetStream server
func (t *StreamTemplate) Reset() error {
	return loadConfigForStreamTemplate(t)
}

func (t *StreamTemplate) Configuration() server.StreamTemplateConfig { return t.cfg }
func (t *StreamTemplate) StreamConfiguration() server.StreamConfig   { return *t.cfg.Config }
func (t *StreamTemplate) Name() string                               { return t.cfg.Name }
func (t *StreamTemplate) MaxStreams() uint32                         { return t.cfg.MaxStreams }
func (t *StreamTemplate) Streams() []string                          { return t.streams }

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

package main

import (
	"testing"
	"time"

	"github.com/nats-io/jsm.go/api"
)

type validator interface {
	Validate(...api.StructValidator) (bool, []string)
}

func validateExpectSuccess(t *testing.T, cfg validator) {
	t.Helper()

	ok, errs := cfg.Validate(new(SchemaValidator))
	if !ok {
		t.Fatalf("expected success but got: %v", errs)
	}
}

func validateExpectFailure(t *testing.T, cfg validator) {
	t.Helper()

	ok, errs := cfg.Validate(new(SchemaValidator))
	if ok {
		t.Fatalf("expected success but got: %v", errs)
	}
}

func TestStreamConfiguration(t *testing.T) {
	reset := func() api.StreamConfig {
		return api.StreamConfig{
			Name:         "BASIC",
			Subjects:     []string{"BASIC"},
			Retention:    api.LimitsPolicy,
			MaxConsumers: -1,
			MaxAge:       0,
			MaxBytes:     -1,
			MaxMsgs:      -1,
			Storage:      api.FileStorage,
			Replicas:     1,
		}
	}

	cfg := reset()
	validateExpectSuccess(t, cfg)

	// invalid names
	cfg = reset()
	cfg.Name = "X.X"
	validateExpectFailure(t, cfg)

	// valid subject
	cfg = reset()
	cfg.Subjects = []string{"bob"}
	validateExpectSuccess(t, cfg)

	// invalid retention
	cfg.Retention = 10
	validateExpectFailure(t, cfg)

	// max consumers >= -1
	cfg = reset()
	cfg.MaxConsumers = -2
	validateExpectFailure(t, cfg)
	cfg.MaxConsumers = 10
	validateExpectSuccess(t, cfg)

	// max messages >= -1
	cfg = reset()
	cfg.MaxMsgs = -2
	validateExpectFailure(t, cfg)
	cfg.MaxMsgs = 10
	validateExpectSuccess(t, cfg)

	// max bytes >= -1
	cfg = reset()
	cfg.MaxBytes = -2
	validateExpectFailure(t, cfg)
	cfg.MaxBytes = 10
	validateExpectSuccess(t, cfg)

	// max age >= 0
	cfg = reset()
	cfg.MaxAge = -1
	validateExpectFailure(t, cfg)
	cfg.MaxAge = time.Second
	validateExpectSuccess(t, cfg)

	// max msg size >= -1
	cfg = reset()
	cfg.MaxMsgSize = -2
	validateExpectFailure(t, cfg)
	cfg.MaxMsgSize = 10
	validateExpectSuccess(t, cfg)

	// storage is valid
	cfg = reset()
	cfg.Storage = 10
	validateExpectFailure(t, cfg)

	// num replicas > 0
	cfg = reset()
	cfg.Replicas = -1
	validateExpectFailure(t, cfg)
	cfg.Replicas = 0
	validateExpectFailure(t, cfg)
}

func TestStreamTemplateConfiguration(t *testing.T) {
	reset := func() api.StreamTemplateConfig {
		return api.StreamTemplateConfig{
			Name:       "BASIC_T",
			MaxStreams: 10,
			Config: &api.StreamConfig{
				Name:         "BASIC",
				Subjects:     []string{"BASIC"},
				Retention:    api.LimitsPolicy,
				MaxConsumers: -1,
				MaxAge:       0,
				MaxBytes:     -1,
				MaxMsgs:      -1,
				Storage:      api.FileStorage,
				Replicas:     1,
			},
		}
	}

	cfg := reset()
	validateExpectSuccess(t, cfg)

	cfg.Name = ""
	validateExpectFailure(t, cfg)

	// should also validate config
	cfg = reset()
	cfg.Config.Storage = 10
	validateExpectFailure(t, cfg)

	// unlimited managed streams
	cfg = reset()
	cfg.MaxStreams = 0
	validateExpectSuccess(t, cfg)
}

func TestConsumerConfiguration(t *testing.T) {
	reset := func() api.ConsumerConfig {
		return api.ConsumerConfig{
			DeliverPolicy: api.DeliverAll,
			AckPolicy:     api.AckExplicit,
			ReplayPolicy:  api.ReplayInstant,
		}
	}

	cfg := reset()
	validateExpectSuccess(t, cfg)

	// durable name
	cfg = reset()
	cfg.Durable = "bob.bob"
	validateExpectFailure(t, cfg)

	// last policy
	cfg = reset()
	cfg.DeliverPolicy = api.DeliverLast
	validateExpectSuccess(t, cfg)

	// new policy
	cfg = reset()
	cfg.DeliverPolicy = api.DeliverNew
	validateExpectSuccess(t, cfg)

	// start sequence policy
	cfg = reset()
	cfg.DeliverPolicy = api.DeliverByStartSequence
	cfg.OptStartSeq = 10
	validateExpectSuccess(t, cfg)
	cfg.OptStartSeq = 0
	validateExpectFailure(t, cfg)

	// start time policy
	cfg = reset()
	ts := time.Now()
	cfg.DeliverPolicy = api.DeliverByStartTime
	cfg.OptStartTime = &ts
	validateExpectSuccess(t, cfg)
	cfg.OptStartTime = nil
	validateExpectFailure(t, cfg)

	// ack policy
	cfg = reset()
	cfg.AckPolicy = 10
	validateExpectFailure(t, cfg)
	cfg.AckPolicy = api.AckExplicit
	validateExpectSuccess(t, cfg)
	cfg.AckPolicy = api.AckAll
	validateExpectSuccess(t, cfg)
	cfg.AckPolicy = api.AckNone
	validateExpectSuccess(t, cfg)

	// replay policy
	cfg = reset()
	cfg.ReplayPolicy = 10
	validateExpectFailure(t, cfg)
	cfg.ReplayPolicy = api.ReplayInstant
	validateExpectSuccess(t, cfg)
	cfg.ReplayPolicy = api.ReplayOriginal
	validateExpectSuccess(t, cfg)
}

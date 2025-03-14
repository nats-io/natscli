// Copyright 2020-2025 The NATS Authors
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

package exporter

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ghodss/yaml"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/monitor"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/prometheus/client_golang/prometheus"
)

type Check struct {
	Name       string          `json:"name" yaml:"name"`
	Kind       string          `json:"kind" yaml:"kind"`
	Context    string          `json:"context" yaml:"context"`
	ReuseConn  bool            `json:"reuse_connection" yaml:"reuse_connection"`
	Properties json.RawMessage `json:"properties" yaml:"properties"`
	nc         *nats.Conn
	mu         sync.Mutex
}

func (c *Check) connect(urls string, opts ...nats.Option) (*nats.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.ReuseConn {
		return nil, nil
	}

	if c.nc != nil {
		return c.nc, nil
	}

	opts = append(opts, nats.MaxReconnects(-1))

	var err error
	c.nc, err = nats.Connect(urls, opts...)

	return c.nc, err
}

type Config struct {
	Context string   `yaml:"context"`
	Checks  []*Check `yaml:"checks"`
}

type Exporter struct {
	ns     string
	config Config
}

func NewExporter(ns string, f string) (*Exporter, error) {
	cf, err := os.ReadFile(f)
	if err != nil {
		return nil, err
	}

	if ns == "" {
		ns = "natscli"
	}

	exporter := &Exporter{
		ns: ns,
	}

	err = yaml.Unmarshal(cf, &exporter.config)
	if err != nil {
		return nil, err
	}

	return exporter, nil
}

// Describe implements prometheus.Collector
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// we have dynamic metrics so we cant really do this all upfront at the moment
	//
	// according to https://github.com/prometheus/client_golang/issues/47 doing nothing
	// here is the right, if discouraged, thing to do here.
}

// Collect implements prometheus.Collector
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	callCheck := func(check *Check, f func(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result)) {
		result := &monitor.Result{Name: check.Name, Check: check.Kind, NameSpace: e.ns, RenderFormat: monitor.NagiosFormat}
		defer result.Collect(ch)

		nctx, err := e.natsContext(check)
		if result.CriticalIfErr(err, "could not load context: %v", err) {
			return
		}

		opts, err := nctx.NATSOptions()
		if result.CriticalIfErr(err, "could not load context: %v", err) {
			return
		}

		jsmopts, err := nctx.JSMOptions()
		if result.CriticalIfErr(err, "could not load jetstream options: %v", err) {
			return
		}

		f(nctx.ServerURL(), opts, jsmopts, check, result)
		log.Print(result)
	}

	for _, check := range e.config.Checks {
		var f func(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result)

		switch check.Kind {
		case "connection":
			f = e.checkConnection
		case "stream":
			f = e.checkStream
		case "consumer":
			f = e.checkConsumer
		case "message":
			f = e.checkMessage
		case "meta":
			f = e.checkMeta
		case "jetstream":
			f = e.checkJetStream
		case "server":
			f = e.checkServer
		case "kv":
			f = e.checkKv
		case "credential":
			f = e.checkCredential
		case "request":
			f = e.checkRequest
		default:
			log.Printf("Unknown check kind %s", check.Kind)
			continue
		}

		callCheck(check, f)
	}
}

func (e *Exporter) natsContext(check *Check) (*natscontext.Context, error) {
	ctxName := check.Context
	if ctxName == "" {
		ctxName = e.config.Context
	}

	if iu.FileExists(ctxName) {
		return natscontext.NewFromFile(ctxName)
	}

	return natscontext.New(ctxName, true)
}

func (e *Exporter) checkRequest(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckRequestOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckRequestWithConnection(nc, result, 5*time.Second, copts)
	} else {
		err = monitor.CheckRequest(servers, natsOpts, result, 5*time.Second, copts)
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkCredential(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckCredentialOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	err = monitor.CheckCredential(result, copts)
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkServer(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckServerOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckServerWithConnection(nc, result, time.Second, copts)
	} else {
		err = monitor.CheckServer(servers, natsOpts, result, time.Second, copts)
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkJetStream(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckJetStreamAccountOptions{
		MemoryCritical:    -1,
		MemoryWarning:     -1,
		FileWarning:       -1,
		FileCritical:      -1,
		ConsumersCritical: -1,
		ConsumersWarning:  -1,
		StreamCritical:    -1,
		StreamWarning:     -1,
	}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckJetStreamAccountWithConnection(nc, jsmOpts, result, copts)
	} else {
		err = monitor.CheckJetStreamAccount(servers, natsOpts, jsmOpts, result, copts)
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkMeta(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckJetstreamMetaOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckJetstreamMetaWithConnection(nc, result, copts)
	} else {
		err = monitor.CheckJetstreamMeta(servers, natsOpts, result, copts)
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkMessage(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckStreamMessageOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckStreamMessageWithConnection(nc, jsmOpts, result, copts)
	} else {
		err = monitor.CheckStreamMessage(servers, natsOpts, jsmOpts, result, copts)
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkKv(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckKVBucketAndKeyOptions{
		ValuesWarning:  -1,
		ValuesCritical: -1,
	}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckKVBucketAndKeyWithConnection(nc, result, copts)
	} else {
		err = monitor.CheckKVBucketAndKey(servers, natsOpts, result, copts)
	}

	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkConsumer(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.ConsumerHealthCheckOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.ConsumerHealthCheckWithConnection(nc, jsmOpts, result, copts, api.NewDiscardLogger())
	} else {
		err = monitor.ConsumerHealthCheck(servers, natsOpts, jsmOpts, result, copts, api.NewDiscardLogger())
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkStream(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckStreamHealthOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	nc, err := check.connect(servers, natsOpts...)
	if result.CriticalIfErr(err, "connection failed: %v", err) {
		return
	}

	if nc != nil {
		err = monitor.CheckStreamHealthWithConnection(nc, jsmOpts, result, copts, api.NewDiscardLogger())
	} else {
		err = monitor.CheckStreamHealth(servers, natsOpts, jsmOpts, result, copts, api.NewDiscardLogger())
	}
	result.CriticalIfErr(err, "check failed: %v", err)
}

func (e *Exporter) checkConnection(servers string, natsOpts []nats.Option, jsmOpts []jsm.Option, check *Check, result *monitor.Result) {
	copts := monitor.CheckConnectionOptions{}
	err := yaml.Unmarshal(check.Properties, &copts)
	if result.CriticalIfErr(err, "invalid properties: %v", err) {
		return
	}

	err = monitor.CheckConnection(servers, natsOpts, 2*time.Second, result, copts)
	result.CriticalIfErr(err, "check failed: %v", err)
}

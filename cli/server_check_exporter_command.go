// Copyright 2024 The NATS Authors
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

package cli

import (
	"fmt"
	"net/http"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/internal/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (c *SrvCheckCmd) exporterAction(_ *fisk.ParseContext) error {
	exp, err := exporter.NewExporter(opts().PrometheusNamespace, c.exporterConfigFile)
	if err != nil {
		return err
	}

	prometheus.MustRegister(exp)
	http.Handle("/metrics", promhttp.Handler())

	if c.exporterCertificate != "" && c.exporterKey != "" {
		log.Printf("NATS CLI Prometheus Exporter listening on https://0.0.0.0:%da/metrics", c.exporterPort)
		return http.ListenAndServeTLS(fmt.Sprintf(":%d", c.exporterPort), c.exporterCertificate, c.exporterKey, nil)
	} else {
		log.Printf("NATS CLI Prometheus Exporter listening on http://0.0.0.0:%d/metrics", c.exporterPort)
		return http.ListenAndServe(fmt.Sprintf(":%d", c.exporterPort), nil)
	}
}

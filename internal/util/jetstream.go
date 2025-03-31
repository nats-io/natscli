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

package util

import (
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/natscli/columns"
)

// RenderMetaApi draws the _nats.* metadata on streams and consumers
func RenderMetaApi(cols *columns.Writer, metadata map[string]string) {
	versionMeta := metadata[api.JSMetaCurrentServerVersion]
	levelMeta := metadata[api.JSMetaCurrentServerLevel]
	requiredMeta := metadata[api.JsMetaRequiredServerLevel]

	if versionMeta != "" || levelMeta != "" || requiredMeta != "" {
		if versionMeta != "" {
			cols.AddRow("Host Version", versionMeta)
		}

		if levelMeta != "" || requiredMeta != "" {
			if requiredMeta == "" {
				requiredMeta = "0"
			}
			cols.AddRowf("Required API Level", "%s hosted at level %s", requiredMeta, levelMeta)
		}
	}
}

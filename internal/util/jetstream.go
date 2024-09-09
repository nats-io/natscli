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
	createdVersion := metadata[api.JSMetaCreatedServerVersion]

	if versionMeta != "" || levelMeta != "" || requiredMeta != "" {
		if versionMeta != "" {
			if createdVersion == "" || createdVersion == versionMeta {
				cols.AddRow("Host Version", versionMeta)
			} else {
				cols.AddRowf("Host Version", "%s created on %s", versionMeta, createdVersion)
			}
		}

		if levelMeta != "" || requiredMeta != "" {
			if requiredMeta == "" {
				requiredMeta = "0"
			}
			cols.AddRowf("Required API Level", "%s hosted at level %s", requiredMeta, levelMeta)
		}
	}
}

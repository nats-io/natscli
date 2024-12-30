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
	"time"
)

func configureAuditCommand(app commandHost) {
	audit := app.Command("audit", "Audit a NATS deployment").Hidden()

	configureAuditGatherCommand(audit)
	configureAuditAnalyzeCommand(audit)
	configureAuditChecksCommand(audit)
}

func init() {
	registerCommand("audit", 19, configureAuditCommand)
}

type auditMetadata struct {
	Timestamp              time.Time `json:"capture_timestamp"`
	ConnectedServerName    string    `json:"connected_server_name"`
	ConnectedServerVersion string    `json:"connected_server_version"`
	ConnectURL             string    `json:"connect_url"`
	UserName               string    `json:"user_name"`
	CLIVersion             string    `json:"cli_version"`
}

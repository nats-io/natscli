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

package sysclient

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/internal/util"
)

const (
	StatusOK HealthStatus = iota
	StatusUnavailable
	StatusError
)

type (
	HealthzResp struct {
		Server  server.ServerInfo `json:"server"`
		Healthz Healthz           `json:"data"`
	}

	Healthz struct {
		Status HealthStatus `json:"status"`
		Error  string       `json:"error,omitempty"`
	}

	HealthStatus int
)

func (hs *HealthStatus) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case util.JSONString("ok"):
		*hs = StatusOK
	case util.JSONString("na"), util.JSONString("unavailable"):
		*hs = StatusUnavailable
	case util.JSONString("error"):
		*hs = StatusError
	default:
		return fmt.Errorf("cannot unmarshal %q", data)
	}

	return nil
}

func (hs HealthStatus) MarshalJSON() ([]byte, error) {
	switch hs {
	case StatusOK:
		return json.Marshal("ok")
	case StatusUnavailable:
		return json.Marshal("na")
	case StatusError:
		return json.Marshal("error")
	default:
		return nil, fmt.Errorf("unknown health status: %v", hs)
	}
}

func (hs HealthStatus) String() string {
	switch hs {
	case StatusOK:
		return "ok"
	case StatusUnavailable:
		return "na"
	case StatusError:
		return "error"
	default:
		return "unknown health status"
	}
}

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
	"github.com/nats-io/nats.go"
)

type contextProvider interface {
	User() string
	Password() string
	Creds() string
	NKey() string
	Certificate() string
	Key() string
	CA() string
}

func NATSOptions(c contextProvider) (opts []nats.Option, err error) {
	if c.User() != "" {
		opts = append(opts, nats.UserInfo(c.User(), c.Password()))
	}

	if c.Creds() != "" {
		opts = append(opts, nats.UserCredentials(c.Creds()))
	}

	if c.NKey() != "" {
		nko, err := nats.NkeyOptionFromSeed(c.NKey())
		if err != nil {
			return nil, err
		}

		opts = append(opts, nko)
	}

	if c.Certificate() != "" && c.Key() != "" {
		opts = append(opts, nats.ClientCert(c.Certificate(), c.Key()))
	}

	if c.CA() != "" {
		opts = append(opts, nats.RootCAs(c.CA()))
	}

	return opts, nil
}

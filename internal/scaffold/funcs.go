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

package scaffold

import (
	"html/template"

	"github.com/nats-io/natscli/internal/auth"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"getOperator": func(op string) (ab.Operator, error) {
			ab, err := auth.GetAuthBuilder()
			if err != nil {
				return nil, err
			}
			return ab.Operators().Get(op)
		},
		"getAccount": func(op string, account string) (ab.Account, error) {
			ab, err := auth.GetAuthBuilder()
			if err != nil {
				return nil, err
			}
			operator, err := ab.Operators().Get(op)
			if err != nil {
				return nil, err
			}

			return operator.Accounts().Get(account)
		},
	}
}

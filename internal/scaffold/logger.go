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
	"log"

	"github.com/choria-io/scaffold"
)

type logger struct {
	debug bool
}

func (l logger) Debugf(format string, v ...any) {
	if l.debug {
		log.Printf(format, v...)
	}
}

func (l logger) Infof(format string, v ...any) {
	log.Printf(format, v...)
}

func newLogger(debug bool) scaffold.Logger {
	return &logger{
		debug: debug,
	}
}

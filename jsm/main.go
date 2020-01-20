// Copyright 2019 The NATS Authors
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
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	servers string
	creds   string
	tlsCert string
	tlsKey  string
	tlsCA   string
	timeout time.Duration
	version string
)

func main() {
	jsm := kingpin.New("jsm", "JetStream Management Utility")
	jsm.Author("NATS Authors <info@nats.io>")
	jsm.Version(version)
	jsm.HelpFlag.Short('h')

	jsm.Flag("server", "NATS servers").Short('s').Default("localhost:4222").Envar("NATS_URL").StringVar(&servers)
	jsm.Flag("creds", "User credentials").Envar("NATS_CREDS").StringVar(&creds)
	jsm.Flag("tlscert", "TLS public certificate").ExistingFileVar(&tlsCert)
	jsm.Flag("tlskey", "TLS private key").ExistingFileVar(&tlsCert)
	jsm.Flag("tlsca", "TLS certificate authority chain").ExistingFileVar(&tlsCA)
	jsm.Flag("timeout", "Time to wait on responses from JetStream").Default("2s").DurationVar(&timeout)

	configureActCommand(jsm)
	configureStreamCommand(jsm)
	configureConsumerCommand(jsm)

	kingpin.MustParse(jsm.Parse(os.Args[1:]))
}

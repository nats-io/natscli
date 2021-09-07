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
	"log"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jsm.go/natscontext"
)

var (
	config        *natscontext.Context
	servers       string
	creds         string
	tlsCert       string
	tlsKey        string
	tlsCA         string
	timeout       time.Duration
	version       string
	username      string
	password      string
	nkey          string
	jsApiPrefix   string
	jsEventPrefix string
	jsDomain      string
	cfgCtx        string
	ctxError      error
	trace         bool

	// used during tests
	skipContexts bool

	cheats map[string]string

	// These are persisted by contexts, as properties thereof.
	// So don't include NATS_CONTEXT in this list.
	overrideEnvVars = []string{"NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CREDS", "NATS_NKEY", "NATS_CERT", "NATS_KEY", "NATS_CA", "NATS_TIMEOUT"}
)

func main() {
	if version == "" {
		version = "development"
	}

	help := `NATS Utility

NATS Server and JetStream administration.

See 'nats cheat' for a quick cheatsheet of commands
	`

	ncli := kingpin.New("nats", help)
	ncli.Author("NATS Authors <info@nats.io>")
	ncli.UsageWriter(os.Stdout)
	ncli.Version(version)
	ncli.HelpFlag.Short('h')

	ncli.Flag("server", "NATS server urls").Short('s').Envar("NATS_URL").PlaceHolder("NATS_URL").StringVar(&servers)
	ncli.Flag("user", "Username or Token").Envar("NATS_USER").PlaceHolder("NATS_USER").StringVar(&username)
	ncli.Flag("password", "Password").Envar("NATS_PASSWORD").PlaceHolder("NATS_PASSWORD").StringVar(&password)
	ncli.Flag("creds", "User credentials").Envar("NATS_CREDS").PlaceHolder("NATS_CREDS").StringVar(&creds)
	ncli.Flag("nkey", "User NKEY").Envar("NATS_NKEY").PlaceHolder("NATS_NKEY").StringVar(&nkey)
	ncli.Flag("tlscert", "TLS public certificate").Envar("NATS_CERT").PlaceHolder("NATS_CERT").ExistingFileVar(&tlsCert)
	ncli.Flag("tlskey", "TLS private key").Envar("NATS_KEY").PlaceHolder("NATS_KEY").ExistingFileVar(&tlsKey)
	ncli.Flag("tlsca", "TLS certificate authority chain").Envar("NATS_CA").PlaceHolder("NATS_CA").ExistingFileVar(&tlsCA)
	ncli.Flag("timeout", "Time to wait on responses from NATS").Default("5s").Envar("NATS_TIMEOUT").PlaceHolder("NATS_TIMEOUT").DurationVar(&timeout)
	ncli.Flag("js-api-prefix", "Subject prefix for access to JetStream API").PlaceHolder("PREFIX").StringVar(&jsApiPrefix)
	ncli.Flag("js-event-prefix", "Subject prefix for access to JetStream Advisories").PlaceHolder("PREFIX").StringVar(&jsEventPrefix)
	ncli.Flag("js-domain", "JetStream domain to access").PlaceHolder("PREFIX").PlaceHolder("DOMAIN").StringVar(&jsDomain)
	ncli.Flag("domain", "JetStream domain to access").PlaceHolder("PREFIX").PlaceHolder("DOMAIN").Hidden().StringVar(&jsDomain)
	ncli.Flag("context", "Configuration context").Envar("NATS_CONTEXT").StringVar(&cfgCtx)
	ncli.Flag("trace", "Trace API interactions").BoolVar(&trace)

	ncli.PreAction(prepareConfig)

	log.SetFlags(log.Ltime)

	cheats = make(map[string]string)

	configureActCommand(ncli)
	configureBackupCommand(ncli)
	configureBenchCommand(ncli)
	configureCheatCommand(ncli)
	configureConsumerCommand(ncli)
	configureCtxCommand(ncli)
	configureErrCommand(ncli)
	configureEventsCommand(ncli)
	configureGovernorCommand(ncli)
	configureKVCommand(ncli)
	configureLatencyCommand(ncli)
	configurePubCommand(ncli)
	configureRTTCommand(ncli)
	configureReplyCommand(ncli)
	configureRestoreCommand(ncli)
	configureSchemaCommand(ncli)
	configureServerCommand(ncli)
	configureStreamCommand(ncli)
	configureSubCommand(ncli)
	configureTrafficCommand(ncli)

	kingpin.MustParse(ncli.Parse(os.Args[1:]))
}

// Copyright 2020-2024 The NATS Authors
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
	iu "github.com/nats-io/natscli/internal/util"
	"log"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/plugins"

	"github.com/nats-io/natscli/cli"
)

var version = "development"

func main() {
	help := `NATS Utility

NATS Server and JetStream administration.

See 'nats cheat' for a quick cheatsheet of commands`

	ncli := fisk.New("nats", help)
	ncli.Author("NATS Authors <info@nats.io>")
	ncli.UsageWriter(os.Stdout)
	ncli.Version(getVersion())
	ncli.HelpFlag.Short('h')
	ncli.WithCheats().CheatCommand.Hidden()

	opts, err := cli.ConfigureInApp(ncli, nil, true)
	if err != nil {
		return
	}
	cli.SetVersion(getVersion())

	ncli.Flag("server", "NATS server urls").Short('s').Envar("NATS_URL").PlaceHolder("URL").StringVar(&opts.Servers)
	ncli.Flag("user", "Username or Token").Envar("NATS_USER").PlaceHolder("USER").StringVar(&opts.Username)
	ncli.Flag("password", "Password").Envar("NATS_PASSWORD").PlaceHolder("PASSWORD").StringVar(&opts.Password)
	ncli.Flag("token", "Token").Envar("NATS_TOKEN").PlaceHolder("TOKEN").StringVar(&opts.Token)
	ncli.Flag("connection-name", "Nickname to use for the underlying NATS Connection").Default("NATS CLI Version " + getVersion()).PlaceHolder("NAME").StringVar(&opts.ConnectionName)
	ncli.Flag("creds", "User credentials").Envar("NATS_CREDS").PlaceHolder("FILE").StringVar(&opts.Creds)
	ncli.Flag("nkey", "User NKEY").Envar("NATS_NKEY").PlaceHolder("FILE").StringVar(&opts.Nkey)
	ncli.Flag("tlscert", "TLS public certificate").Envar("NATS_CERT").PlaceHolder("FILE").ExistingFileVar(&opts.TlsCert)
	ncli.Flag("tlskey", "TLS private key").Envar("NATS_KEY").PlaceHolder("FILE").ExistingFileVar(&opts.TlsKey)
	ncli.Flag("tlsca", "TLS certificate authority chain").Envar("NATS_CA").PlaceHolder("FILE").ExistingFileVar(&opts.TlsCA)
	ncli.Flag("tlsfirst", "Perform TLS handshake before expecting the server greeting").BoolVar(&opts.TlsFirst)
	if runtime.GOOS == "windows" {
		ncli.Flag("certstore", "Uses a Windows Certificate Store for TLS (user, machine)").PlaceHolder("TYPE").EnumVar(&opts.WinCertStoreType, "user", "windowscurrentuser", "machine", "windowslocalmachine")
		ncli.Flag("certstore-match", "Which certificate to use in the store").PlaceHolder("QUERY").StringVar(&opts.WinCertStoreMatch)
		ncli.Flag("certstore-match-by", "Configures the way certificates are searched for (subject, issuer)").PlaceHolder("MATCH").Default("subject").EnumVar(&opts.WinCertStoreMatchBy, "subject", "issuer")
		ncli.Flag("certstore-ca-match", "Which certificate authority should be used from the store").StringsVar(&opts.WinCertCaStoreMatch)
	}
	ncli.Flag("timeout", "Time to wait on responses from NATS").Default("5s").Envar("NATS_TIMEOUT").PlaceHolder("DURATION").DurationVar(&opts.Timeout)
	ncli.Flag("socks-proxy", "SOCKS5 proxy for connecting to NATS server").Envar("NATS_SOCKS_PROXY").PlaceHolder("PROXY").StringVar(&opts.SocksProxy)
	ncli.Flag("js-api-prefix", "Subject prefix for access to JetStream API").PlaceHolder("PREFIX").StringVar(&opts.JsApiPrefix)
	ncli.Flag("js-event-prefix", "Subject prefix for access to JetStream Advisories").PlaceHolder("PREFIX").StringVar(&opts.JsEventPrefix)
	ncli.Flag("js-domain", "JetStream domain to access").PlaceHolder("DOMAIN").StringVar(&opts.JsDomain)
	ncli.Flag("inbox-prefix", "Custom inbox prefix to use for inboxes").PlaceHolder("PREFIX").StringVar(&opts.InboxPrefix)
	ncli.Flag("domain", "JetStream domain to access").PlaceHolder("DOMAIN").Hidden().StringVar(&opts.JsDomain)
	ncli.Flag("colors", "Sets a color scheme to use").PlaceHolder("SCHEME").Envar("NATS_COLOR").EnumVar(&opts.ColorScheme, iu.ValidStyles()...)
	ncli.Flag("context", "Configuration context").Envar("NATS_CONTEXT").PlaceHolder("NAME").StringVar(&opts.CfgCtx)
	ncli.Flag("trace", "Trace API interactions").UnNegatableBoolVar(&opts.Trace)
	ncli.Flag("no-context", "Disable the selected context").UnNegatableBoolVar(&cli.SkipContexts)

	log.SetFlags(log.Ltime)

	plugins.AddToApp(ncli)

	ncli.MustParseWithUsage(os.Args[1:])
}

func getVersion() string {
	if version != "development" {
		return version
	}

	nfo, ok := debug.ReadBuildInfo()
	if !ok || (nfo != nil && nfo.Main.Version == "") {
		return version
	}

	return nfo.Main.Version
}

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
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/cli"
)

var version = "development"

var ShorterMainUsageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\

{{define "FormatCommands"}}\
{{.Commands|FormatCommandColumns}}
{{end}}\

{{ define "FormatCommandsForTopLevel" }}\
{{.Commands|FormatCommandColumns}}
{{end}}\

{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\
{{end}}\

{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{if .Context.SelectedCommand.HelpLong}}{{.Context.SelectedCommand.HelpLong|Wrap 0}}
{{end}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\
{{if .Context.SelectedCommand}}\
{{if len .Context.SelectedCommand.Commands}}\
Subcommands:
{{template "FormatCommands" .Context.SelectedCommand}}
{{end}}\
{{if .Context.Flags|VisibleFlags}}\
Flags:
{{.Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{else if .App.Commands}}\
Commands:
{{template "FormatCommandsForTopLevel" .App}}
{{end}}\
`

func main() {
	help := `NATS Utility

NATS Server and JetStream administration.

See 'nats cheat' for a quick cheatsheet of commands`

	ncli := fisk.New("nats", help)
	ncli.Author("NATS Authors <info@nats.io>")
	ncli.UsageWriter(os.Stdout)
	ncli.UsageTemplate(ShorterMainUsageTemplate)
	ncli.UsageFuncs(template.FuncMap{
		"FormatCommandColumns": func(commands []*fisk.CmdModel) string {
			buf := new(bytes.Buffer)
			tw := tabwriter.NewWriter(buf, 8, 8, 1, '\t', tabwriter.AlignRight)
			for _, c := range commands {
				if !c.Hidden && c.FullCommand != "help" {
					fmt.Fprintf(tw, "  %s\t%s\n", c.FullCommand, strings.Split(c.Help, "\n")[0])
				}
			}
			tw.Flush()
			return buf.String()
		},
	})
	ncli.Version(getVersion())
	ncli.HelpFlag.Short('h')
	ncli.WithCheats().CheatCommand.Hidden()

	opts, err := cli.ConfigureInApp(ncli, nil, true)
	if err != nil {
		return
	}
	cli.SetVersion(version)

	ncli.Flag("server", "NATS server urls").Short('s').Envar("NATS_URL").PlaceHolder("URL").StringVar(&opts.Servers)
	ncli.Flag("user", "Username or Token").Envar("NATS_USER").PlaceHolder("USER").StringVar(&opts.Username)
	ncli.Flag("password", "Password").Envar("NATS_PASSWORD").PlaceHolder("PASSWORD").StringVar(&opts.Password)
	ncli.Flag("connection-name", "Nickname to use for the underlying NATS Connection").Default("NATS CLI Version " + version).PlaceHolder("NAME").StringVar(&opts.ConnectionName)
	ncli.Flag("creds", "User credentials").Envar("NATS_CREDS").PlaceHolder("FILE").StringVar(&opts.Creds)
	ncli.Flag("nkey", "User NKEY").Envar("NATS_NKEY").PlaceHolder("FILE").StringVar(&opts.Nkey)
	ncli.Flag("tlscert", "TLS public certificate").Envar("NATS_CERT").PlaceHolder("FILE").ExistingFileVar(&opts.TlsCert)
	ncli.Flag("tlskey", "TLS private key").Envar("NATS_KEY").PlaceHolder("FILE").ExistingFileVar(&opts.TlsKey)
	ncli.Flag("tlsca", "TLS certificate authority chain").Envar("NATS_CA").PlaceHolder("FILE").ExistingFileVar(&opts.TlsCA)
	ncli.Flag("timeout", "Time to wait on responses from NATS").Default("5s").Envar("NATS_TIMEOUT").PlaceHolder("DURATION").DurationVar(&opts.Timeout)
	ncli.Flag("js-api-prefix", "Subject prefix for access to JetStream API").PlaceHolder("PREFIX").StringVar(&opts.JsApiPrefix)
	ncli.Flag("js-event-prefix", "Subject prefix for access to JetStream Advisories").PlaceHolder("PREFIX").StringVar(&opts.JsEventPrefix)
	ncli.Flag("js-domain", "JetStream domain to access").PlaceHolder("DOMAIN").StringVar(&opts.JsDomain)
	ncli.Flag("inbox-prefix", "Custom inbox prefix to use for inboxes").PlaceHolder("PREFIX").StringVar(&opts.InboxPrefix)
	ncli.Flag("domain", "JetStream domain to access").PlaceHolder("DOMAIN").Hidden().StringVar(&opts.JsDomain)
	ncli.Flag("context", "Configuration context").Envar("NATS_CONTEXT").PlaceHolder("NAME").StringVar(&opts.CfgCtx)
	ncli.Flag("trace", "Trace API interactions").UnNegatableBoolVar(&opts.Trace)

	log.SetFlags(log.Ltime)

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

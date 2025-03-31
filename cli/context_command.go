// Copyright 2020-2025 The NATS Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strings"
	"text/template"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/fatih/color"
	"github.com/ghodss/yaml"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
)

type ctxCommand struct {
	json             bool
	completionFormat bool
	namesFormat      bool
	activate         bool
	description      string
	name             string
	source           string
	nsc              string
	force            bool
	validateErrors   int
}

func configureCtxCommand(app commandHost) {
	c := ctxCommand{}

	context := app.Command("context", "Manage nats configuration contexts").Alias("ctx")
	addCheat("contexts", context)

	save := context.Command("add", "Update or create a context").Alias("create").Alias("save").Action(c.createCommand)
	save.Arg("name", "The context name to act on").Required().StringVar(&c.name)
	save.Flag("description", "Set a friendly description for this context").StringVar(&c.description)
	save.Flag("select", "Select the saved context as the default one").UnNegatableBoolVar(&c.activate)
	save.Flag("nsc", "URL to a nsc user, eg. nsc://<operator>/<account>/<user>").StringVar(&c.nsc)

	dupe := context.Command("copy", "Copies an existing context").Alias("cp").Action(c.copyCommand)
	dupe.Arg("source", "The name of the context to copy from").Required().StringVar(&c.source)
	dupe.Arg("name", "The name of the context to create").Required().StringVar(&c.name)
	dupe.Flag("description", "Set a friendly description for this context").StringVar(&c.description)
	dupe.Flag("select", "Select the saved context as the default one").UnNegatableBoolVar(&c.activate)
	dupe.Flag("nsc", "URL to a nsc user, eg. nsc://<operator>/<account>/<user>").StringVar(&c.nsc)

	edit := context.Command("edit", "Edit a context in your EDITOR").Alias("vi").Action(c.editCommand)
	edit.Arg("name", "The context name to edit").Required().StringVar(&c.name)

	ls := context.Command("ls", "List known contexts").Alias("list").Alias("l").Action(c.listCommand)
	ls.Flag("completion", "Format the list for use by shell completion").Hidden().UnNegatableBoolVar(&c.completionFormat)
	ls.Flag("json", "Show the list in JSON format").Short('j').UnNegatableBoolVar(&c.json)
	ls.Flag("names", "List just the names of known contexts").UnNegatableBoolVar(&c.namesFormat)

	rm := context.Command("rm", "Remove a context").Alias("remove").Action(c.removeCommand)
	rm.Arg("name", "The context name to remove").Required().StringVar(&c.name)
	rm.Flag("force", "Force remove without prompting").Short('f').UnNegatableBoolVar(&c.force)

	pick := context.Command("select", "Select the default context").Alias("switch").Alias("set").Action(c.selectCommand)
	pick.Arg("name", "The context name to select").StringVar(&c.name)

	context.Command("unselect", "Ensures that no context is the default context").Action(c.unselectCommand)

	info := context.Command("info", "Display information on the current or named context").Alias("show").Alias("v").Alias("view").Action(c.showCommand)
	info.Arg("name", "The context name to show").StringVar(&c.name)
	info.Flag("json", "Show the context in JSON format").Short('j').UnNegatableBoolVar(&c.json)
	info.Flag("connect", "Attempts to connect to NATS using the context while validating").UnNegatableBoolVar(&c.activate)

	validate := context.Command("validate", "Validate one or all contexts").Action(c.validateCommand)
	validate.Arg("name", "Validate a specific context, validates all when not supplied").StringVar(&c.name)
	validate.Flag("connect", "Attempts to connect to NATS using the context while validating").UnNegatableBoolVar(&c.activate)

	context.Command("previous", "switch to the previous context").Alias("-").Action(c.switchPreviousCtx)
}

func init() {
	registerCommand("context", 5, configureCtxCommand)
}

func (c *ctxCommand) hasOverrides() bool {
	return len(c.overrideVars()) != 0
}

func (c *ctxCommand) overrideVars() []string {
	var list []string
	for _, v := range overrideEnvVars {
		if os.Getenv(v) != "" {
			list = append(list, v)
		}
	}

	return list
}
func (c *ctxCommand) validateCommand(pc *fisk.ParseContext) error {
	var contexts []string
	if c.name == "" {
		contexts = natscontext.KnownContexts()
	} else {
		contexts = append(contexts, c.name)
	}

	for _, name := range contexts {
		c.name = name
		err := c.showCommand(pc)
		if err != nil {
			fmt.Printf("Could not load %s: %s\n\n", name, color.RedString(err.Error()))
		}

		fmt.Println()
	}

	if c.validateErrors > 0 {
		return fmt.Errorf("validation failed")
	}

	return nil
}

func (c *ctxCommand) copyCommand(pc *fisk.ParseContext) error {
	if !natscontext.IsKnown(c.source) {
		return fmt.Errorf("unknown context %q", c.source)
	}

	if natscontext.IsKnown(c.name) {
		return fmt.Errorf("context %q already exist", c.name)
	}

	opts().CfgCtx = c.source

	return c.createCommand(pc)
}

var ctxYamlTemplate = `# Friendly description for this context shown when listing contexts
description: {{ .Description | t }}

# A comma separated list of NATS Servers to connect to
url: {{ .ServerURL | t }}

# Connect using a specific username, requires password to be set
user: {{ .User | t }}
password: {{ .Password | t }}

# Connect using a NATS Credentials stored in a file
creds: {{ .Creds | t }}

# Connect using a NKey with seed stored in a file
nkey: {{ .NKey | t }}

# Configures a token to pass in the connection
token: {{ .Token | t }}

# Sets a x509 certificate to use, both cert and key should be set
cert: {{ .Certificate | t }}
key: {{ .Key | t }}

# Sets an optional x509 trust chain to use
ca: {{ .CA | t }}

# Performs TLS Handshake before Server sends a greeting
tls_first: {{ .TLSHandshakeFirst | t }}

# Windows Certificate Store support requires windows_cert_store and windows_cert_match to be set
#
# windows_cert_store must be one of 'user' or 'machine'
# windows_cert_match_by may be 'subject' or 'issuer'
windows_cert_store: {{ .WindowsCertStore | t }}
windows_cert_match: {{ .WindowsCertStoreMatch | t }}
windows_cert_match_by: {{ .WindowsCertStoreMatchBy | t }}
windows_ca_certs_match: {{ .WindowsCaCertsMatch | t }}

# Retrieves connection information from 'nsc'
#
# Example: nsc://Acme+Inc/HR/Automation
nsc: {{ .NscURL | t }}

# Use a custom inbox prefix
#
# Example : _INBOX.private.userid
inbox_prefix: {{ .InboxPrefix | t }}

# Sets a color scheme to use for the nats command line tool
# this will influence table color choices allowing different
# contexts to be visually distinguished.
#
# Valid values are:
#
#   rounded
#   double
#   yellow
#   blue
#   cyan
#   green
#   magenta
#   red
#
# When not set "rounded" is used
color_scheme: {{ .ColorScheme | t }}

# Connects to a specific JetStream domain
jetstream_domain: {{ .JSDomain | t }}

# Subject used as a prefix when accessing the JetStream API if imported from another account
jetstream_api_prefix: {{ .JSAPIPrefix | t }}

# Subject prefix used to access JetStream events if imported from another account
jetstream_event_prefix: {{ .JSEventPrefix | t }}

# Use a Socks5 proxy like ssh to connect to the NATS server URLS
#
# Example: socks5://example.net:1090
socks_proxy: {{ .SocksProxy | t }}
`

func (c *ctxCommand) editCommand(pc *fisk.ParseContext) error {
	if !natscontext.IsKnown(c.name) {
		return fmt.Errorf("unknown context %q", c.name)
	}

	path, err := natscontext.ContextPath(c.name)
	if err != nil {
		return err
	}
	editFp := path

	var ctx *natscontext.Context

	ctx, err = natscontext.New(c.name, true)
	if err != nil {
		fmt.Printf("Context file %s cannot be loaded: %v\n\n", path, err)
		ok, err := askConfirmation("Edit the JSON file", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("corrupt context configuration file: %v", err)
		}
		fmt.Println()
	} else {
		tpl, err := template.New("context").Funcs(template.FuncMap{"t": func(s any) (string, error) {
			res, err := yaml.Marshal(s)
			if err != nil {
				return "", err
			}
			return string(bytes.TrimRight(res, "\n")), nil
		}}).Parse(ctxYamlTemplate)
		if err != nil {
			return err
		}

		f, err := os.CreateTemp("", "*.yaml")
		if err != nil {
			return fmt.Errorf("could not create temporary copy to edit: %w", err)
		}
		defer f.Close()

		err = tpl.ExecuteTemplate(f, "context", ctx)
		if err != nil {
			return fmt.Errorf("could not create temporary copy to edit: %w", err)
		}
		f.Close()

		editFp = f.Name()
	}

	err = iu.EditFile(editFp)
	if err != nil {
		return err
	}

	if path != editFp {
		yctx, err := os.ReadFile(editFp)
		if err != nil {
			return fmt.Errorf("could not read temporary copy: %w", err)
		}

		jctx, err := yaml.YAMLToJSON(yctx)
		if err != nil {
			return err
		}
		buff := bytes.NewBuffer([]byte{})
		err = json.Indent(buff, jctx, "", "  ")
		if err != nil {
			return fmt.Errorf("could not format JSON output: %w", err)
		}

		err = os.Rename(path, path+".bak")
		if err != nil {
			return fmt.Errorf("could not create a backup of the context definition: %w", err)
		}

		err = os.WriteFile(path, buff.Bytes(), 0600)
		if err != nil {
			return fmt.Errorf("could not save the context: %w", err)
		}

	}

	// There was an error with some data in the modified config
	// Save the known clean version and show the error
	err = c.showCommand(pc)
	if err != nil {
		// but not if the file was already corrupt and we are editing the json directly
		if path == editFp {
			return err
		}

		if ctx != nil {
			ctx.Save(c.name)
		}

		return fmt.Errorf("updated context validation failed - rolling back changes: %w", err)
	}

	return nil
}

func (c *ctxCommand) renderListNames(current string, known []*natscontext.Context) {
	var names []string
	for _, v := range known {
		names = append(names, v.Name)
	}
	sort.Strings(names)

	for _, v := range names {
		fmt.Println(v)
	}
}

func (c *ctxCommand) renderListJson(current string, known []*natscontext.Context) {
	sort.Slice(known, func(i, j int) bool {
		return known[i].Name < known[j].Name
	})

	iu.PrintJSON(known)
}

func (c *ctxCommand) renderListCompletion(current string, known []*natscontext.Context) {
	for _, nctx := range known {
		name := strings.ReplaceAll(nctx.Name, ":", `\:`)

		if name == current {
			name = name + "*"
		}

		fmt.Printf("%s:%s\n", name, nctx.Description())
	}
}

func (c *ctxCommand) renderListTable(current string, known []*natscontext.Context) {
	if len(known) == 0 {
		fmt.Println("No known contexts")
		return
	}

	table := iu.NewTableWriter(opts(), "Known Contexts")
	table.AddHeaders("Name", "Description")

	for _, nctx := range known {
		if nctx.Name == current {
			nctx.Name = nctx.Name + "*"
		}

		table.AddRow(nctx.Name, nctx.Description())
	}

	fmt.Println(table.Render())

}
func (c *ctxCommand) listCommand(_ *fisk.ParseContext) error {
	names := natscontext.KnownContexts()
	current := natscontext.SelectedContext()
	var contexts []*natscontext.Context

	for _, name := range names {
		cfg, err := natscontext.New(name, true)
		if err != nil {
			if !c.completionFormat {
				log.Printf("Could not load context %s: %s", name, err)
			}
			continue
		}

		contexts = append(contexts, cfg)
	}

	switch {
	case c.completionFormat:
		c.renderListCompletion(current, contexts)
	case c.json:
		c.renderListJson(current, contexts)
	case c.namesFormat:
		c.renderListNames(current, contexts)
	default:
		c.renderListTable(current, contexts)
	}

	return nil
}

func (c *ctxCommand) showCommand(_ *fisk.ParseContext) error {
	if c.name == "" {
		c.name = natscontext.SelectedContext()
	}

	if c.name == "" {
		return fmt.Errorf("no default context and no name supplied")
	}

	cfg, err := natscontext.New(c.name, true)
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(cfg)
		return nil
	}

	checkFile := func(file string) string {
		if file == "" {
			return ""
		}
		if strings.HasPrefix(file, "op://") {
			return color.CyanString("1Password")
		}
		if file[0] == '~' {
			usr, err := user.Current()
			if err != nil {
				return color.YellowString("failed to expand '~'. $HOME or $USER possibly not set")
			}
			file = strings.Replace(file, "~", usr.HomeDir, 1)
		}

		ok, err := iu.IsFileAccessible(file)
		if !ok || err != nil {
			c.validateErrors++
			return color.RedString("ERROR")
		}

		return color.GreenString("OK")
	}

	cols := newColumns(fmt.Sprintf("NATS Configuration Context %q", c.name))
	cols.AddRowIfNotEmpty("Description", cfg.Description())
	cols.AddRowIfNotEmpty("Server URLs", cfg.ServerURL())
	cols.AddRowIfNotEmpty("SOCKS5 Proxy", cfg.SocksProxy())
	cols.AddRowIfNotEmpty("Username", cfg.User())
	cols.AddRowIfNotEmpty("Password", strings.Repeat("*", len(cfg.Password())))
	cols.AddRowIfNotEmpty("Token", cfg.Token())
	cols.AddRowIf("Credentials", fmt.Sprintf("%s (%s)", cfg.Creds(), checkFile(cfg.Creds())), cfg.Creds() != "")
	cols.AddRowIf("NKey", fmt.Sprintf("%s (%s)", cfg.NKey(), checkFile(cfg.NKey())), cfg.NKey() != "")
	if cfg.WindowsCertStore() == "" {
		cols.AddRowIf("Certificate", fmt.Sprintf("%s (%s)", cfg.Certificate(), checkFile(cfg.Certificate())), cfg.Certificate() != "")
		cols.AddRowIf("Key", fmt.Sprintf("%s (%s)", cfg.Key(), checkFile(cfg.Key())), cfg.Key() != "")
	} else {
		cols.AddRow("Certificate Store", cfg.WindowsCertStore())
		cols.AddRow("Certificate Store Match", cfg.WindowsCertStoreMatch())
		cols.AddRow("Certificate Store Match By", cfg.WindowsCertStoreMatchBy())
		cols.AddRow("Certificate Store CA Match", cfg.WindowsCaCertsMatch())
	}
	cols.AddRowIf("CA", fmt.Sprintf("%s (%s)", cfg.CA(), checkFile(cfg.CA())), cfg.CA() != "")
	cols.AddRowIf("TLS First", cfg.TLSHandshakeFirst(), cfg.TLSHandshakeFirst())
	cols.AddRowIfNotEmpty("NSC Lookup", cfg.NscURL())
	cols.AddRowIfNotEmpty("JS API Prefix", cfg.JSAPIPrefix())
	cols.AddRowIfNotEmpty("JS Event Prefix", cfg.JSEventPrefix())
	cols.AddRowIfNotEmpty("JS Domain", cfg.JSDomain())
	cols.AddRowIfNotEmpty("Inbox Prefix", cfg.InboxPrefix())
	cols.AddRowIfNotEmpty("Path", cfg.Path())
	cols.AddRowIfNotEmpty("Color Scheme", cfg.ColorScheme())

	checkConn := func() error {
		opts, err := cfg.NATSOptions()
		opts = append(opts, nats.MaxReconnects(1))
		if err != nil {
			return err
		}
		nc, err := nats.Connect(cfg.ServerURL(), opts...)
		if err != nil {
			return err
		}
		nc.Close()

		return nil
	}

	if c.activate {
		err = checkConn()
		if err != nil {
			c.validateErrors++
			cols.AddRow("Connection", color.RedString(err.Error()))
		} else {
			cols.AddRow("Connection", color.GreenString("OK"))
		}
	}

	cols.Frender(os.Stdout)

	fmt.Println()

	if c.hasOverrides() {
		fmt.Printf("%s: Shell environment overrides in place using %v", color.HiRedString("WARNING"), f(c.overrideVars()))
		fmt.Println()
	}

	return nil
}
func (c *ctxCommand) createCommand(pc *fisk.ParseContext) error {
	lname := ""
	load := false
	opts := opts()

	switch {
	case natscontext.IsKnown(c.name):
		lname = c.name
		load = true
	case opts.CfgCtx != "":
		lname = opts.CfgCtx
		load = true
	}

	token := ""
	if opts.Password == "" && opts.Username != "" {
		token = opts.Username
		opts.Username = ""
	}

	ctxopts := []natscontext.Option{
		natscontext.WithServerURL(opts.Servers),
		natscontext.WithUser(opts.Username),
		natscontext.WithPassword(opts.Password),
		natscontext.WithToken(token),
		natscontext.WithCreds(opts.Creds),
		natscontext.WithNKey(opts.Nkey),
		natscontext.WithCertificate(opts.TlsCert),
		natscontext.WithKey(opts.TlsKey),
		natscontext.WithCA(opts.TlsCA),
		natscontext.WithWindowsCertStore(opts.WinCertStoreType),
		natscontext.WithWindowsCertStoreMatch(opts.WinCertStoreMatch),
		natscontext.WithWindowsCertStoreMatchBy(opts.WinCertStoreMatchBy),
		natscontext.WithWindowsCaCertsMatch(opts.WinCertCaStoreMatch...),
		natscontext.WithDescription(c.description),
		natscontext.WithNscUrl(c.nsc),
		natscontext.WithSocksProxy(opts.SocksProxy),
		natscontext.WithJSAPIPrefix(opts.JsApiPrefix),
		natscontext.WithJSEventPrefix(opts.JsEventPrefix),
		natscontext.WithJSDomain(opts.JsDomain),
		natscontext.WithInboxPrefix(opts.InboxPrefix),
		natscontext.WithColorScheme(opts.ColorScheme),
	}
	if opts.TlsFirst {
		ctxopts = append(ctxopts, natscontext.WithTLSHandshakeFirst())
	}

	config, err := natscontext.New(lname, load, ctxopts...)
	if err != nil {
		return err
	}

	err = config.Save(c.name)
	if err != nil {
		return err
	}

	if c.activate {
		return c.selectCommand(pc)
	}

	return c.showCommand(pc)
}

func (c *ctxCommand) removeCommand(_ *fisk.ParseContext) error {
	if natscontext.SelectedContext() == c.name {
		if !c.force {
			return fmt.Errorf("cannot remove the selected context, select another one or use the unselect command")
		}

		err := natscontext.UnSelectContext()
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete context %q", c.name), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %s", err)
		}

		if !ok {
			return nil
		}
	}

	return natscontext.DeleteContext(c.name)
}

func (c *ctxCommand) switchPreviousCtx(pc *fisk.ParseContext) error {
	ctxToSwitch := natscontext.PreviousContext()
	if ctxToSwitch == "" {
		return c.showCommand(pc)
	}

	if err := natscontext.SelectContext(ctxToSwitch); err != nil {
		return err
	}

	return c.showCommand(pc)
}

func (c *ctxCommand) unselectCommand(pc *fisk.ParseContext) error {
	current := natscontext.SelectedContext()
	if current == "" {
		fmt.Println("No context currently selected")
		return nil
	}

	err := natscontext.UnSelectContext()
	if err != nil {
		return err
	}

	fmt.Printf("Unselected the %q context\n", current)
	return nil
}

func (c *ctxCommand) selectCommand(pc *fisk.ParseContext) error {
	known := natscontext.KnownContexts()

	if len(known) == 0 {
		return fmt.Errorf("no context defined")
	}

	if c.name == "" {
		err := iu.AskOne(&survey.Select{
			Message:  "Select a Context",
			Options:  known,
			PageSize: iu.SelectPageSize(len(known)),
		}, &c.name)
		if err != nil {
			return err
		}
	}

	if c.name == "" {
		return fmt.Errorf("please select a context to activate")
	}

	err := natscontext.SelectContext(c.name)
	if err != nil {
		return err
	}

	return c.showCommand(pc)
}

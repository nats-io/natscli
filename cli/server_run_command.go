// Copyright 2021-2025 The NATS Authors
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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"text/template"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats-server/v2/server"
	"golang.org/x/crypto/bcrypt"
)

type SrvRunCmd struct {
	config SrvRunConfig
}

type SrvRunConfig struct {
	Name                 string
	Port                 string
	UserPassword         string
	UserPasswordCrypt    string
	ServicePassword      string
	ServicePasswordCrypt string
	SystemPassword       string
	SystemPasswordCrypt  string
	ExtendDemoNetwork    bool
	ExtendWithContext    bool
	JetStream            bool
	JSDomain             string
	Verbose              bool
	Debug                bool
	StoreDir             string
	Clean                bool
	MonitorPort          int
	Context              *natscontext.Context
}

var serverRunConfig = `
listen: 0.0.0.0:{{.Port}}
server_name: {{.Name}}
debug: {{.Debug}}
trace: {{.Verbose}}
system_account: SYSTEM
logtime: false
{{- if .MonitorPort }}
http_port: {{.MonitorPort}}
{{- end }}
{{- if .JetStream }}
jetstream {
    store_dir: "{{ .StoreDir | escape }}"
{{- if .JSDomain }}
	domain: {{ .JSDomain }}
{{- end }}
}
{{- end }}

accounts {
    USER: {
        jetstream: enabled
        users: [
            {
                user: "local",
                password: "{{.UserPasswordCrypt}}"
            }
        ]

        imports: [
            {service: {account: SERVICE, subject: "service.>"}, to: "imports.SERVICE.>"}
        ]
    }

    SERVICE: {
        jetstream: enabled
        users: [
            {
                user: "service",
                password: "{{.ServicePasswordCrypt}}"
            }
        ]

        exports: [
            {service: service.>}
        ]
    }

    SYSTEM: {
        users: [{"user": "system", "password": "{{.SystemPasswordCrypt}}"}],
    }
}

leafnodes {
    remotes = [
{{- if .ExtendDemoNetwork }}
        {
            url:"nats://demo.nats.io:7422", 
            account: "USER"
        }
{{- end }}
{{- if .ExtendWithContext }}
        {
            url: "{{.Context.ServerURL}}",
            {{- if .Context.Creds | escape }}
            credentials: "{{.Context.Creds | escape }}",
            {{- end }}
            account: "USER"
        }
{{- end }}
    ]
}
`

func configureServerRunCommand(srv *fisk.CmdClause) {
	c := &SrvRunCmd{}

	run := srv.Command("run", "Runs a local development NATS server").Hidden().Action(c.runAction)
	run.Arg("name", "Uses a named context for local access to the server").Default("nats_development").StringVar(&c.config.Name)
	run.Flag("extend-demo", "Extends the NATS demo network").UnNegatableBoolVar(&c.config.ExtendDemoNetwork)
	run.Flag("extend", "Extends a NATS network using a context").UnNegatableBoolVar(&c.config.ExtendWithContext)
	run.Flag("jetstream", "Enables JetStream support").UnNegatableBoolVar(&c.config.JetStream)
	run.Flag("port", "Sets the local listening port").Default("-1").StringVar(&c.config.Port)
	run.Flag("monitor", "Enable HTTP based monitoring on a local listening port").IntVar(&c.config.MonitorPort)
	run.Flag("clean", "Remove contexts after exiting").UnNegatableBoolVar(&c.config.Clean)
	run.Flag("verbose", "Log in debug mode").UnNegatableBoolVar(&c.config.Debug)
}

// server doesnt know what port -1 will pick since its the os at Listen time that does it
// so we call a quick listen to get a unused random port and return that for us
func (c *SrvRunCmd) getRandomPort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()

	_, port, err := net.SplitHostPort(l.Addr().String())
	return port, err
}

func (c *SrvRunCmd) validate() error {
	if c.config.ExtendWithContext {
		if opts().Config.ServerURL() == "" {
			return fmt.Errorf("extending using a context requires a server url in the context")
		}
	}

	return nil
}

func (c *SrvRunCmd) prepareConfig() error {
	err := c.validate()
	if err != nil {
		return err
	}

	if c.config.ExtendWithContext {
		c.config.Context, err = natscontext.New(c.config.Name, true)
		if err != nil {
			return err
		}
	} else {
		c.config.Context = opts().Config
	}

	if opts().Trace {
		c.config.Verbose = true
	}

	// we use existing contexts to re-use previously generated
	// passwords and ports
	if natscontext.IsKnown(c.config.Name) {
		nctx, err := natscontext.New(c.config.Name, true)
		if err != nil {
			return err
		}
		c.config.UserPassword = nctx.Password()
		u, err := url.Parse(nctx.ServerURL())
		if err != nil {
			return err
		}
		c.config.Port = u.Port()
	}

	svcName := fmt.Sprintf("%s_service", c.config.Name)
	if natscontext.IsKnown(svcName) {
		nctx, err := natscontext.New(svcName, true)
		if err != nil {
			return err
		}
		c.config.ServicePassword = nctx.Password()
	}

	sysName := fmt.Sprintf("%s_system", c.config.Name)
	if natscontext.IsKnown(sysName) {
		nctx, err := natscontext.New(sysName, true)
		if err != nil {
			return err
		}
		c.config.SystemPassword = nctx.Password()
	}

	if c.config.Port == "-1" {
		p, err := c.getRandomPort()
		if err != nil {
			return err
		}

		c.config.Port = p
	}

	if c.config.UserPassword == "" {
		c.config.UserPassword = iu.RandomString(32, 32)
	}
	b, err := bcrypt.GenerateFromPassword([]byte(c.config.UserPassword), 5)
	if err != nil {
		return err
	}
	c.config.UserPasswordCrypt = string(b)

	if c.config.SystemPassword == "" {
		c.config.SystemPassword = iu.RandomString(32, 32)
	}
	b, err = bcrypt.GenerateFromPassword([]byte(c.config.SystemPassword), 5)
	if err != nil {
		return err
	}
	c.config.SystemPasswordCrypt = string(b)

	if c.config.ServicePassword == "" {
		c.config.ServicePassword = iu.RandomString(32, 32)
	}
	b, err = bcrypt.GenerateFromPassword([]byte(c.config.ServicePassword), 5)
	if err != nil {
		return err
	}
	c.config.ServicePasswordCrypt = string(b)

	if c.config.JetStream {
		parent, err := iu.XdgShareHome()
		if err != nil {
			return err
		}
		c.config.StoreDir = filepath.Join(parent, "nats", c.config.Name)

		if c.config.ExtendWithContext || c.config.ExtendDemoNetwork {
			c.config.JSDomain = strings.ToUpper(c.config.Name)
		}
	}

	return nil
}

func (c *SrvRunCmd) writeConfig() (string, error) {
	err := c.prepareConfig()
	if err != nil {
		return "", err
	}

	tf, err := os.CreateTemp("", "nats-server-run-*.cfg")
	if err != nil {
		return "", err
	}
	defer tf.Close()

	funcs := template.FuncMap{
		"escape": func(v string) string {
			if runtime.GOOS == "windows" {
				return strings.ReplaceAll(v, `\`, "\\\\")
			}

			return v
		},
	}

	t, err := template.New("server.cfg").Funcs(funcs).Parse(serverRunConfig)
	if err != nil {
		os.Remove(tf.Name())
		return "", err
	}

	err = t.Execute(tf, c.config)
	if err != nil {
		os.Remove(tf.Name())
		return "", err
	}

	return tf.Name(), nil
}

func (c *SrvRunCmd) interruptWatcher(ctx context.Context, cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-sigs:
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func (c *SrvRunCmd) configureContexts(url string) (string, string, string, error) {
	if !natscontext.IsKnown(c.config.Name) {
		nctx, _ := natscontext.New(c.config.Name, false,
			natscontext.WithServerURL(url),
			natscontext.WithUser("local"),
			natscontext.WithPassword(c.config.UserPassword),
			natscontext.WithDescription("Local user access for NATS Development instance"),
			natscontext.WithJSDomain(c.config.JSDomain),
		)
		err := nctx.Save(nctx.Name)
		if err != nil {
			return "", "", "", err
		}
	}

	svcName := fmt.Sprintf("%s_service", c.config.Name)
	if !natscontext.IsKnown(svcName) {
		nctx, _ := natscontext.New(svcName, false,
			natscontext.WithServerURL(url),
			natscontext.WithUser("service"),
			natscontext.WithPassword(c.config.ServicePassword),
			natscontext.WithDescription("Local service access for NATS Development instance"),
			natscontext.WithJSDomain(c.config.JSDomain),
		)
		err := nctx.Save(svcName)
		if err != nil {
			return "", "", "", err
		}
	}

	sysName := fmt.Sprintf("%s_system", c.config.Name)
	if !natscontext.IsKnown(sysName) {
		nctx, _ := natscontext.New(sysName, false,
			natscontext.WithServerURL(url),
			natscontext.WithUser("system"),
			natscontext.WithPassword(c.config.SystemPassword),
			natscontext.WithDescription("System user access for NATS Development instance"),
		)
		err := nctx.Save(nctx.Name)
		if err != nil {
			return "", "", "", err
		}
	}

	return c.config.Name, svcName, sysName, nil
}

func (c *SrvRunCmd) runAction(_ *fisk.ParseContext) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.interruptWatcher(ctx, cancel)

	tf, err := c.writeConfig()
	if err != nil {
		return err
	}
	defer os.Remove(tf)

	opts := &server.Options{ConfigFile: tf}
	err = opts.ProcessConfigFile(tf)
	if err != nil {
		return err
	}
	opts.NoSigs = true

	srv, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	u, svc, s, err := c.configureContexts(srv.ClientURL())
	if err != nil {
		return err
	}

	fmt.Printf("Starting local development NATS Server instance: %s\n", c.config.Name)
	fmt.Println()
	fmt.Printf("        User Credentials: User: local   Password: %s Context: %s\n", c.config.UserPassword, u)
	fmt.Printf("     Service Credentials: User: service Password: %s Context: %s\n", c.config.ServicePassword, svc)
	fmt.Printf("      System Credentials: User: system  Password: %s Context: %s\n", c.config.SystemPassword, s)
	if c.config.JSDomain != "" {
		fmt.Printf("        JetStream Domain: %s\n", c.config.JSDomain)
	}
	fmt.Printf("  Extending Demo Network: %v\n", c.config.ExtendDemoNetwork)
	if c.config.ExtendWithContext {
		fmt.Printf("   Extending Remote NATS: using %s context\n", c.config.Context.Name)
	} else {
		fmt.Printf("   Extending Remote NATS: %v\n", c.config.ExtendWithContext)
	}
	fmt.Printf("                     URL: %s\n", srv.ClientURL())

	if c.config.Clean {
		fmt.Println("           Clean on Exit: true")
		defer natscontext.DeleteContext(u)
		defer natscontext.DeleteContext(s)
		defer natscontext.DeleteContext(svc)
	} else {
		fmt.Println("           Clean on Exit: false")
	}

	fmt.Println()
	fmt.Println()
	fmt.Println("NOTE: This is not a supported way to run a production NATS Server, view documentation")
	fmt.Println("      at https://docs.nats.io/running-a-nats-service/introduction for production use.")

	fmt.Println()

	srv.ConfigureLogger()

	srv.Start()

	<-ctx.Done()

	srv.Shutdown()
	srv.WaitForShutdown()

	return nil
}

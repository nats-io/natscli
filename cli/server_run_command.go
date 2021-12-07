package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"

	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats-server/v2/server"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/alecthomas/kingpin.v2"
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
	Listen               string
	Context              *natscontext.Context
}

var serverRunConfig = `
listen: 0.0.0.0:{{.Port}}
server_name: {{.Name}}
debug: {{.Debug}}
trace: {{.Verbose}}
system_account: SYSTEM
logtime: false

{{ if .JetStream }}
jetstream {
    store_dir: {{ .StoreDir }}
{{ if .JSDomain }}
	domain: {{ .JSDomain }}
{{ end }}
}
{{ end }}

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
            {{- if .Context.Creds }}
            credentials: "{{.Context.Creds}}",
            {{- end }}
            account: "USER"
        }
{{- end }}
    ]
}
`

func configureServerRunCommand(srv *kingpin.CmdClause) {
	c := &SrvRunCmd{}

	run := srv.Command("run", "Runs a local development NATS server").Hidden().Action(c.runAction)
	run.Arg("name", "Uses a named context for local access to the server").Default("nats_development").StringVar(&c.config.Name)
	run.Flag("extend-demo", "Extends the NATS demo network").BoolVar(&c.config.ExtendDemoNetwork)
	run.Flag("extend", "Extends a NATS network using a context").BoolVar(&c.config.ExtendWithContext)
	run.Flag("jetstream", "Enables JetStream support").BoolVar(&c.config.JetStream)
	run.Flag("port", "Sets the local listening port").Default("-1").StringVar(&c.config.Port)
	run.Flag("verbose", "Log in debug mode").BoolVar(&c.config.Debug)
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

func (c *SrvRunCmd) dataParentDir() (string, error) {
	parent := os.Getenv("XDG_DATA_HOME")
	if parent != "" {
		return filepath.Join(parent, ".local", "share"), nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}

	if u.HomeDir == "" {
		return "", fmt.Errorf("cannot determine home directory")
	}

	return filepath.Join(u.HomeDir, ".local", "share"), nil
}

func (c *SrvRunCmd) validate() error {
	if c.config.ExtendWithContext {
		if opts.Config.ServerURL() == "" {
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

	c.config.Context = opts.Config

	if opts.Trace {
		c.config.Verbose = true
	}

	if c.config.Port == "-1" {
		p, err := c.getRandomPort()
		if err != nil {
			return err
		}

		c.config.Port = p
	}

	c.config.UserPassword = randomString(32, 32)
	b, err := bcrypt.GenerateFromPassword([]byte(c.config.UserPassword), 5)
	if err != nil {
		return err
	}
	c.config.UserPasswordCrypt = string(b)

	c.config.SystemPassword = randomString(32, 32)
	b, err = bcrypt.GenerateFromPassword([]byte(c.config.SystemPassword), 5)
	if err != nil {
		return err
	}
	c.config.SystemPasswordCrypt = string(b)

	c.config.ServicePassword = randomString(32, 32)
	b, err = bcrypt.GenerateFromPassword([]byte(c.config.ServicePassword), 5)
	if err != nil {
		return err
	}
	c.config.ServicePasswordCrypt = string(b)

	if c.config.JetStream {
		parent, err := c.dataParentDir()
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

	t, err := template.New("server.cfg").Parse(serverRunConfig)
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
	svcName := fmt.Sprintf("%s_service", c.config.Name)
	sysName := fmt.Sprintf("%s_system", c.config.Name)

	if natscontext.IsKnown(c.config.Name) {
		return "", "", "", fmt.Errorf("context %s already exist, choose a new instance name or remove it with 'nats context rm %s'", c.config.Name, c.config.Name)
	}
	if natscontext.IsKnown(svcName) {
		return "", "", "", fmt.Errorf("context %s already exist, choose a new instance name or remove it with 'nats context rm %s'", svcName, svcName)
	}
	if natscontext.IsKnown(sysName) {
		return "", "", "", fmt.Errorf("context %s already exist, choose a new instance name or remove it with 'nats context rm %s'", sysName, sysName)
	}

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

	nctx, _ = natscontext.New(svcName, false,
		natscontext.WithServerURL(url),
		natscontext.WithUser("service"),
		natscontext.WithPassword(c.config.ServicePassword),
		natscontext.WithDescription("Local service access for NATS Development instance"),
		natscontext.WithJSDomain(c.config.JSDomain),
	)
	err = nctx.Save(svcName)
	if err != nil {
		return "", "", "", err
	}

	nctx, _ = natscontext.New(sysName, false,
		natscontext.WithServerURL(url),
		natscontext.WithUser("system"),
		natscontext.WithPassword(c.config.SystemPassword),
		natscontext.WithDescription("System user access for NATS Development instance"),
	)
	err = nctx.Save(nctx.Name)
	if err != nil {
		return "", "", "", err
	}

	return c.config.Name, svcName, sysName, nil
}

func (c *SrvRunCmd) runAction(_ *kingpin.ParseContext) error {
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
	defer natscontext.DeleteContext(u)
	defer natscontext.DeleteContext(s)
	defer natscontext.DeleteContext(svc)

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

	fmt.Println()

	srv.ConfigureLogger()

	srv.Start()

	<-ctx.Done()

	srv.Shutdown()
	srv.WaitForShutdown()

	return nil
}

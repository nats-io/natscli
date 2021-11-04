package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
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
	Name                string
	Port                int
	UserPassword        string
	UserPasswordCrypt   string
	SystemPassword      string
	SystemPasswordCrypt string
	ExtendDemoNetwork   bool
	ExtendNGSNetwork    bool
	JetStream           bool
	Verbose             bool
	Debug               bool
	StoreDir            string
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
}
{{ end }}

accounts {
    USER: {
        jetstream: enabled
        users: [{"user": "local", "password": "{{.UserPasswordCrypt}}"}],
    }

    SYSTEM: {
        users: [{"user": "system", "password": "{{.SystemPasswordCrypt}}"}],
    }
}

leafnodes {
    remotes = [
{{ if .ExtendDemoNetwork }}
        {url: "nats://demo.nats.io:7422", account: "USER"}
{{ end }}
    ]
}
`

func configureServerRunCommand(srv *kingpin.CmdClause) {
	c := &SrvRunCmd{}

	run := srv.Command("run", "Runs a local development NATS server").Hidden().Action(c.runAction)
	run.Arg("name", "Uses a named context for local access to the server").Default("nats_development").StringVar(&c.config.Name)
	run.Flag("extend-demo", "Extends the NATS demo network").BoolVar(&c.config.ExtendDemoNetwork)
	// run.Flag("extend-ngs", "Extends Synadia NGS network").BoolVar(&c.config.ExtendNGSNetwork)
	run.Flag("jetstream", "Enables JetStream support").BoolVar(&c.config.JetStream)
	run.Flag("port", "Sets the local listening port").Default("4222").IntVar(&c.config.Port)
	run.Flag("verbose", "Log verbosely").BoolVar(&c.config.Verbose)
	run.Flag("debug", "Log in debug mode").BoolVar(&c.config.Debug)
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

func (c *SrvRunCmd) prepareConfig() error {
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

	if c.config.JetStream {
		parent, err := c.dataParentDir()
		if err != nil {
			return err
		}
		c.config.StoreDir = filepath.Join(parent, "nats", c.config.Name)
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

func (c *SrvRunCmd) configureContexts(url string) (string, string, error) {
	nctx, _ := natscontext.New(c.config.Name, false,
		natscontext.WithServerURL(url),
		natscontext.WithUser("local"),
		natscontext.WithPassword(c.config.UserPassword),
		natscontext.WithDescription("Local user access for NATS Development instance"),
	)
	err := nctx.Save(nctx.Name)
	if err != nil {
		return "", "", err
	}

	sysName := fmt.Sprintf("%s_system", c.config.Name)
	nctx, _ = natscontext.New(sysName, false,
		natscontext.WithServerURL(url),
		natscontext.WithUser("system"),
		natscontext.WithPassword(c.config.SystemPassword),
		natscontext.WithDescription("System user access for NATS Development instance"),
	)
	err = nctx.Save(nctx.Name)
	if err != nil {
		return "", "", err
	}

	return c.config.Name, sysName, nil
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

	u, s, err := c.configureContexts(srv.ClientURL())
	if err != nil {
		return err
	}
	defer natscontext.DeleteContext(u)
	defer natscontext.DeleteContext(s)

	fmt.Printf("Starting local development NATS Server instance: %s\n", c.config.Name)
	fmt.Println()
	fmt.Printf("        User Credentials: User: local  Password: %s Context: %s\n", c.config.UserPassword, u)
	fmt.Printf("      System Credentials: User: system Password: %s Context: %s\n", c.config.SystemPassword, s)
	fmt.Printf("                     URL: %s\n", srv.ClientURL())
	fmt.Printf("           Configuration: %s\n", tf)
	fmt.Printf("  Extending Demo Network: %v\n", c.config.ExtendDemoNetwork)
	// fmt.Printf("   Extending NGS Network: %v\n", c.config.ExtendNGSNetwork)

	fmt.Println()

	srv.ConfigureLogger()

	srv.Start()

	<-ctx.Done()

	srv.Shutdown()
	srv.WaitForShutdown()

	return nil
}

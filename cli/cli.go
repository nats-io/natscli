package cli

import (
	glog "log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"gopkg.in/alecthomas/kingpin.v2"
)

type command struct {
	Name    string
	Order   int
	Command func(app *kingpin.Application)
}

// Logger provides a plugable logger implementation
type Logger interface {
	Printf(format string, a ...interface{})
	Fatalf(format string, a ...interface{})
	Print(a ...interface{})
	Fatal(a ...interface{})
	Println(a ...interface{})
}

var (
	cheats   = make(map[string]string)
	opts     = &Options{}
	commands = []*command{}
	mu       sync.Mutex
	Version  = "development"
	log      Logger

	// These are persisted by contexts, as properties thereof.
	// So don't include NATS_CONTEXT in this list.
	overrideEnvVars = []string{"NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CREDS", "NATS_NKEY", "NATS_CERT", "NATS_KEY", "NATS_CA", "NATS_TIMEOUT"}
)

func registerCommand(name string, order int, c func(app *kingpin.Application)) {
	mu.Lock()
	commands = append(commands, &command{name, order, c})
	mu.Unlock()
}

// Options configure the CLI
type Options struct {
	// Config is a nats configuration context
	Config *natscontext.Context
	// Servers is the list of servers to connect to
	Servers string
	// Creds is nats credentials to authenticate with
	Creds string
	// TlsCert is the TLS Public Certificate
	TlsCert string
	// TlsKey is the TLS Private Key
	TlsKey string
	// TlsCA is the certificate authority to verify the connection with
	TlsCA string
	// Timeout is how long to wait for operations
	Timeout time.Duration
	// Username is the username or token to connect with
	Username string
	// Password is the password to connect with
	Password string
	// Nkey is the file holding a nkey to connect with
	Nkey string
	// JsApiPrefix is the JetStream API prefix
	JsApiPrefix string
	// JsEventPrefix is the JetStream events prefix
	JsEventPrefix string
	// JsDomain is the domain to connect to
	JsDomain string
	// CfgCtx is the context name to use
	CfgCtx string
	// Trace enables verbose debug logging
	Trace bool

	// Conn sets a prepared connect to connect with
	Conn *nats.Conn
	// Mgr sets a prepared jsm Manager to use for JetStream access
	Mgr *jsm.Manager
	// JSc is a prepared NATS JetStream context to use for KV and Object access
	JSc nats.JetStreamContext
}

// SkipContexts used during tests
var SkipContexts bool

// Configure attaches the cli commands to app, prepare will load the context on demand and should be true unless override nats,
// manager and js context is given.  Disable is a list of command names to skip.
func Configure(app *kingpin.Application, options *Options, prepare bool, logger Logger, disable ...string) error {
	opts = options
	log = logger

	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second
	}

	if logger == nil {
		log = goLogger{}
	}

	sort.Slice(commands, func(i int, j int) bool {
		return commands[i].Name < commands[j].Name
	})

	shouldEnable := func(name string) bool {
		for _, d := range disable {
			if d == name {
				return false
			}
		}

		return true
	}

	for _, c := range commands {
		if shouldEnable(c.Name) {
			c.Command(app)
		}
	}

	if prepare {
		app.PreAction(preAction)
	}

	return nil
}

func preAction(_ *kingpin.ParseContext) (err error) {
	loadContext()

	rand.Seed(time.Now().UnixNano())

	return nil
}

type goLogger struct{}

func (goLogger) Fatalf(format string, a ...interface{}) { glog.Fatalf(format, a...) }
func (goLogger) Printf(format string, a ...interface{}) { glog.Printf(format, a...) }
func (goLogger) Print(a ...interface{})                 { glog.Print(a...) }
func (goLogger) Println(a ...interface{})               { glog.Println(a...) }
func (goLogger) Fatal(a ...interface{})                 { glog.Fatal(a...) }

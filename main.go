package main

import (
	"log"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/nats-io/jetstream/nats/natscontext"
)

var (
	config   *natscontext.Context
	servers  string
	creds    string
	tlsCert  string
	tlsKey   string
	tlsCA    string
	timeout  time.Duration
	version  string
	username string
	password string
	nkey     string
	cfgCtx   string
	ctxError error
	trace    bool

	// used during tests
	skipContexts bool

	overrideEnvVars = []string{"NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CREDS", "NATS_NKEY", "NATS_CERT", "NATS_KEY", "NATS_CA", "NATS_TIMEOUT"}
)

func main() {
	if version == "" {
		version = "development"
	}

	ncli := kingpin.New("nats", "NATS Management Utility")
	ncli.Author("NATS Authors <info@nats.io>")
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
	ncli.Flag("timeout", "Time to wait on responses from NATS").Default("2s").Envar("NATS_TIMEOUT").PlaceHolder("NATS_TIMEOUT").DurationVar(&timeout)
	ncli.Flag("context", "Configuration context").StringVar(&cfgCtx)
	ncli.Flag("trace", "Trace API interactions").BoolVar(&trace)

	ncli.PreAction(prepareConfig)

	log.SetFlags(log.Ltime)

	configureActCommand(ncli)
	configureBackupCommand(ncli)
	configureBenchCommand(ncli)
	configureConsumerCommand(ncli)
	configureCtxCommand(ncli)
	configureEventsCommand(ncli)
	configureLatencyCommand(ncli)
	configurePubCommand(ncli)
	configureRTTCommand(ncli)
	configureReplyCommand(ncli)
	configureRestoreCommand(ncli)
	configureSchemaCommand(ncli)
	configureServerCommand(ncli)
	configureStreamCommand(ncli)
	configureSubCommand(ncli)

	kingpin.MustParse(ncli.Parse(os.Args[1:]))
}

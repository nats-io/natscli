package main

import (
	"log"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	servers  string
	creds    string
	tlsCert  string
	tlsKey   string
	tlsCA    string
	timeout  time.Duration
	version  string
	username string
	password string
	trace    bool
)

func main() {
	if version == "" {
		version = "development"
	}

	ncli := kingpin.New("nats", "NATS Management Utility")
	ncli.Author("NATS Authors <info@nats.io>")
	ncli.Version(version)
	ncli.HelpFlag.Short('h')

	ncli.Flag("server", "NATS servers").Short('s').Default("localhost:4222").Envar("NATS_URL").StringVar(&servers)
	ncli.Flag("user", "Username of Token").Envar("NATS_USER").StringVar(&username)
	ncli.Flag("password", "Password").Envar("NATS_PASSWORD").StringVar(&password)
	ncli.Flag("creds", "User credentials").Envar("NATS_CREDS").StringVar(&creds)
	ncli.Flag("tlscert", "TLS public certificate").Envar("NATS_CERT").ExistingFileVar(&tlsCert)
	ncli.Flag("tlskey", "TLS private key").Envar("NATS_KEY").ExistingFileVar(&tlsCert)
	ncli.Flag("tlsca", "TLS certificate authority chain").Envar("NATS_CA").ExistingFileVar(&tlsCA)
	ncli.Flag("timeout", "Time to wait on responses from NATS").Default("2s").Envar("NATS_TIMEOUT").DurationVar(&timeout)
	ncli.Flag("trace", "Trace the JetStream JSON API interactions").BoolVar(&trace)

	log.SetFlags(log.Ltime)

	configurePubCommand(ncli)
	configureSubCommand(ncli)
	configureReplyCommand(ncli)
	configureBenchCommand(ncli)
	configureServerCommand(ncli)
	configureActCommand(ncli)
	configureEventsCommand(ncli)
	configureStreamCommand(ncli)
	configureConsumerCommand(ncli)
	configureBackupCommand(ncli)
	configureRestoreCommand(ncli)
	configureRTTCommand(ncli)

	kingpin.MustParse(ncli.Parse(os.Args[1:]))
}

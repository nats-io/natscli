package serverdata

import (
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// RequestFunc defines the callback used when connecting to live servers for data
type RequestFunc func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error)

// DataSource abstracts server data retrieval
type DataSource interface {
	Varz(opts server.VarzEventOptions) ([]*server.ServerAPIVarzResponse, error)
	Connz(opts server.ConnzEventOptions) ([]*server.ServerAPIConnzResponse, error)
	Routez(opts server.RoutezEventOptions) ([]*server.ServerAPIRoutezResponse, error)
	Gatewayz(opts server.GatewayzEventOptions) ([]*server.ServerAPIGatewayzResponse, error)
	Leafz(opts server.LeafzEventOptions) ([]*server.ServerAPILeafzResponse, error)
	Subsz(opts server.SubszEventOptions) ([]*server.ServerAPISubszResponse, error)
	Jsz(opts server.JszEventOptions) ([]*server.ServerAPIJszResponse, error)
	Healthz(opts server.HealthzEventOptions) ([]*server.ServerAPIHealthzResponse, error)
	Accountz(opts server.AccountzEventOptions) ([]*server.ServerAPIAccountzResponse, error)
	Statz(opts server.StatszEventOptions) ([]*server.ServerStatsMsg, error)
	CollectAccounts() ([]*server.AccountDetail, error)
	Close() error
}

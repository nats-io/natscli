package serverdata

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Server implements DataSource by querying live servers
type Server struct {
	nc      *nats.Conn
	reqFn   RequestFunc
	waitFor int
}

// NewServer creates a Server data source that delegates requests to the reqFn callback
func NewServer(nc *nats.Conn, reqFn RequestFunc, waitFor int) *Server {
	return &Server{nc: nc, reqFn: reqFn, waitFor: waitFor}
}

func (s *Server) Varz(opts server.VarzEventOptions) ([]*server.ServerAPIVarzResponse, error) {
	return doRequest[server.ServerAPIVarzResponse](s, opts, "$SYS.REQ.SERVER.PING.VARZ")
}

func (s *Server) Connz(opts server.ConnzEventOptions) ([]*server.ServerAPIConnzResponse, error) {
	return doRequest[server.ServerAPIConnzResponse](s, opts, "$SYS.REQ.SERVER.PING.CONNZ")
}

func (s *Server) Routez(opts server.RoutezEventOptions) ([]*server.ServerAPIRoutezResponse, error) {
	return doRequest[server.ServerAPIRoutezResponse](s, opts, "$SYS.REQ.SERVER.PING.ROUTEZ")
}

func (s *Server) Gatewayz(opts server.GatewayzEventOptions) ([]*server.ServerAPIGatewayzResponse, error) {
	return doRequest[server.ServerAPIGatewayzResponse](s, opts, "$SYS.REQ.SERVER.PING.GATEWAYZ")
}

func (s *Server) Leafz(opts server.LeafzEventOptions) ([]*server.ServerAPILeafzResponse, error) {
	return doRequest[server.ServerAPILeafzResponse](s, opts, "$SYS.REQ.SERVER.PING.LEAFZ")
}

func (s *Server) Subsz(opts server.SubszEventOptions) ([]*server.ServerAPISubszResponse, error) {
	return doRequest[server.ServerAPISubszResponse](s, opts, "$SYS.REQ.SERVER.PING.SUBSZ")
}

func (s *Server) Jsz(opts server.JszEventOptions) ([]*server.ServerAPIJszResponse, error) {
	return doRequest[server.ServerAPIJszResponse](s, opts, "$SYS.REQ.SERVER.PING.JSZ")
}

func (s *Server) Healthz(opts server.HealthzEventOptions) ([]*server.ServerAPIHealthzResponse, error) {
	return doRequest[server.ServerAPIHealthzResponse](s, opts, "$SYS.REQ.SERVER.PING.HEALTHZ")
}

func (s *Server) Accountz(opts server.AccountzEventOptions) ([]*server.ServerAPIAccountzResponse, error) {
	return doRequest[server.ServerAPIAccountzResponse](s, opts, "$SYS.REQ.SERVER.PING.ACCOUNTZ")
}

func (s *Server) Statz(opts server.StatszEventOptions) ([]*server.ServerStatsMsg, error) {
	return doRequest[server.ServerStatsMsg](s, opts, "$SYS.REQ.SERVER.PING")
}

// CollectAccounts gathers account-level JetStream metadata from all servers
func (s *Server) CollectAccounts() ([]*server.AccountDetail, error) {
	const pageLimit = 1024

	baseOpts := server.JszEventOptions{
		JSzOptions: server.JSzOptions{
			Accounts: true,
			Streams:  true,
			Consumer: true,
			Config:   true,
			Limit:    pageLimit,
		},
	}

	initialResponses, err := s.Jsz(baseOpts)
	if err != nil && len(initialResponses) == 0 {
		return nil, err
	}

	accountMap := map[string]*server.AccountDetail{}
	mergeAccounts := func(details []*server.AccountDetail) {
		for _, acct := range details {
			if existing, found := accountMap[acct.Name]; found {
				existing.Streams = mergeStreams(existing.Streams, acct.Streams)
			} else {
				cp := *acct
				accountMap[acct.Name] = &cp
			}
		}
	}

	offsets := map[string]int{}
	for _, resp := range initialResponses {
		if resp.Data == nil || resp.Server == nil {
			continue
		}
		mergeAccounts(resp.Data.AccountDetails)
		if len(resp.Data.AccountDetails) == pageLimit {
			offsets[resp.Server.Name] = pageLimit
		}
	}

	for len(offsets) > 0 {
		for name, offset := range offsets {
			opts := baseOpts
			opts.EventFilterOptions.Name = name
			opts.ExactMatch = true
			opts.Offset = offset

			pages, err := doRequest[server.ServerAPIJszResponse](s, opts, "$SYS.REQ.SERVER.PING.JSZ")
			if err != nil {
				return nil, err
			}
			if len(pages) == 0 || pages[0].Data == nil {
				delete(offsets, name)
				continue
			}

			jsz := pages[0]
			mergeAccounts(jsz.Data.AccountDetails)

			if len(jsz.Data.AccountDetails) == pageLimit {
				offsets[name] += pageLimit
			} else {
				delete(offsets, name)
			}
		}
	}

	accounts := make([]*server.AccountDetail, 0, len(accountMap))
	for _, acct := range accountMap {
		accounts = append(accounts, acct)
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Name < accounts[j].Name })
	for _, acct := range accounts {
		sort.Slice(acct.Streams, func(i, j int) bool { return acct.Streams[i].Name < acct.Streams[j].Name })
	}

	return accounts, nil
}

// Close is a noop for connecting to live servers. The caller owns the connections
func (s *Server) Close() error {
	return nil
}

func doRequest[T any](s *Server, opts any, subj string) ([]*T, error) {
	results, err := s.reqFn(opts, subj, s.waitFor, s.nc)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", subj, err)
	}

	responses := make([]*T, 0, len(results))
	for _, raw := range results {
		resp := new(T)
		if err := json.Unmarshal(raw, resp); err != nil {
			return nil, fmt.Errorf("unmarshal %s response: %w", subj, err)
		}
		responses = append(responses, resp)
	}

	return responses, nil
}

func mergeStreams(a, b []server.StreamDetail) []server.StreamDetail {
	seen := map[string]any{}
	var result []server.StreamDetail

	for _, s := range append(a, b...) {
		if _, found := seen[s.Name]; found {
			continue
		}
		seen[s.Name] = struct{}{}
		result = append(result, s)
	}
	return result
}

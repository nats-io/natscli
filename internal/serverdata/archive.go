package serverdata

import (
	"slices"
	"sort"
	"strings"

	"github.com/nats-io/jsm.go/audit/archive"
	"github.com/nats-io/nats-server/v2/server"
)

// Archive implements DataSource by reading from a captured archive.
type Archive struct {
	reader *archive.Reader
}

// NewArchive creates an Archive data source from an archive file.
func NewArchive(archivePath string) (*Archive, error) {
	r, err := archive.NewReader(archivePath)
	if err != nil {
		return nil, err
	}
	return &Archive{reader: r}, nil
}

// Varz builds and filters a Varz response from the archive
func (a *Archive) Varz(opts server.VarzEventOptions) ([]*server.ServerAPIVarzResponse, error) {
	var results []*server.ServerAPIVarzResponse
	_, err := a.reader.EachClusterServerVarz(func(_ *archive.Tag, _ *archive.Tag, cbError error, vz *server.ServerAPIVarzResponse) error {
		if cbError != nil || !matchesFilter(vz.Server, &opts.EventFilterOptions) {
			return nil
		}
		results = append(results, vz)
		return nil
	})
	return results, err
}

// Connz builds and filters a Connz response from the archive
func (a *Archive) Connz(opts server.ConnzEventOptions) ([]*server.ServerAPIConnzResponse, error) {
	seen := map[string]*server.ServerAPIConnzResponse{}
	var results []*server.ServerAPIConnzResponse

	for _, accountName := range a.reader.AccountNames() {
		tags := []*archive.Tag{
			archive.TagAccount(accountName),
			archive.TagAccountConnections(),
		}
		err := archive.ForEachTaggedArtifact(a.reader, tags, func(cz *server.ServerAPIConnzResponse) error {
			if !matchesFilter(cz.Server, &opts.EventFilterOptions) {
				return nil
			}
			for _, conn := range cz.Data.Conns {
				if conn.Account == "" {
					conn.Account = accountName
				}
			}
			if cz.Server == nil {
				return nil
			}
			key := cz.Server.ID
			if base, ok := seen[key]; ok {
				base.Data.Conns = append(base.Data.Conns, cz.Data.Conns...)
			} else {
				seen[key] = cz
				results = append(results, cz)
			}
			return nil
		})
		if err != nil && err != archive.ErrNoMatches {
			return nil, err
		}
	}

	return results, nil
}

// Routez builds and filters a Routez response from the archive
func (a *Archive) Routez(opts server.RoutezEventOptions) ([]*server.ServerAPIRoutezResponse, error) {
	var results []*server.ServerAPIRoutezResponse
	_, err := archive.EachClusterServerArtifact(a.reader, archive.TagServerRoutes(),
		func(_ *archive.Tag, _ *archive.Tag, cbError error, rz *server.ServerAPIRoutezResponse) error {
			if cbError != nil || !matchesFilter(rz.Server, &opts.EventFilterOptions) {
				return nil
			}
			results = append(results, rz)
			return nil
		})
	return results, err
}

// Gatewayz builds and filters a Gatewayz response from the archive
func (a *Archive) Gatewayz(opts server.GatewayzEventOptions) ([]*server.ServerAPIGatewayzResponse, error) {
	var results []*server.ServerAPIGatewayzResponse
	_, err := archive.EachClusterServerArtifact(a.reader, archive.TagServerGateways(),
		func(_ *archive.Tag, _ *archive.Tag, cbError error, gz *server.ServerAPIGatewayzResponse) error {
			if cbError != nil || !matchesFilter(gz.Server, &opts.EventFilterOptions) {
				return nil
			}
			results = append(results, gz)
			return nil
		})
	return results, err
}

// Leafz builds and filters a Leafz response from the archive
func (a *Archive) Leafz(opts server.LeafzEventOptions) ([]*server.ServerAPILeafzResponse, error) {
	var results []*server.ServerAPILeafzResponse
	_, err := a.reader.EachClusterServerLeafz(func(_ *archive.Tag, _ *archive.Tag, cbError error, lz *server.ServerAPILeafzResponse) error {
		if cbError != nil || !matchesFilter(lz.Server, &opts.EventFilterOptions) {
			return nil
		}
		results = append(results, lz)
		return nil
	})
	return results, err
}

// Subsz builds and filters a Subsz response from the archive
func (a *Archive) Subsz(opts server.SubszEventOptions) ([]*server.ServerAPISubszResponse, error) {
	seen := map[string]*server.ServerAPISubszResponse{}
	var results []*server.ServerAPISubszResponse

	_, err := archive.EachClusterServerArtifact(a.reader, archive.TagServerSubs(),
		func(clusterTag *archive.Tag, serverTag *archive.Tag, cbError error, sz *server.ServerAPISubszResponse) error {
			if cbError != nil || !matchesFilter(sz.Server, &opts.EventFilterOptions) {
				return nil
			}
			key := serverKey(clusterTag, serverTag)
			if base, ok := seen[key]; ok {
				base.Data.Subs = append(base.Data.Subs, sz.Data.Subs...)
			} else {
				seen[key] = sz
				results = append(results, sz)
			}
			return nil
		})
	return results, err
}

// Jsz builds and filters a Jsz response from the archive
func (a *Archive) Jsz(opts server.JszEventOptions) ([]*server.ServerAPIJszResponse, error) {
	seen := map[string]*server.ServerAPIJszResponse{}
	var results []*server.ServerAPIJszResponse

	_, err := a.reader.EachClusterServerJsz(func(clusterTag *archive.Tag, serverTag *archive.Tag, cbError error, jsz *server.ServerAPIJszResponse) error {
		if cbError != nil || !matchesFilter(jsz.Server, &opts.EventFilterOptions) {
			return nil
		}
		key := serverKey(clusterTag, serverTag)
		if base, ok := seen[key]; ok {
			base.Data.AccountDetails = append(base.Data.AccountDetails, jsz.Data.AccountDetails...)
		} else {
			seen[key] = jsz
			results = append(results, jsz)
		}
		return nil
	})
	return results, err
}

// Healthz builds and filters a Healthz response from the archive
func (a *Archive) Healthz(opts server.HealthzEventOptions) ([]*server.ServerAPIHealthzResponse, error) {
	var results []*server.ServerAPIHealthzResponse
	_, err := a.reader.EachClusterServerHealthz(func(_ *archive.Tag, _ *archive.Tag, cbError error, hz *server.ServerAPIHealthzResponse) error {
		if cbError != nil || !matchesFilter(hz.Server, &opts.EventFilterOptions) {
			return nil
		}
		results = append(results, hz)
		return nil
	})
	return results, err
}

type accountInfoResponse struct {
	Server *server.ServerInfo  `json:"server"`
	Data   *server.AccountInfo `json:"data,omitempty"`
}

// Accountz builds and filters an Accountz response from the archive.
func (a *Archive) Accountz(opts server.AccountzEventOptions) ([]*server.ServerAPIAccountzResponse, error) {
	if opts.AccountzOptions.Account != "" {
		return a.accountzDetail(opts)
	}

	var results []*server.ServerAPIAccountzResponse
	_, err := a.reader.EachClusterServerAccountz(func(_ *archive.Tag, _ *archive.Tag, cbError error, az *server.ServerAPIAccountzResponse) error {
		if cbError != nil || !matchesFilter(az.Server, &opts.EventFilterOptions) {
			return nil
		}
		results = append(results, az)
		return nil
	})
	return results, err
}

// accountzDetail reads per-account account_info artifacts and wraps them
func (a *Archive) accountzDetail(opts server.AccountzEventOptions) ([]*server.ServerAPIAccountzResponse, error) {
	var results []*server.ServerAPIAccountzResponse
	tags := []*archive.Tag{
		archive.TagAccount(opts.AccountzOptions.Account),
		archive.TagAccountInfo(),
	}

	err := archive.ForEachTaggedArtifact(a.reader, tags, func(air *accountInfoResponse) error {
		if !matchesFilter(air.Server, &opts.EventFilterOptions) {
			return nil
		}
		resp := &server.ServerAPIAccountzResponse{
			Server: air.Server,
			Data: &server.Accountz{
				Account: air.Data,
			},
		}
		results = append(results, resp)
		return nil
	})
	if err == archive.ErrNoMatches {
		return results, nil
	}
	return results, err
}

// Statz builds and filters a Statz response from the archive
// TODO(ploubser:) ActiveAccounts, StaleConnections, StaleConnectionStats, StalledClients
// and ActiveServers are not available in archive data and cannot be added to the response.
func (a *Archive) Statz(opts server.StatszEventOptions) ([]*server.ServerStatsMsg, error) {
	varzResponses, err := a.Varz(server.VarzEventOptions{EventFilterOptions: opts.EventFilterOptions})
	if err != nil {
		return nil, err
	}

	routesByID := map[string]*server.Routez{}
	routezResponses, _ := a.Routez(server.RoutezEventOptions{EventFilterOptions: opts.EventFilterOptions})
	for _, r := range routezResponses {
		if r.Server != nil {
			routesByID[r.Server.ID] = r.Data
		}
	}

	gatewaysByID := map[string]*server.Gatewayz{}
	gatewayzResponses, _ := a.Gatewayz(server.GatewayzEventOptions{EventFilterOptions: opts.EventFilterOptions})
	for _, g := range gatewayzResponses {
		if g.Server != nil {
			gatewaysByID[g.Server.ID] = g.Data
		}
	}

	var results []*server.ServerStatsMsg
	for _, vr := range varzResponses {
		if vr.Server == nil || vr.Data == nil {
			continue
		}

		msg := &server.ServerStatsMsg{
			Server: *vr.Server,
			Stats: server.ServerStats{
				Start:            vr.Data.Start,
				Mem:              vr.Data.Mem,
				Cores:            vr.Data.Cores,
				CPU:              vr.Data.CPU,
				Connections:      vr.Data.Connections,
				TotalConnections: vr.Data.TotalConnections,
				NumSubs:          vr.Data.Subscriptions,
				Sent: server.DataStats{
					MsgBytes: server.MsgBytes{Msgs: vr.Data.OutMsgs, Bytes: vr.Data.OutBytes},
				},
				Received: server.DataStats{
					MsgBytes: server.MsgBytes{Msgs: vr.Data.InMsgs, Bytes: vr.Data.InBytes},
				},
				SlowConsumers:      vr.Data.SlowConsumers,
				SlowConsumersStats: vr.Data.SlowConsumersStats,
				MemLimit:           vr.Data.MemLimit,
				MaxProcs:           vr.Data.MaxProcs,
			},
		}

		if vr.Data.JetStream.Config != nil {
			msg.Stats.JetStream = &vr.Data.JetStream
		}

		if rz, ok := routesByID[vr.Server.ID]; ok {
			for _, ri := range rz.Routes {
				msg.Stats.Routes = append(msg.Stats.Routes, &server.RouteStat{
					ID:   ri.Rid,
					Name: ri.RemoteName,
					Sent: server.DataStats{
						MsgBytes: server.MsgBytes{Msgs: ri.OutMsgs, Bytes: ri.OutBytes},
					},
					Received: server.DataStats{
						MsgBytes: server.MsgBytes{Msgs: ri.InMsgs, Bytes: ri.InBytes},
					},
					Pending: ri.Pending,
				})
			}
		}

		if gz, ok := gatewaysByID[vr.Server.ID]; ok {
			gwStats := map[string]*server.GatewayStat{}

			for gname, conns := range gz.InboundGateways {
				stat := gwStats[gname]
				if stat == nil {
					stat = &server.GatewayStat{Name: gname}
					gwStats[gname] = stat
				}
				stat.NumInbound += len(conns)
				for _, c := range conns {
					if c.Connection != nil {
						stat.Received.Msgs += c.Connection.InMsgs
						stat.Received.Bytes += c.Connection.InBytes
					}
				}
			}

			for gname, gw := range gz.OutboundGateways {
				stat := gwStats[gname]
				if stat == nil {
					stat = &server.GatewayStat{Name: gname}
					gwStats[gname] = stat
				}
				if gw.Connection != nil {
					stat.Sent.Msgs += gw.Connection.OutMsgs
					stat.Sent.Bytes += gw.Connection.OutBytes
				}
			}

			for _, stat := range gwStats {
				msg.Stats.Gateways = append(msg.Stats.Gateways, stat)
			}
		}

		results = append(results, msg)
	}

	return results, nil
}

// CollectAccounts gathers account-level JetStream metadata from the archive
func (a *Archive) CollectAccounts() ([]*server.AccountDetail, error) {
	jszResponses, err := a.Jsz(server.JszEventOptions{
		JSzOptions: server.JSzOptions{
			Accounts: true,
			Streams:  true,
			Consumer: true,
			Config:   true,
		},
	})
	if err != nil {
		return nil, err
	}

	accountMap := map[string]*server.AccountDetail{}
	for _, jsz := range jszResponses {
		if jsz.Data == nil {
			continue
		}
		for _, acct := range jsz.Data.AccountDetails {
			if existing, found := accountMap[acct.Name]; found {
				existing.Streams = mergeArchiveStreams(existing.Streams, acct.Streams)
			} else {
				cp := *acct
				accountMap[acct.Name] = &cp
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

func mergeArchiveStreams(a, b []server.StreamDetail) []server.StreamDetail {
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

func (a *Archive) Close() error {
	return a.reader.Close()
}

func serverKey(clusterTag, serverTag *archive.Tag) string {
	return clusterTag.Value + "/" + serverTag.Value
}

// matchesFilter checks whether a server matches the given EventFilterOptions.
func matchesFilter(info *server.ServerInfo, f *server.EventFilterOptions) bool {
	if info == nil || f == nil {
		return true
	}

	if f.Name != "" && !matchField(info.Name, f.Name, f.ExactMatch) {
		return false
	}
	if f.Host != "" && !matchField(info.Host, f.Host, f.ExactMatch) {
		return false
	}
	if f.Cluster != "" && !matchField(info.Cluster, f.Cluster, f.ExactMatch) {
		return false
	}
	if f.Domain != "" && info.Domain != f.Domain {
		return false
	}
	for _, ft := range f.Tags {
		if !hasTag(info.Tags, ft) {
			return false
		}
	}

	return true
}

func matchField(value, filter string, exact bool) bool {
	if exact {
		return value == filter
	}
	return strings.Contains(value, filter)
}

func hasTag(tags []string, filter string) bool {
	filter = strings.ToLower(strings.TrimSpace(filter))
	return slices.Contains(tags, filter)
}

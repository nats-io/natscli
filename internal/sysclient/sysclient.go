// Copyright 2024 The NATS Authors
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

package sysclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	srvJszSubj = "$SYS.REQ.SERVER.%s.JSZ"
	// Healthz checks server health status.
	srvHealthzSubj        = "$SYS.REQ.SERVER.%s.HEALTHZ"
	DefaultRequestTimeout = 60 * time.Second
)

var (
	ErrValidation      = errors.New("validation error")
	ErrInvalidServerID = errors.New("server with given ID does not exist")
)

type (
	// SysClient can be used to request monitoring data from the server.
	// This is used by the stream-check and consumer-check commands
	SysClient struct {
		nc    *nats.Conn
		trace bool
	}

	FetchOpts struct {
		Timeout     time.Duration
		ReadTimeout time.Duration
		Expected    int
	}

	FetchOpt func(*FetchOpts) error

	JSZResp struct {
		Server server.ServerInfo `json:"server"`
		JSInfo server.JSInfo     `json:"data"`
	}
)

// New creates an instace of SyscClient
func New(nc *nats.Conn, trace bool) SysClient {
	return SysClient{
		nc:    nc,
		trace: trace,
	}
}

// JszPing sends a PING request to the JSZ endpoint and returns parsed responses.
func (s *SysClient) JszPing(opts server.JszEventOptions, fopts ...FetchOpt) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, fetchErr := s.Fetch(subj, payload, fopts...)
	srvJsz := make([]JSZResp, 0, len(resp))
	for _, msg := range resp {
		var jszResp JSZResp
		if err := json.Unmarshal(msg.Data, &jszResp); err != nil {
			return nil, err
		}
		srvJsz = append(srvJsz, jszResp)
	}
	return srvJsz, fetchErr
}

// FetchJszPaged pages through JSZ account data from all servers.
// It calls the provided function for each page of AccountDetail results.
func (s *SysClient) FetchJszPaged(baseOpts server.JszEventOptions, limit int, timeout time.Duration, active int, fn func([]*server.AccountDetail)) error {
	offsets := map[string]int{}
	fopts := []FetchOpt{fetchTimeout(timeout)}
	if active > 0 {
		fopts = append(fopts, fetchExpected(active))
	}
	baseOpts.Limit = limit

	// Initial request
	initialPages, err := s.JszPing(baseOpts, fopts...)
	if err != nil && len(initialPages) == 0 {
		return err
	}

	for _, page := range initialPages {
		name := page.Server.Name
		fn(page.JSInfo.AccountDetails)
		if len(page.JSInfo.AccountDetails) == baseOpts.Limit {
			offsets[name] = baseOpts.Offset + baseOpts.Limit
		}
	}

	fopts = append(fopts, fetchExpected(1))

	// iterate through all the servers that still have pages
	for len(offsets) > 0 {
		for name, offset := range offsets {
			opts := baseOpts
			opts.EventFilterOptions.Name = name
			opts.ExactMatch = true
			opts.Offset = offset

			page, err := s.JszPing(opts, fopts...)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				delete(offsets, name)
				continue
			}

			jsz := page[0]
			fn(jsz.JSInfo.AccountDetails)

			if len(jsz.JSInfo.AccountDetails) == baseOpts.Limit {
				offsets[name] += baseOpts.Limit
			} else {
				delete(offsets, name)
			}
		}
	}

	return nil
}

// CollectClusterAccounts gathers account-level JetStream metadata from the cluster.
// It fetches paged JSZ responses and merges streams across pages for each account.
func (s *SysClient) CollectClusterAccounts(timeout time.Duration, active int) ([]*server.AccountDetail, error) {
	accountMap := map[string]*server.AccountDetail{}
	opts := server.JszEventOptions{
		JSzOptions: server.JSzOptions{
			Accounts: true,
			Streams:  true,
			Consumer: true,
			Config:   true,
		},
	}

	err := s.FetchJszPaged(opts, 1024, timeout, active, func(pages []*server.AccountDetail) {
		for _, acct := range pages {
			if existing, found := accountMap[acct.Name]; found {
				existing.Streams = mergeStreams(existing.Streams, acct.Streams)
			} else {
				copy := *acct
				accountMap[acct.Name] = &copy
			}
		}
	})

	if err != nil {
		return nil, err
	}

	var accounts []*server.AccountDetail
	for _, acct := range accountMap {
		accounts = append(accounts, acct)
	}

	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Name < accounts[j].Name })
	for _, acct := range accounts {
		sort.Slice(acct.Streams, func(i, j int) bool { return acct.Streams[i].Name < acct.Streams[j].Name })
	}

	return accounts, nil
}

// Fetch publishes a request to the given subject and collects replies.
// If FetchOpts.Expected > 0, it blocks until that number of replies is received or the timeout elapses.
// If FetchOpts.Expected == 0, it collects as many replies as possible until the discovery interval elapses with no new data.
func (s *SysClient) Fetch(subject string, data []byte, opts ...FetchOpt) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: expected subject 0", ErrValidation)
	}

	reqOpts := defaultFetchOpts()
	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), reqOpts.Timeout)
	defer cancel()

	inbox := nats.NewInbox()
	msgsChan := make(chan *nats.Msg, 256)
	res := make([]*nats.Msg, 0, 32)

	sub, err := s.nc.Subscribe(inbox, func(msg *nats.Msg) {
		if s.trace {
			log.Printf("<<< %q", msg.Data)
		}
		msgsChan <- msg
	})
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	if s.trace {
		log.Printf(">>> %s: %s", subject, string(data))
	}
	if err := s.nc.PublishRequest(subject, inbox, data); err != nil {
		return nil, err
	}

	if reqOpts.Expected == 0 {
		return s.fetchUntilTimeout(ctx, msgsChan, res, reqOpts)
	}
	return s.fetchUntilExpected(ctx, msgsChan, res, reqOpts)
}

func (s *SysClient) fetchUntilTimeout(ctx context.Context, msgsChan <-chan *nats.Msg, res []*nats.Msg, reqOpts *FetchOpts) ([]*nats.Msg, error) {
	defaultInterval := 2 * time.Second
	discoveryInterval := max(min(defaultInterval, reqOpts.Timeout/2), 10*time.Millisecond)

	timer := time.NewTimer(discoveryInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return res, fmt.Errorf("server request timed out")
		case <-timer.C:
			return res, nil
		case msg := <-msgsChan:
			if msg.Header.Get("Status") == "503" {
				return nil, fmt.Errorf("server request on subject %q failed: %w", msg.Subject, ErrInvalidServerID)
			}
			res = append(res, msg)
			timer.Reset(discoveryInterval)
		}
	}
}

func (s *SysClient) fetchUntilExpected(ctx context.Context, msgsChan <-chan *nats.Msg, res []*nats.Msg, reqOpts *FetchOpts) ([]*nats.Msg, error) {
	timer := time.NewTimer(reqOpts.ReadTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return res, fmt.Errorf("server request timed out")
		case <-timer.C:
			return res, fmt.Errorf("server request timed out (received %d of %d expected)", len(res), reqOpts.Expected)
		case msg := <-msgsChan:
			if msg.Header.Get("Status") == "503" {
				return nil, fmt.Errorf("server request on subject %q failed: %w", msg.Subject, ErrInvalidServerID)
			}
			res = append(res, msg)
			if len(res) >= reqOpts.Expected {
				return res, nil
			}
			timer.Reset(reqOpts.ReadTimeout)
		}
	}
}

func (s *SysClient) Healthz(id string, opts server.HealthzOptions) (*HealthzResp, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: server id cannot be empty", ErrValidation)
	}
	subj := fmt.Sprintf(srvHealthzSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.nc.Request(subj, payload, DefaultRequestTimeout)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidServerID, id)
		}
		return nil, err
	}
	var healthzResp HealthzResp
	if err := json.Unmarshal(resp.Data, &healthzResp); err != nil {
		return nil, err
	}

	return &healthzResp, nil
}

func (s *SysClient) FindServers(stdin bool, expected int, timeout time.Duration, readTimeout time.Duration, getConsumer bool) ([]JSZResp, error) {
	var err error
	servers := []JSZResp{}

	if stdin {
		if expected == 0 {
			expected = 999
		}

		decoder := json.NewDecoder(os.Stdin)

		for i := 0; i < expected; i++ {
			var jszResp JSZResp
			if err := decoder.Decode(&jszResp); err != nil {
				if err == io.EOF {
					break
				}
				return servers, err
			}
			servers = append(servers, jszResp)
		}
	} else {
		jszOptions := server.JSzOptions{
			Streams:    true,
			RaftGroups: true,
		}

		if getConsumer {
			jszOptions.Consumer = true
		}

		fetchTimeout := fetchTimeout(timeout * time.Second)
		fetchExpected := fetchExpected(expected)
		fetchReadTimeout := fetchReadTimeout(readTimeout * time.Second)
		servers, err = s.JszPing(server.JszEventOptions{
			JSzOptions: jszOptions,
		}, fetchTimeout, fetchReadTimeout, fetchExpected)
		if err != nil {
			return servers, err
		}
	}

	return servers, nil
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

func defaultFetchOpts() *FetchOpts {
	return &FetchOpts{
		Timeout:     5 * time.Second,
		ReadTimeout: 2 * time.Second,
		Expected:    0,
	}
}

func fetchTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: timeout has to be greater than 0", ErrValidation)
		}
		opts.Timeout = timeout
		return nil
	}
}

func fetchReadTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: read timeout has to be greater than 0", ErrValidation)
		}
		opts.ReadTimeout = timeout
		return nil
	}
}

func fetchExpected(expected int) FetchOpt {
	return func(opts *FetchOpts) error {
		if expected <= 0 {
			return fmt.Errorf("%w: expected request count has to be greater than 0", ErrValidation)
		}
		opts.Expected = expected
		return nil
	}
}

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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
		nc *nats.Conn
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

func New(nc *nats.Conn) SysClient {
	return SysClient{
		nc: nc,
	}
}

func (s *SysClient) JszPing(opts server.JszEventOptions, fopts ...FetchOpt) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.Fetch(subj, payload, fopts...)
	if err != nil {
		return nil, err
	}
	srvJsz := make([]JSZResp, 0, len(resp))
	for _, msg := range resp {
		var jszResp JSZResp
		if err := json.Unmarshal(msg.Data, &jszResp); err != nil {
			return nil, err
		}
		srvJsz = append(srvJsz, jszResp)
	}
	return srvJsz, nil
}

func (s *SysClient) Fetch(subject string, data []byte, opts ...FetchOpt) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: expected subject 0", ErrValidation)
	}

	conn := s.nc
	reqOpts := &FetchOpts{}
	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	inbox := nats.NewInbox()
	res := make([]*nats.Msg, 0)
	msgsChan := make(chan *nats.Msg, 100)

	readTimer := time.NewTimer(reqOpts.ReadTimeout)
	sub, err := conn.Subscribe(inbox, func(msg *nats.Msg) {
		readTimer.Reset(reqOpts.ReadTimeout)
		msgsChan <- msg
	})
	defer sub.Unsubscribe()

	if err := conn.PublishRequest(subject, inbox, data); err != nil {
		return nil, err
	}

	for {
		select {
		case msg := <-msgsChan:
			if msg.Header.Get("Status") == "503" {
				return nil, fmt.Errorf("server request on subject %q failed: %w", subject, err)
			}
			res = append(res, msg)
			if reqOpts.Expected != -1 && len(res) == reqOpts.Expected {
				return res, nil
			}
		case <-readTimer.C:
			return res, nil
		case <-time.After(reqOpts.Timeout):
			return res, nil
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
		reader := bufio.NewReader(os.Stdin)

		if expected == 0 {
			expected = 999
		}

		for i := 0; i < expected; i++ {
			data, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return servers, err
			}
			if len(data) > 0 {
				var jszResp JSZResp
				if err := json.Unmarshal([]byte(data), &jszResp); err != nil {
					return servers, err
				}
				servers = append(servers, jszResp)
			}
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
			log.Fatal(err)
		}
	}

	return servers, nil
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

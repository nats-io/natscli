// Copyright 2025 The NATS Authors
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

package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/natscli/options"
)

func TestBenchProcessActionArgsDuration(t *testing.T) {
	// processActionArgs consults the global options for the active context, so
	// provide an empty one and restore whatever was there afterwards.
	origOpts := options.DefaultOptions
	t.Cleanup(func() { options.DefaultOptions = origOpts })

	nctx, err := natscontext.New("test", false)
	if err != nil {
		t.Fatalf("creating test context: %v", err)
	}
	options.DefaultOptions = &options.Options{Config: nctx}

	tests := []struct {
		name        string
		numMsg      int64
		throughput  int
		duration    time.Duration
		expectErr   string
		expectedMsg int64
	}{
		{
			name:        "duration derives msgs from throughput",
			numMsg:      100000,
			throughput:  50000,
			duration:    30 * time.Second,
			expectedMsg: 1500000,
		},
		{
			name:        "duration takes precedence over msgs",
			numMsg:      42,
			throughput:  1000,
			duration:    2 * time.Second,
			expectedMsg: 2000,
		},
		{
			name:      "duration without throughput errors",
			numMsg:    100000,
			duration:  10 * time.Second,
			expectErr: "--duration requires --throughput to be set",
		},
		{
			name:       "negative duration errors",
			numMsg:     100000,
			throughput: 50000,
			duration:   -1 * time.Second,
			expectErr:  "--duration cannot be negative",
		},
		{
			name:       "duration too short for throughput rounds to zero",
			numMsg:     100000,
			throughput: 1,
			duration:   100 * time.Millisecond,
			expectErr:  "results in zero messages",
		},
		{
			name:        "no duration leaves msgs unchanged",
			numMsg:      777,
			throughput:  50000,
			expectedMsg: 777,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &benchCmd{
				numMsg:     tc.numMsg,
				throughput: tc.throughput,
				duration:   tc.duration,
			}

			err := c.processActionArgs()

			if tc.expectErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectErr)
				}
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Fatalf("expected error containing %q, got %q", tc.expectErr, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if c.numMsg != tc.expectedMsg {
				t.Fatalf("expected numMsg %d, got %d", tc.expectedMsg, c.numMsg)
			}
		})
	}
}

// TestBenchDurationWithoutMsgsFlag ensures --msgs is optional: supplying only
// --duration (with --throughput) must parse cleanly through the real command
// wiring, so users are not forced to also pass --msgs.
func TestBenchDurationWithoutMsgsFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "core pub with only duration and throughput",
			args: []string{"bench", "pub", "foo", "--throughput", "1000", "--duration", "2s"},
		},
		{
			name: "js sync pub with only duration and throughput",
			args: []string{"bench", "js", "pub", "sync", "foo", "--throughput", "1000", "--duration", "2s"},
		},
		{
			name: "kv put with only duration and throughput",
			args: []string{"bench", "kv", "put", "--throughput", "1000", "--duration", "2s"},
		},
	}

	origOpts := options.DefaultOptions
	t.Cleanup(func() { options.DefaultOptions = origOpts })
	options.DefaultOptions = &options.Options{NoCheats: true}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := fisk.New("test", "test")
			configureBenchCommand(app)

			if _, err := app.ParseContext(tc.args); err != nil {
				t.Fatalf("expected %v to parse without --msgs, got: %v", tc.args, err)
			}
		})
	}
}

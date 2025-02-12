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

package util

import (
	"github.com/nats-io/nats.go"
	"testing"
)

func TestParseStringsToHeader(t *testing.T) {
	_, err := ParseStringsToHeader([]string{"A:1", "B"}, 0)
	if err == nil || err.Error() != `invalid header "B"` {
		t.Fatalf("expected invalid header error, got: %v", err)
	}

	res, err := ParseStringsToHeader([]string{"A:1", "B:2", "C:{{ Count }}"}, 10)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if res.Get("A") != "1" {
		t.Fatalf("expected 1, got: %v", res.Get("A"))
	}

	if res.Get("B") != "2" {
		t.Fatalf("expected 2, got: %v", res.Get("B"))
	}

	if res.Get("C") != "10" {
		t.Fatalf("expected 10, got: %v", res.Get("C"))
	}
}

func TestParseStringsToMsgHeader(t *testing.T) {
	msg := nats.NewMsg("")
	err := ParseStringsToMsgHeader([]string{"A:1", "B"}, 0, msg)
	if err == nil || err.Error() != `invalid header "B"` {
		t.Fatalf("expected invalid header error, got: %v", err)
	}

	err = ParseStringsToMsgHeader([]string{"A:1", "B:2", "C:{{ Count }}"}, 10, msg)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if msg.Header.Get("A") != "1" {
		t.Fatalf("expected 1, got: %v", msg.Header.Get("A"))
	}

	if msg.Header.Get("B") != "2" {
		t.Fatalf("expected 2, got: %v", msg.Header.Get("B"))
	}

	if msg.Header.Get("C") != "10" {
		t.Fatalf("expected 10, got: %v", msg.Header.Get("C"))
	}
}

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

import "testing"

func TestRandomString(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if len(RandomString(1024, 1024)) != 1024 {
			t.Fatalf("got a !1024 length string")
		}
	}

	for i := 0; i < 1000; i++ {
		n := RandomString(2024, 1024)
		if len(n) > 2024 {
			t.Fatalf("got a > 2024 length string")
		}

		if len(n) < 1024 {
			t.Fatalf("got a < 1024 length string (%d)", len(n))
		}
	}

	for i := 0; i < 1000; i++ {
		n := RandomString(1024, 2024)
		if len(n) > 2024 {
			t.Fatalf("got a > 2024 length string")
		}

		if len(n) < 1024 {
			t.Fatalf("got a < 1024 length string (%d)", len(n))
		}
	}
}

// Copyright 2026 The NATS Authors
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

//go:build fips

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var fipsEnv = map[string]string{"GODEBUG": "fips140=only"}

func expectFIPSError(t *testing.T, out []byte, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected command to fail under FIPS, got success.\noutput:\n%s", out)
	}
	if !strings.Contains(string(out), "FIPS") {
		t.Fatalf("expected FIPS error in output, got:\n%s", out)
	}
}

func TestFIPSNkeyGenCurveRejected(t *testing.T) {
	out, err := runNatsCliCore(t, "", fipsEnv, "auth", "nkey", "gen", "curve")
	expectFIPSError(t, out, err)
	if !strings.Contains(string(out), "X25519") {
		t.Fatalf("expected X25519 in error output, got:\n%s", out)
	}
}

func TestFIPSNkeyGenX25519Rejected(t *testing.T) {
	out, err := runNatsCliCore(t, "", fipsEnv, "auth", "nkey", "gen", "x25519")
	expectFIPSError(t, out, err)
}

func TestFIPSNkeyGenUserAllowed(t *testing.T) {
	tmp := t.TempDir()
	outFile := filepath.Join(tmp, "user.nk")
	out, err := runNatsCliCore(t, "", fipsEnv, "auth", "nkey", "gen", "user", "--output", outFile)
	if err != nil {
		t.Fatalf("nats auth nkey gen user failed under FIPS: %v\noutput:\n%s", err, out)
	}
	data, readErr := os.ReadFile(outFile)
	if readErr != nil {
		t.Fatalf("could not read generated user seed: %v", readErr)
	}
	if len(data) == 0 || data[0] != 'S' {
		t.Fatalf("expected seed file to start with 'S', got %q", string(data[:1]))
	}
}

func TestFIPSServerPasswdRejected(t *testing.T) {
	out, err := runNatsCliCore(t, "", fipsEnv, "server", "passwd", "--pass", "longenoughpassword", "--cost", "4")
	expectFIPSError(t, out, err)
	if !strings.Contains(string(out), "bcrypt") {
		t.Fatalf("expected bcrypt in error output, got:\n%s", out)
	}
}

func TestFIPSNkeyGenCurveAllowedWithoutFIPS(t *testing.T) {
	out, err := runNatsCliCore(t, "", nil, "auth", "nkey", "gen", "curve")
	if err != nil {
		t.Fatalf("nats auth nkey gen curve should succeed without FIPS, got error: %v\noutput:\n%s", err, out)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(out)), "S") {
		t.Fatalf("expected curve seed starting with 'S', got:\n%s", out)
	}
}

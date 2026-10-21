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

package main

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func auditGatherCmd(srv *server.Server, archive string, args string) string {
	return fmt.Sprintf("--server='%s' %s --timeout 500ms audit gather --no-progress -o '%s' %s", srv.ClientURL(), sysUserCreds, archive, args)
}

func archiveProfiles(t *testing.T, archive string) []string {
	t.Helper()

	zr, err := zip.OpenReader(archive)
	checkErr(t, err, "opening archive failed: %v", err)
	defer zr.Close()

	var profiles []string
	for _, f := range zr.File {
		if !strings.HasPrefix(f.Name, "capture/profiles/") || !strings.HasSuffix(f.Name, ".prof") {
			continue
		}
		_, name, _ := strings.Cut(strings.TrimSuffix(filepath.Base(f.Name), ".prof"), "__")
		if !slices.Contains(profiles, name) {
			profiles = append(profiles, name)
		}
	}
	slices.Sort(profiles)

	return profiles
}

func archiveHasServerEndpoints(t *testing.T, archive string) bool {
	t.Helper()

	zr, err := zip.OpenReader(archive)
	checkErr(t, err, "opening archive failed: %v", err)
	defer zr.Close()

	return slices.ContainsFunc(zr.File, func(f *zip.File) bool {
		return strings.Contains(filepath.ToSlash(f.Name), "/variables/")
	})
}

func writeAuditGatherConfig(t *testing.T, content string) string {
	t.Helper()

	file := filepath.Join(t.TempDir(), "gather.yaml")
	err := os.WriteFile(file, []byte(content), 0600)
	checkErr(t, err, "writing config failed: %v", err)

	return file
}

func TestAuditGather(t *testing.T) {
	withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
		config := writeAuditGatherConfig(t, "server_endpoints: false\nserver_profiles:\n  profiles: [heap, mutex]\n")

		for _, tc := range []struct {
			name      string
			args      string
			env       map[string]string
			profiles  []string
			endpoints bool
		}{
			{"default", "--cpu-profile-duration 500ms", nil, []string{"allocs", "block", "cpu", "goroutine_1", "heap", "mutex", "threadcreate"}, true},
			{"profile", "--profile goroutine_2 --profile heap", nil, []string{"goroutine_2", "heap"}, true},
			{"skip-profile", "--skip-profile cpu --skip-profile block", nil, []string{"allocs", "goroutine_1", "heap", "mutex", "threadcreate"}, true},
			{"config file", fmt.Sprintf("--config '%s'", config), nil, []string{"heap", "mutex"}, false},
			{"flags override config file", fmt.Sprintf("--config '%s' --profile allocs --server-endpoints", config), nil, []string{"allocs"}, true},
			{"config from environment with skip-profile", "--skip-profile mutex", map[string]string{"NATS_AUDIT_GATHER_CONFIG": config}, []string{"heap"}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archive := filepath.Join(t.TempDir(), "audit.zip")
				output, err := runNatsCliCore(t, "", tc.env, auditGatherCmd(srv, archive, tc.args))
				checkErr(t, err, "gather failed: %v: %s", err, output)

				profiles := archiveProfiles(t, archive)
				if !reflect.DeepEqual(profiles, tc.profiles) {
					t.Errorf("expected profiles %v, got %v", tc.profiles, profiles)
				}
				if archiveHasServerEndpoints(t, archive) != tc.endpoints {
					t.Errorf("expected server endpoints captured to be %t", tc.endpoints)
				}
			})
		}

		for _, tc := range []struct {
			name   string
			args   string
			config string
			want   string
		}{
			{"profile and skip-profile", "--profile heap --skip-profile cpu", "", "--profile and --skip-profile cannot be used together"},
			{"unknown profile flag", "--profile goroutine", "", "enum value must be one of"},
			{"profile with no-server-profiles", "--no-server-profiles --profile heap", "", "--profile cannot be used with --no-server-profiles"},
			{"skip-profile with profiles disabled in file", "--skip-profile cpu", "server_profiles:\n  enabled: false\n", "--skip-profile cannot be used when"},
			{"cpu duration flag out of range", "--cpu-profile-duration 16s", "", "cpu profile duration must be more than 0s and at most 15s, got 16s"},
			{"unknown key", "", "server_profiles:\n  profile: [heap]\n", "field profile not found"},
			{"unknown profile in file", "", "server_profiles:\n  profiles: [goroutine]\n", `unknown server profile "goroutine"`},
			{"numeric cpu duration", "", "server_profiles:\n  cpu_duration: 5\n", "cannot unmarshal !!int `5` into time.Duration"},
			{"cpu duration in file out of range", "", "server_profiles:\n  cpu_duration: 20s\n", "cpu profile duration must be more than 0s and at most 15s, got 20s"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archive := filepath.Join(t.TempDir(), "audit.zip")
				args := tc.args
				if tc.config != "" {
					args = fmt.Sprintf("%s --config '%s'", args, writeAuditGatherConfig(t, tc.config))
				}

				err := runNatsCliWithError(t, auditGatherCmd(srv, archive, args))
				if err == nil {
					t.Fatalf("expected an error")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("expected error containing %q, got: %v", tc.want, err)
				}
				if _, err := os.Stat(archive); err == nil {
					t.Errorf("archive was written")
				}
			})
		}

		return nil
	})
}

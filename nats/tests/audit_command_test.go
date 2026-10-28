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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/jsm.go/audit/archive"
	gatherer "github.com/nats-io/jsm.go/audit/gather"
	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func auditGatherCmd(srv *server.Server, archivePath string, args string) string {
	return fmt.Sprintf("--server='%s' %s --timeout 500ms audit gather --no-progress -o '%s' %s", srv.ClientURL(), sysUserCreds, archivePath, args)
}

func archiveProfiles(t *testing.T, path string) []string {
	t.Helper()

	src, err := serverdata.NewAuditArchive(path)
	checkErr(t, err, "opening archive failed: %v", err)
	defer src.Close()

	var profiles []string
	for _, profile := range gatherer.ServerProfiles {
		name, debug := serverdata.SplitProfileName(string(profile))
		res, err := src.Profilez(server.ProfilezEventOptions{ProfilezOptions: server.ProfilezOptions{Name: name, Debug: debug}})
		checkErr(t, err, "reading %s profiles failed: %v", profile, err)
		if len(res) > 0 {
			profiles = append(profiles, string(profile))
		}
	}
	slices.Sort(profiles)

	return profiles
}

func archiveHasServerEndpoints(t *testing.T, path string) bool {
	t.Helper()

	ar, err := archive.NewReader(path)
	checkErr(t, err, "opening archive failed: %v", err)
	defer ar.Close()

	count, err := ar.EachClusterServerVarz(func(_ *archive.Tag, _ *archive.Tag, _ error, _ *server.ServerAPIVarzResponse) error {
		return nil
	})
	checkErr(t, err, "reading server endpoints failed: %v", err)

	return count > 0
}

func archiveSubszPageSizes(t *testing.T, path string) ([]int, int) {
	t.Helper()

	ar, err := archive.NewReader(path)
	checkErr(t, err, "opening archive failed: %v", err)
	defer ar.Close()

	var sizes []int
	var total int
	_, err = archive.EachClusterServerArtifact(ar, archive.TagServerSubs(), func(_ *archive.Tag, _ *archive.Tag, err error, sz *server.ServerAPISubszResponse) error {
		if err != nil {
			return err
		}
		sizes = append(sizes, len(sz.Data.Subs))
		total = sz.Data.Total
		return nil
	})
	checkErr(t, err, "reading SUBSZ pages failed: %v", err)

	return sizes, total
}

func writeAuditGatherConfig(t *testing.T, content string) string {
	t.Helper()

	file := filepath.Join(t.TempDir(), "gather.json")
	err := os.WriteFile(file, []byte(content), 0600)
	checkErr(t, err, "writing config failed: %v", err)

	return file
}

func TestAuditGather(t *testing.T) {
	withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
		config := writeAuditGatherConfig(t, `{"server_endpoints": false, "server_profiles": {"profiles": ["heap", "mutex"]}}`)
		disabled := writeAuditGatherConfig(t, `{"server_profiles": {"enabled": false}}`)

		for _, tc := range []struct {
			name      string
			args      string
			env       map[string]string
			profiles  []string
			endpoints bool
		}{
			{"profile", "--profile goroutine_2 --profile heap", nil, []string{"goroutine_2", "heap"}, true},
			{"skip-profile", "--skip-profile cpu --skip-profile block", nil, []string{"allocs", "goroutine_1", "heap", "mutex", "threadcreate"}, true},
			{"config file", fmt.Sprintf("--config '%s'", config), nil, []string{"heap", "mutex"}, false},
			{"flags override config file", fmt.Sprintf("--config '%s' --profile allocs --server-endpoints", config), nil, []string{"allocs"}, true},
			{"config from environment with skip-profile", "--skip-profile mutex", map[string]string{"NATS_AUDIT_GATHER_CONFIG": config}, []string{"heap"}, false},
			{"server-profiles overrides config file", fmt.Sprintf("--config '%s' --server-profiles --profile heap", disabled), nil, []string{"heap"}, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archivePath := filepath.Join(t.TempDir(), "audit.zip")
				output, err := runNatsCliCore(t, "", tc.env, auditGatherCmd(srv, archivePath, tc.args))
				checkErr(t, err, "gather failed: %v: %s", err, output)

				profiles := archiveProfiles(t, archivePath)
				if !reflect.DeepEqual(profiles, tc.profiles) {
					t.Errorf("expected profiles %v, got %v", tc.profiles, profiles)
				}
				if archiveHasServerEndpoints(t, archivePath) != tc.endpoints {
					t.Errorf("expected server endpoints captured to be %t", tc.endpoints)
				}
			})
		}

		full := writeAuditGatherConfig(t, `{"cluster": "c1", "server_endpoints": false, "account_endpoints": false, "streams": false, "consumers": false, "details": false, "subsz_page_size": 100, "server_profiles": {"enabled": false, "cpu_duration": "1s", "profiles": ["heap"]}}`)
		noProfiles := writeAuditGatherConfig(t, `{"server_profiles": {"enabled": false, "profiles": []}}`)
		durationOnly := writeAuditGatherConfig(t, `{"server_profiles": {"cpu_duration": "1s"}}`)

		for _, tc := range []struct {
			name string
			args string
			want string
		}{
			{"write config from file and flags", fmt.Sprintf("--config '%s' --skip-profile mutex --no-streams --cpu-profile-duration 2s", config), `{"cluster":"","server_endpoints":false,"account_endpoints":true,"streams":false,"consumers":true,"details":true,"subsz_page_size":4096,"server_profiles":{"enabled":true,"cpu_duration":"2s","profiles":["heap"]}}`},
			{"write config with every key from file", fmt.Sprintf("--config '%s'", full), `{"cluster":"c1","server_endpoints":false,"account_endpoints":false,"streams":false,"consumers":false,"details":false,"subsz_page_size":100,"server_profiles":{"enabled":false,"cpu_duration":"1s","profiles":["heap"]}}`},
			{"write config with flags overriding every key", fmt.Sprintf("--config '%s' --cluster c2 --server-endpoints --account-endpoints --streams --consumers --details --server-profiles --cpu-profile-duration 3s --profile mutex --subsz-page-size 200", full), `{"cluster":"c2","server_endpoints":true,"account_endpoints":true,"streams":true,"consumers":true,"details":true,"subsz_page_size":200,"server_profiles":{"enabled":true,"cpu_duration":"3s","profiles":["mutex"]}}`},
			{"write config with default profiles when file omits them", fmt.Sprintf("--config '%s'", durationOnly), `{"cluster":"","server_endpoints":true,"account_endpoints":true,"streams":true,"consumers":true,"details":true,"subsz_page_size":4096,"server_profiles":{"enabled":true,"cpu_duration":"1s","profiles":["goroutine_1","heap","allocs","mutex","threadcreate","block","cpu"]}}`},
			{"write config with maximum cpu duration", "--cpu-profile-duration 15s", `{"cluster":"","server_endpoints":true,"account_endpoints":true,"streams":true,"consumers":true,"details":true,"subsz_page_size":4096,"server_profiles":{"enabled":true,"cpu_duration":"15s","profiles":["goroutine_1","heap","allocs","mutex","threadcreate","block","cpu"]}}`},
			{"write config with no profiles while disabled", fmt.Sprintf("--config '%s' --no-server-profiles", noProfiles), `{"cluster":"","server_endpoints":true,"account_endpoints":true,"streams":true,"consumers":true,"details":true,"subsz_page_size":4096,"server_profiles":{"enabled":false,"cpu_duration":"5s","profiles":[]}}`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archivePath := filepath.Join(t.TempDir(), "audit.zip")
				written := filepath.Join(t.TempDir(), "written.json")

				output := runNatsCli(t, auditGatherCmd(srv, archivePath, fmt.Sprintf("%s --write-config '%s'", tc.args, written)))
				if len(output) != 0 {
					t.Errorf("expected no output, got: %s", output)
				}
				if _, err := os.Stat(archivePath); err == nil {
					t.Errorf("archive was written")
				}

				content, err := os.ReadFile(written)
				checkErr(t, err, "reading written config failed: %v", err)

				var compact bytes.Buffer
				err = json.Compact(&compact, content)
				checkErr(t, err, "written config is not valid JSON: %v", err)
				if compact.String() != tc.want {
					t.Errorf("expected config:\n%s\ngot:\n%s", tc.want, compact.String())
				}
			})
		}

		for _, tc := range []struct {
			name     string
			args     string
			pageSize int
			paged    bool
		}{
			{"subsz default page size", "", 4096, false},
			{"subsz page size flag", "--subsz-page-size 5", 5, true},
			{"subsz page size from file", fmt.Sprintf("--config '%s'", writeAuditGatherConfig(t, `{"subsz_page_size": 5}`)), 5, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archivePath := filepath.Join(t.TempDir(), "audit.zip")
				output, err := runNatsCliCore(t, "", nil, auditGatherCmd(srv, archivePath, tc.args+" --no-server-profiles"))
				checkErr(t, err, "gather failed: %v: %s", err, output)

				pages, total := archiveSubszPageSizes(t, archivePath)
				if len(pages) == 0 {
					t.Fatalf("no SUBSZ pages captured")
				}
				if (len(pages) > 1) != tc.paged {
					t.Errorf("expected paged to be %t, got %d pages", tc.paged, len(pages))
				}
				captured := 0
				for _, size := range pages {
					if size > tc.pageSize {
						t.Errorf("expected at most %d subscriptions per page, got %d", tc.pageSize, size)
					}
					captured += size
				}
				if captured != total {
					t.Errorf("expected %d subscriptions captured, got %d", total, captured)
				}
			})
		}

		t.Run("gather with written config", func(t *testing.T) {
			archivePath := filepath.Join(t.TempDir(), "audit.zip")
			written := filepath.Join(t.TempDir(), "written.json")

			runNatsCli(t, auditGatherCmd(srv, archivePath, fmt.Sprintf("--profile heap --write-config '%s'", written)))

			output, err := runNatsCliCore(t, "", nil, auditGatherCmd(srv, archivePath, fmt.Sprintf("--config '%s'", written)))
			checkErr(t, err, "gather failed: %v: %s", err, output)

			profiles := archiveProfiles(t, archivePath)
			if !reflect.DeepEqual(profiles, []string{"heap"}) {
				t.Errorf("expected profiles [heap], got %v", profiles)
			}

			invalid := filepath.Join(t.TempDir(), "invalid.json")
			err = runNatsCliWithError(t, auditGatherCmd(srv, archivePath, fmt.Sprintf("--cpu-profile-duration 20s --write-config '%s'", invalid)))
			if err == nil || !strings.Contains(err.Error(), "--cpu-profile-duration: must be more than 0s and at most 15s, got 20s") {
				t.Errorf("expected cpu_duration error, got: %v", err)
			}
			if _, err := os.Stat(invalid); err == nil {
				t.Errorf("invalid config was written")
			}

			noProfiles := filepath.Join(t.TempDir(), "no-profiles.json")
			err = runNatsCliWithError(t, auditGatherCmd(srv, archivePath, fmt.Sprintf("--skip-profile goroutine_1 --skip-profile heap --skip-profile allocs --skip-profile mutex --skip-profile threadcreate --skip-profile block --skip-profile cpu --write-config '%s'", noProfiles)))
			if err == nil || !strings.Contains(err.Error(), "no server profiles selected") {
				t.Errorf("expected no server profiles error, got: %v", err)
			}
			if _, err := os.Stat(noProfiles); err == nil {
				t.Errorf("config without profiles was written")
			}

			err = runNatsCliWithError(t, auditGatherCmd(srv, archivePath, fmt.Sprintf("--write-config '%s'", filepath.Join(t.TempDir(), "missing", "unwritable.json"))))
			if err == nil || !strings.Contains(err.Error(), "unwritable.json") {
				t.Errorf("expected write error, got: %v", err)
			}
		})

		for _, tc := range []struct {
			name   string
			args   string
			config string
			want   string
		}{
			{"profile and skip-profile", "--profile heap --skip-profile cpu", "", "--profile and --skip-profile cannot be used together"},
			{"skip-profile removes every profile", "--skip-profile goroutine_1 --skip-profile heap --skip-profile allocs --skip-profile mutex --skip-profile threadcreate --skip-profile block --skip-profile cpu", "", "no server profiles selected, choose some with --profile or capture none with --no-server-profiles"},
			{"profile with no-server-profiles", "--no-server-profiles --profile heap", "", "--profile cannot be used with --no-server-profiles"},
			{"skip-profile with profiles disabled in file", "--skip-profile cpu", `{"server_profiles": {"enabled": false}}`, "gather.json disables server profiles, add --server-profiles to enable them"},
			{"empty profiles in file", "", `{"server_profiles": {"profiles": []}}`, "selects no server profiles, choose some with --profile or capture none with --no-server-profiles"},
			{"server-profiles with empty profiles in file", "--server-profiles", `{"server_profiles": {"enabled": false, "profiles": []}}`, "selects no server profiles"},
			{"unknown profile in file", "", `{"server_profiles": {"profiles": ["goroutine"]}}`, `profiles: unknown server profile "goroutine"`},
			{"invalid cpu duration", "", `{"server_profiles": {"cpu_duration": "5 seconds"}}`, "cpu_duration: invalid duration"},
			{"zero cpu duration in file", "", `{"server_profiles": {"cpu_duration": "0s"}}`, "cpu_duration: must be more than 0s and at most 15s, got 0s"},
			{"cpu duration in file out of range", "", `{"server_profiles": {"cpu_duration": "20s"}}`, "cpu_duration: must be more than 0s and at most 15s, got 20s"},
			{"zero subsz page size", "--subsz-page-size 0", "", "subsz page size must be more than 0, got 0"},
			{"negative subsz page size in file", "", `{"subsz_page_size": -1}`, "subsz page size must be more than 0, got -1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				archivePath := filepath.Join(t.TempDir(), "audit.zip")
				args := tc.args
				if tc.config != "" {
					args = fmt.Sprintf("%s --config '%s'", args, writeAuditGatherConfig(t, tc.config))
				}

				err := runNatsCliWithError(t, auditGatherCmd(srv, archivePath, args))
				if err == nil {
					t.Fatalf("expected an error")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("expected error containing %q, got: %v", tc.want, err)
				}
				if _, err := os.Stat(archivePath); err == nil {
					t.Errorf("archive was written")
				}
			})
		}

		return nil
	})
}

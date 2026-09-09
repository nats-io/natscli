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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type backupFixture struct {
	name  string
	dir   string
	state api.StreamState
}

func setupBackupFixture(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) *backupFixture {
	t.Helper()

	name := setupStreamTest(t, mgr, jsm.FileStorage())
	stream, err := mgr.LoadStream(name)
	checkErr(t, err, "load failed: %v", err)
	_, err = stream.NewConsumer(jsm.DurableName("C1"))
	checkErr(t, err, "consumer failed: %v", err)

	for i, subject := range []string{"ORDERS.new", "ORDERS.paid", "ORDERS.new", "ORDERS.paid", "ORDERS.new"} {
		msg := nats.NewMsg(subject)
		msg.Data = fmt.Appendf(nil, "message %d", i+1)
		if i%2 == 1 {
			msg.Header.Set("X-Batch", "7")
		}
		_, err := nc.RequestMsg(msg, time.Second)
		checkErr(t, err, "publish failed: %v", err)
	}

	state, err := stream.State()
	checkErr(t, err, "state failed: %v", err)

	dir := filepath.Join(t.TempDir(), "src")
	runNatsCli(t, fmt.Sprintf("--server='%s' stream backup %s '%s'", srv.ClientURL(), name, dir))

	return &backupFixture{name: name, dir: dir, state: state}
}

func restoreBackup(t *testing.T, srv *server.Server, mgr *jsm.Manager, name string, dir string) *jsm.Stream {
	t.Helper()

	checkErr(t, mgr.DeleteStream(name), "delete failed")
	runNatsCli(t, fmt.Sprintf("--server='%s' stream restore '%s'", srv.ClientURL(), dir))
	stream, err := mgr.LoadStream(name)
	checkErr(t, err, "restored stream missing: %v", err)

	return stream
}

func TestBackupValidate(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)

		output := string(runNatsCli(t, fmt.Sprintf("backup validate '%s'", fx.dir)))
		if !expectMatchLine(t, output, `^OK: 8 entries, 1 consumers, 5 messages, 2 subjects, sequences 1 to 5$`) {
			t.Errorf("unexpected output: %s", output)
		}

		return nil
	})
}

func TestBackupInfo(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)

		output := string(runNatsCli(t, fmt.Sprintf("backup info '%s'", fx.dir)))
		err := expectMatchJSON(t, output, map[string]any{
			"Configuration": map[string]any{"Name": "^" + fx.name + "$", "Subjects": `^ORDERS\.\*$`, "Storage": "^File$", "Retention": "^Limits$"},
			"Consumers":     map[string]any{"Count": "^1$", "Names": "^C1$"},
			"Messages":      map[string]any{"Messages": "^5$", "Subjects": "^2$", "First Sequence": "^1 @ ", "Last Sequence": "^5 @ "},
		})
		if err != nil {
			t.Errorf("unexpected info: %v: %s", err, output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --subjects", fx.dir)))
		if !expectMatchLine(t, output, "ORDERS.new", "3") || !expectMatchLine(t, output, "ORDERS.paid", "2") {
			t.Errorf("subjects not listed: %s", output)
		}
		if strings.Contains(output, "Edit") || strings.Contains(output, "advisory") {
			t.Errorf("server written backup reported as edited: %s", output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --json", fx.dir)))
		err = expectMatchJSON(t, output, map[string]any{
			"config":    map[string]any{"name": "^" + fx.name + "$"},
			"consumers": []any{"^C1$"},
			"messages":  "^5$",
			"first_seq": "^1$",
			"last_seq":  "^5$",
		})
		if err != nil {
			t.Errorf("unexpected json: %v: %s", err, output)
		}

		return nil
	})
}

func TestBackupEditIdentity(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s'", fx.dir, target)))
		for _, want := range [][]string{
			{"Source Messages", "5"},
			{"Messages Kept", "5"},
			{"Consumers Kept", "1"},
			{"Restored Messages", "5"},
			{"Restored Consumers", "1"},
		} {
			if !expectMatchLine(t, output, want...) {
				t.Errorf("missing %v in output: %s", want, output)
			}
		}
		if strings.Contains(output, "Dropped by") {
			t.Errorf("identity edit reported drops: %s", output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup validate '%s'", target)))
		if !expectMatchLine(t, output, `^OK: 8 entries, 1 consumers, 5 messages, 2 subjects, sequences 1 to 5$`) {
			t.Errorf("unexpected validate output: %s", output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s'", target)))
		err := expectMatchJSON(t, output, map[string]any{
			"Edit": map[string]any{"Source Digest": "^sha256:[0-9a-f]{64}$", "Options": "^none$", "Obfuscated": "^false$"},
		})
		if err != nil {
			t.Errorf("edit block missing from info: %v: %s", err, output)
		}

		stream := restoreBackup(t, srv, mgr, fx.name, target)
		state, err := stream.State()
		checkErr(t, err, "state failed: %v", err)
		if state.Msgs != fx.state.Msgs || state.FirstSeq != fx.state.FirstSeq || state.LastSeq != fx.state.LastSeq || state.Consumers != 1 {
			t.Errorf("restored state %+v does not match source %+v", state, fx.state)
		}
		if _, err := mgr.LoadConsumer(fx.name, "C1"); err != nil {
			t.Errorf("consumer C1 missing after restore: %v", err)
		}

		return nil
	})
}

func TestBackupEditSubjectFilter(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.new", fx.dir, target)))
		for _, want := range [][]string{
			{"Messages Kept", "3"},
			{"Dropped by Subject Filter", "2"},
			{"Restored First Sequence", "1"},
			{"Restored Last Sequence", "5"},
		} {
			if !expectMatchLine(t, output, want...) {
				t.Errorf("missing %v in output: %s", want, output)
			}
		}

		return nil
	})
}

func TestBackupEditRenumber(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.new --renumber", fx.dir, target)))
		for _, want := range [][]string{
			{"Messages Kept", "3"},
			{"Consumers Dropped", "1"},
			{"Restored First Sequence", "1"},
			{"Restored Last Sequence", "3"},
			{"Restored Consumers", "0"},
		} {
			if !expectMatchLine(t, output, want...) {
				t.Errorf("missing %v in output: %s", want, output)
			}
		}

		return nil
	})
}

func TestBackupEditDryRun(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.paid --dry-run", fx.dir, target)))
		if !expectMatchLine(t, output, "Dry Run, Nothing Was Written") || !expectMatchLine(t, output, "Messages Kept", "2") {
			t.Errorf("unexpected output: %s", output)
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Errorf("dry run created the target: %v", err)
		}

		return nil
	})
}

func TestBackupEditKVCompact(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		srvFlag := fmt.Sprintf("--server='%s'", srv.ClientURL())
		runNatsCli(t, srvFlag+" kv add B --history 5")
		runNatsCli(t, srvFlag+" kv put B k1 v1")
		runNatsCli(t, srvFlag+" kv put B k1 v2")
		runNatsCli(t, srvFlag+" kv put B k2 x")
		runNatsCli(t, srvFlag+" kv del B k2 -f")

		dir := filepath.Join(t.TempDir(), "src")
		runNatsCli(t, fmt.Sprintf("%s stream backup KV_B '%s'", srvFlag, dir))
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --kv-compact", dir, target)))
		for _, want := range [][]string{
			{"Source Messages", "4"},
			{"Messages Kept", "1"},
			{"Dropped by KV Compaction", "3"},
			{"Subject State Keys", "2"},
			{"Restored Messages", "1"},
		} {
			if !expectMatchLine(t, output, want...) {
				t.Errorf("missing %v in output: %s", want, output)
			}
		}

		return nil
	})
}

func TestBackupEditObfuscate(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")
		keyFile := target + ".keys.json"

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --obfuscate", fx.dir, target)))
		for _, want := range [][]string{
			{"Message Bodies Padded", "5"},
			{"Key File", regexp.QuoteMeta(keyFile)},
			{"keep the key file private"},
		} {
			if !expectMatchLine(t, output, want...) {
				t.Errorf("missing %v in output: %s", want, output)
			}
		}

		if _, err := os.Stat(keyFile); err != nil {
			t.Errorf("key file not written beside the target: %v", err)
		}
		entries, err := os.ReadDir(target)
		checkErr(t, err, "target missing: %v", err)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".keys.json") {
				t.Errorf("key file written inside the target")
			}
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s'", target)))
		if err := expectMatchJSON(t, output, map[string]any{"Edit": map[string]any{"Obfuscated": "^true$"}}); err != nil {
			t.Errorf("edit block missing from info: %v: %s", err, output)
		}
		if strings.Contains(output, fx.name) {
			t.Errorf("obfuscated backup still names the source stream: %s", output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup validate '%s'", target)))
		if !expectMatchLine(t, output, `^OK: 8 entries, 1 consumers, 5 messages, 2 subjects, sequences 1 to 5$`) {
			t.Errorf("unexpected validate output: %s", output)
		}

		hashed := strings.TrimSpace(string(runNatsCli(t, fmt.Sprintf("backup info '%s' --json", target))))
		var nfo struct {
			Config struct {
				Name     string   `json:"name"`
				Subjects []string `json:"subjects"`
			} `json:"config"`
		}
		if err := json.Unmarshal([]byte(hashed), &nfo); err != nil {
			t.Fatalf("info json failed: %v: %s", err, hashed)
		}
		output = string(runNatsCli(t, fmt.Sprintf("backup lookup '%s' %s %s", keyFile, nfo.Config.Name, nfo.Config.Subjects[0])))
		if !expectMatchLine(t, output, "^"+fx.name+"$") || !expectMatchLine(t, output, `^ORDERS\.\*$`) {
			t.Errorf("lookup did not reveal the originals: %s", output)
		}

		return nil
	})
}

func TestBackupEditErrors(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fx := setupBackupFixture(t, srv, nc, mgr)

		v1 := t.TempDir()
		checkErr(t, os.WriteFile(filepath.Join(v1, "backup.json"), []byte(`{"config":{"name":"OLD"}}`), 0o600), "write failed")
		checkErr(t, os.WriteFile(filepath.Join(v1, "stream.tar.s2"), []byte("not an archive"), 0o600), "write failed")

		for _, tc := range []struct {
			name string
			cmd  string
			want string
		}{
			{"not a 2.15 backup", fmt.Sprintf("backup edit '%s' '%s'", v1, filepath.Join(t.TempDir(), "out")), "not a NATS Server 2.15 stream backup"},
			{"validate not a 2.15 backup", fmt.Sprintf("backup validate '%s'", v1), "not a NATS Server 2.15 stream backup"},
			{"kv-compact on a plain stream", fmt.Sprintf("backup edit '%s' '%s' --kv-compact", fx.dir, filepath.Join(t.TempDir(), "out")), "requires a KV bucket backup"},
			{"kv-compact with last-per-subject", fmt.Sprintf("backup edit '%s' '%s' --kv-compact --last-per-subject 1", fx.dir, filepath.Join(t.TempDir(), "out")), "mutually exclusive"},
			{"bad regex", fmt.Sprintf("backup edit '%s' '%s' --payload-match '('", fx.dir, filepath.Join(t.TempDir(), "out")), "invalid payload expression"},
			{"bad time", fmt.Sprintf("backup edit '%s' '%s' --after yesterday", fx.dir, filepath.Join(t.TempDir(), "out")), "invalid time"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := runNatsCliWithError(t, tc.cmd)
				if err == nil {
					t.Fatalf("expected an error")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("expected error containing %q, got: %v", tc.want, err)
				}
			})
		}

		return nil
	})
}

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
	"reflect"
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
	runNatsCli(t, fmt.Sprintf("--server='%s' backup stream %s '%s'", srv.ClientURL(), name, dir))

	return &backupFixture{name: name, dir: dir, state: state}
}

func restoreBackup(t *testing.T, srv *server.Server, mgr *jsm.Manager, name string, dir string) *jsm.Stream {
	t.Helper()

	checkErr(t, mgr.DeleteStream(name), "delete failed")
	runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream '%s'", srv.ClientURL(), dir))
	stream, err := mgr.LoadStream(name)
	checkErr(t, err, "restored stream missing: %v", err)

	return stream
}

func TestBackupValidate(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fixture := setupBackupFixture(t, srv, nc, mgr)

		output := string(runNatsCli(t, fmt.Sprintf("backup validate '%s'", fixture.dir)))
		if !expectMatchLine(t, output, `^OK: 8 entries, 1 consumers, 5 messages, 2 subjects, sequences 1 to 5$`) {
			t.Errorf("unexpected output: %s", output)
		}

		return nil
	})
}

func TestBackupInfo(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fixture := setupBackupFixture(t, srv, nc, mgr)

		output := string(runNatsCli(t, fmt.Sprintf("backup info '%s' --no-progress", fixture.dir)))
		err := expectMatchJSON(t, output, map[string]any{
			"Configuration": map[string]any{"Name": "^" + fixture.name + "$", "Subjects": `^ORDERS\.\*$`, "Storage": "^File$", "Retention": "^Limits$"},
			"Consumers":     map[string]any{"Count": "^1$", "Names": "^C1$"},
			"Messages":      map[string]any{"Messages": "^5$", "Subjects": "^2$", "First Sequence": "^1 @ ", "Last Sequence": "^5 @ "},
		})
		if err != nil {
			t.Errorf("unexpected info: %v: %s", err, output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --subjects", fixture.dir)))
		if !expectMatchLine(t, output, "ORDERS.new", "3") || !expectMatchLine(t, output, "ORDERS.paid", "2") {
			t.Errorf("subjects not listed: %s", output)
		}
		if strings.Contains(output, "Edit") || strings.Contains(output, "advisory") {
			t.Errorf("server written backup reported as edited: %s", output)
		}

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --json", fixture.dir)))
		err = expectMatchJSON(t, output, map[string]any{
			"config":    map[string]any{"name": "^" + fixture.name + "$"},
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
		fixture := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s'", fixture.dir, target)))
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

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --no-progress", target)))
		err := expectMatchJSON(t, output, map[string]any{
			"Edit": map[string]any{"Source Digest": "^sha256:[0-9a-f]{64}$", "Options": "^none$", "Obfuscated": "^false$"},
		})
		if err != nil {
			t.Errorf("edit block missing from info: %v: %s", err, output)
		}

		stream := restoreBackup(t, srv, mgr, fixture.name, target)
		state, err := stream.State()
		checkErr(t, err, "state failed: %v", err)
		if state.Msgs != fixture.state.Msgs || state.FirstSeq != fixture.state.FirstSeq || state.LastSeq != fixture.state.LastSeq || state.Consumers != 1 {
			t.Errorf("restored state %+v does not match source %+v", state, fixture.state)
		}
		if _, err := mgr.LoadConsumer(fixture.name, "C1"); err != nil {
			t.Errorf("consumer C1 missing after restore: %v", err)
		}

		return nil
	})
}

func TestBackupEditSubjectFilter(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fixture := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.new", fixture.dir, target)))
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
		fixture := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.new --renumber", fixture.dir, target)))
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
		fixture := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --subject ORDERS.paid --dry-run", fixture.dir, target)))
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
		runNatsCli(t, fmt.Sprintf("%s backup stream KV_B '%s'", srvFlag, dir))
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
		fixture := setupBackupFixture(t, srv, nc, mgr)
		target := filepath.Join(t.TempDir(), "edited")
		keyFile := target + ".keys.json"

		output := string(runNatsCli(t, fmt.Sprintf("backup edit '%s' '%s' --obfuscate", fixture.dir, target)))
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

		output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --no-progress", target)))
		if err := expectMatchJSON(t, output, map[string]any{"Edit": map[string]any{"Obfuscated": "^true$"}}); err != nil {
			t.Errorf("edit block missing from info: %v: %s", err, output)
		}
		if strings.Contains(output, fixture.name) {
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
		if !expectMatchLine(t, output, "^"+fixture.name+"$") || !expectMatchLine(t, output, `^ORDERS\.\*$`) {
			t.Errorf("lookup did not reveal the originals: %s", output)
		}

		return nil
	})
}

func TestBackupEditErrors(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		fixture := setupBackupFixture(t, srv, nc, mgr)

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
			{"kv-compact on a plain stream", fmt.Sprintf("backup edit '%s' '%s' --kv-compact", fixture.dir, filepath.Join(t.TempDir(), "out")), "requires a KV bucket backup"},
			{"kv-compact with last-per-subject", fmt.Sprintf("backup edit '%s' '%s' --kv-compact --last-per-subject 1", fixture.dir, filepath.Join(t.TempDir(), "out")), "mutually exclusive"},
			{"bad regex", fmt.Sprintf("backup edit '%s' '%s' --payload-match '('", fixture.dir, filepath.Join(t.TempDir(), "out")), "invalid payload expression"},
			{"bad time", fmt.Sprintf("backup edit '%s' '%s' --after yesterday", fixture.dir, filepath.Join(t.TempDir(), "out")), "invalid time"},
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

func TestBackupStream(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := setupStreamTest(t, mgr)
		tmpDir := t.TempDir()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' backup stream %s %s", srv.ClientURL(), name, tmpDir)))
		if !expectMatchLine(t, output, fmt.Sprintf("Starting backup of Stream \"%s\"", name)) ||
			!expectMatchLine(t, output, "done") {
			t.Errorf("Unexecpted output :%s", output)
		}
		return nil
	})
}

func TestBackupRestoreStream(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := setupStreamTest(t, mgr)
		tmpDir := t.TempDir()

		runNatsCli(t, fmt.Sprintf("--server='%s' backup stream %s %s", srv.ClientURL(), name, tmpDir))
		mgr.DeleteStream(name)
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream %s", srv.ClientURL(), tmpDir)))
		if !expectMatchLine(t, output, fmt.Sprintf("Starting restore of Stream \"%s\"", name)) ||
			!expectMatchLine(t, output, fmt.Sprintf("Restored stream \"%s\" in \\d+s", name)) {
			t.Errorf("Unexecpted output :%s", output)
		}
		return nil
	})
}

func TestBackupRestoreStreamWithConfig(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		name := setupStreamTest(t, mgr)
		tmpDir := t.TempDir()

		runNatsCli(t, fmt.Sprintf("--server='%s' backup stream %s %s", srv.ClientURL(), name, tmpDir))
		mgr.DeleteStream(name)

		overrideCfg := api.StreamConfig{
			Name:         name,
			Subjects:     []string{"ORDERS.*", "OVERRIDE.*"},
			Description:  "restored with override",
			Retention:    api.LimitsPolicy,
			Storage:      api.FileStorage,
			MaxConsumers: -1,
			MaxMsgs:      -1,
			MaxMsgsPer:   -1,
			MaxBytes:     -1,
			MaxMsgSize:   -1,
			Replicas:     1,
			Discard:      api.DiscardOld,
			Duplicates:   2 * time.Minute,
		}

		cfgJSON, err := json.Marshal(overrideCfg)
		if err != nil {
			t.Fatalf("unable to marshal config: %v", err)
		}

		overrideFile, err := os.CreateTemp(t.TempDir(), "override.json")
		if err != nil {
			t.Fatalf("unable to create config file: %v", err)
		}
		if _, err := overrideFile.Write(cfgJSON); err != nil {
			t.Fatalf("unable to write config file: %v", err)
		}
		cfgFile := overrideFile.Name()
		overrideFile.Close()

		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream %s --config='%s'", srv.ClientURL(), tmpDir, cfgFile)))
		if !expectMatchLine(t, output, fmt.Sprintf("Restored stream \"%s\"", name)) {
			t.Errorf("Unexpected output :%s", output)
		}

		stream, err := mgr.LoadStream(name)
		if err != nil {
			t.Errorf("failed to load stream %s: %s", name, err)
		}

		if stream.Description() != "restored with override" {
			t.Errorf("expected description %q but got %q", "restored with override", stream.Description())
		}

		subjects := stream.Subjects()
		if len(subjects) != 2 || subjects[0] != "ORDERS.*" || subjects[1] != "OVERRIDE.*" {
			t.Errorf("expected subjects [ORDERS.* OVERRIDE.*] but got %v", subjects)
		}

		return nil
	})
}

func TestBackupStreamAndRestoreState(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	defer srv.Shutdown()

	stream, err := mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "file1")

	for i := 0; i < 1000; i++ {
		nc.Publish("js.file.1", []byte(RandomString(5480)))
	}

	td, err := os.MkdirTemp("", "")
	checkErr(t, err, "temp dir failed")
	os.RemoveAll(td)

	runNatsCli(t, fmt.Sprintf("--server='%s' backup stream file1 '%s' --no-progress", srv.ClientURL(), td))

	preState, err := stream.State()
	checkErr(t, err, "state failed")
	stream.Delete()

	runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream '%s' --no-progress", srv.ClientURL(), td))
	stream, err = mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)

	postState, err := stream.State()
	checkErr(t, err, "state failed")
	if !reflect.DeepEqual(preState, postState) {
		t.Fatalf("restored state differed")
	}

	if postState.Msgs != 1000 {
		t.Fatalf("Expected 1000 messages got %d", postState.Msgs)
	}
}

func TestBackupStreamRestoreSequence(t *testing.T) {
	srv, nc, mgr := setupConsTest(t)
	defer srv.Shutdown()

	dir, err := os.MkdirTemp("", "")
	checkErr(t, err, "temp dir failed")
	defer os.RemoveAll(dir)
	target := filepath.Join(dir, "backup.tgz")

	stream, err := mgr.NewStreamFromDefault("file1", file1Stream())
	checkErr(t, err, "could not create stream: %v", err)
	streamShouldExist(t, mgr, "file1")

	for i := 0; i < 1024; i++ {
		_, err = nc.Request("js.file.1", []byte(fmt.Sprintf("message %d", i)), time.Second)
		checkErr(t, err, "publish failed")
	}

	runNatsCli(t, fmt.Sprintf("--server='%s' backup stream file1 '%s'", srv.ClientURL(), target))

	err = stream.Delete()
	checkErr(t, err, "delete failed")
	streamShouldNotExist(t, mgr, "file1")

	runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream '%s'", srv.ClientURL(), target))
	streamShouldExist(t, mgr, "file1")

	stream, err = mgr.LoadStream("file1")
	checkErr(t, err, "load failed")
	state, err := stream.State()
	checkErr(t, err, "state failed")
	if state.LastSeq != 1024 {
		t.Fatalf("expected 1024 messages got %d", state.LastSeq)
	}
}

func TestBackupAccount(t *testing.T) {
	withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
		_, err := mgr.NewStream("INVOICES", jsm.FileStorage(), jsm.Subjects("INVOICES.*"))
		checkErr(t, err, "unable to create stream: %v", err)
		streamNames := []string{setupStreamTest(t, mgr, jsm.FileStorage()), "INVOICES"}
		publishSubjects := []string{"ORDERS.new", "INVOICES.new"}
		statesBeforeBackup := map[string]api.StreamState{}

		for i, streamName := range streamNames {
			for msgNum := range 5 {
				_, err := nc.Request(publishSubjects[i], fmt.Appendf(nil, "message %d", msgNum), time.Second)
				checkErr(t, err, "publish failed: %v", err)
			}
			stream, err := mgr.LoadStream(streamName)
			checkErr(t, err, "load failed: %v", err)
			statesBeforeBackup[streamName], err = stream.State()
			checkErr(t, err, "state failed: %v", err)
		}

		backupDir := filepath.Join(t.TempDir(), "account")
		output := string(runNatsCli(t, fmt.Sprintf("--server='%s' backup account '%s' --force", srv.ClientURL(), backupDir)))
		for _, streamName := range streamNames {
			if !expectMatchLine(t, output, fmt.Sprintf("Starting backup of Stream \"%s\"", streamName)) {
				t.Errorf("unexpected output: %s", output)
			}
			if _, err := os.Stat(filepath.Join(backupDir, streamName, "backup.json")); err != nil {
				t.Errorf("expected a backup for %s: %v", streamName, err)
			}
			checkErr(t, mgr.DeleteStream(streamName), "delete failed")
		}

		output = string(runNatsCli(t, fmt.Sprintf("--server='%s' backup restore account '%s'", srv.ClientURL(), backupDir)))
		if !expectMatchLine(t, output, "Restoring backup of all 2 streams") {
			t.Errorf("unexpected output: %s", output)
		}

		for _, streamName := range streamNames {
			stream, err := mgr.LoadStream(streamName)
			checkErr(t, err, "restored stream missing: %v", err)
			restoredState, err := stream.State()
			checkErr(t, err, "state failed: %v", err)
			if restoredState.Msgs != statesBeforeBackup[streamName].Msgs || restoredState.FirstSeq != statesBeforeBackup[streamName].FirstSeq || restoredState.LastSeq != statesBeforeBackup[streamName].LastSeq {
				t.Errorf("restored state for %s differed: %+v vs %+v", streamName, restoredState, statesBeforeBackup[streamName])
			}
		}

		return nil
	})
}

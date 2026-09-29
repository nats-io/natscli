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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/backup"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	primaryTestMsgData      = "primary test string"
	secondaryTestMsgData    = "secondary test string"
	nonJetstreamTestMsgData = "not jetstream test string"
)

func TestNatsSubscribe(t *testing.T) {
	var (
		defaultTestMsg = &nats.Msg{
			Subject: "TEST",
			Data:    []byte(primaryTestMsgData),
		}
	)

	t.Run("--dump=file", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dumpDir := t.TempDir()

			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump='%s'", srv.ClientURL(), dumpDir))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatal(err)
			}

			output := <-out
			if strings.Contains(output, "Could not save message") {
				t.Errorf("unexpected error: %s", output)
			}

			resp, err := os.ReadFile(filepath.Join(dumpDir, "1.json"))
			if err != nil {
				t.Fatal(err)
			}

			var responseObj nats.Msg
			if err := json.Unmarshal(resp, &responseObj); err != nil {
				t.Fatal(err)
			}

			if string(responseObj.Data) != primaryTestMsgData {
				t.Errorf("unexpected data section of message. Got %q, expected %q", string(responseObj.Data), primaryTestMsgData)
			}

			return nil
		})
	})

	t.Run("--dump=-", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump=-", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out

			resp := strings.TrimSpace(output)
			resp = resp[:len(resp)-1]

			responseObj := nats.Msg{}
			err = json.Unmarshal([]byte(resp), &responseObj)
			if err != nil {
				t.Fatal(err)
			}

			if string(responseObj.Data) != primaryTestMsgData {
				t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--translate", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --raw --translate='wc -c'", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			resp := strings.TrimSpace(output)
			lines := strings.Split(resp, "\n")
			if len(lines) < 1 {
				t.Fatalf("no output lines found")
			}
			if strings.TrimSpace(lines[len(lines)-1]) != "19" {
				t.Errorf("unexpected response. Got %q, expected %q", strings.TrimSpace(lines[len(lines)-1]), "19")
			}
			return nil
		})
	})

	t.Run("--raw and --translate with subject", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --raw --translate=\"sed 's/^/{{Subject}}: /'\"", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			lines := strings.Split(strings.TrimSpace(output), "\n")
			if len(lines) < 1 {
				t.Fatalf("no output lines found")
			}

			expected := "TEST: " + primaryTestMsgData
			resp := strings.TrimSpace(lines[len(lines)-1])
			if resp != expected {
				t.Errorf("unexpected response. Got %q, expected %q", resp, expected)
			}
			return nil
		})
	})

	t.Run("--translate empty message", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --translate=\"wc -c\"", srv.ClientURL()))

			err := nc.PublishMsg(&nats.Msg{
				Subject: "TEST",
				Data:    []byte(""),
			})
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			lines := strings.Split(strings.TrimSpace(output), "\n")
			if len(lines) < 1 {
				t.Fatalf("no output from CLI")
			}

			resp := strings.TrimSpace(lines[len(lines)-1])
			if resp != "0" {
				t.Errorf("unexpected response. Got %q, expected %q", resp, "0")
			}
			return nil
		})
	})

	t.Run("--dump and --translate", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dumpDir := t.TempDir()

			done := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump='%s' --translate=\"wc -c\"", srv.ClientURL(), dumpDir))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			<-done

			entries, err := os.ReadDir(dumpDir)
			if err != nil {
				t.Fatal(err)
			}

			var dumpFile string
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
					if dumpFile != "" {
						t.Fatalf("unexpected file found in dump directory: %s", entry.Name())
					}
					dumpFile = filepath.Join(dumpDir, entry.Name())
				}
			}

			if dumpFile == "" {
				t.Fatalf("no .json file found in dump directory %q with entries %v", dumpDir, entries)
			}

			resp, err := os.ReadFile(dumpFile)
			if err != nil {
				t.Fatal(err)
			}

			var responseObj nats.Msg
			if err := json.Unmarshal(resp, &responseObj); err != nil {
				t.Fatal(err)
			}

			if strings.TrimSpace(string(responseObj.Data)) != "19" {
				t.Errorf("unexpected data section of message. Got %q, expected %q", string(responseObj.Data), "19")
			}
			return nil
		})
	})

	t.Run("--raw", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1 --raw", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			lines := strings.Split(strings.TrimSpace(output), "\n")

			if len(lines) < 1 {
				t.Fatalf("no CLI output")
			}

			resp := strings.TrimSpace(lines[len(lines)-1])
			if resp != primaryTestMsgData {
				t.Errorf("unexpected response. Got %q, expected %q", resp, primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--pretty", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --count=1", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out

			if !expectMatchLine(t, output, `\[#\d+\] Received on "TEST"`) {
				t.Fatalf("missing expected summary line:\n%s", output)
			}

			if !expectMatchLine(t, output, regexp.QuoteMeta(primaryTestMsgData)) {
				t.Fatalf("missing expected message body:\n%s", output)
			}

			return nil
		})
	})

	t.Run("--queue", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "TEST"
			queue := "TEST_QUEUE"
			msgReceived := false

			_, err := nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
				msgReceived = true
			})
			if err != nil {
				t.Error(err)
			}

			outputCh := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub %s --queue=%s --count=1", srv.ClientURL(), subject, queue))

			// Send a lot of messages. This should be enough to make sure both subscribers get at least 1 msg each and
			// we can test that the subscription is to a named queue group
			for range 20 {
				nc.Publish("TEST", []byte(nonJetstreamTestMsgData))
			}

			output := <-outputCh

			if !expectMatchLine(t, output, nonJetstreamTestMsgData) {
				t.Errorf("unexpected response: %s.", output)
			}

			if !msgReceived {
				t.Error("only one queue member received messages")
			}

			return nil
		})
	})

	t.Run("--ack", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			outputCh := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST_STREAM --count=1 --ack", srv.ClientURL()))
			err := nc.Publish("TEST_STREAM", []byte(primaryTestMsgData))
			if err != nil {
				t.Error(err)
			}

			output := <-outputCh
			if !expectMatchLine(t, output, "Subscribing on TEST_STREAM with acknowledgement of JetStream messages") {
				t.Errorf("unexpected output: %s", output)
			}
			return nil
		})
	})

	t.Run("--match-replies", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			_, err := nc.Subscribe("TEST_STREAMJECT", func(msg *nats.Msg) {
				reply := fmt.Sprintf("test reply %s", string(msg.Data))
				nc.Publish(msg.Reply, []byte(reply))
			})
			if err != nil {
				t.Error(err)
			}

			outputCh := startNatsCli(t, srv, nc, 2, fmt.Sprintf("--server='%s' sub > --match-replies --count=1 --wait=2s", srv.ClientURL()))

			_, err = nc.Request("TEST_STREAMJECT", []byte("test request"), 1*time.Second)
			if err != nil {
				t.Error(err)
			}

			output := <-outputCh
			if !expectMatchLine(t, output, `Matched reply on "_INBOX\.`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--inbox", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --wait=100ms --inbox", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing on _INBOX.") {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--headers-only", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			done := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --headers-only --count=1", srv.ClientURL()))

			err := nc.PublishMsg(&nats.Msg{
				Subject: "TEST",
				Data:    []byte(primaryTestMsgData),
				Header: nats.Header{
					"test": []string{"header"},
				},
			})
			if err != nil {
				t.Fatalf("failed to publish message: %s", err)
			}

			output := <-done

			if !expectMatchLine(t, output, `test: header`) {
				t.Errorf("expected header not found in output:\n%s", output)
			}
			if expectMatchLine(t, output, regexp.QuoteMeta(primaryTestMsgData)) {
				t.Errorf("message data should not be printed when using --headers-only:\n%s", output)
			}

			return nil
		})
	})

	t.Run("--subjects-only", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			done := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST --subjects-only --count=1", srv.ClientURL()))

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-done

			if !expectMatchLine(t, output, `Received on "TEST"`) {
				t.Errorf("expected subject line not found in output:\n%s", output)
			}
			if expectMatchLine(t, output, regexp.QuoteMeta(primaryTestMsgData)) {
				t.Errorf("message data should not be printed when using --subjects-only:\n%s", output)
			}

			return nil
		})
	})

	t.Run("--ignore-subject", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			done := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST.* --ignore-subject=TEST --count=1", srv.ClientURL()))

			// Publish the message to be ignored
			if err := nc.PublishMsg(defaultTestMsg); err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			// Publish the message to be received
			if err := nc.PublishMsg(&nats.Msg{
				Subject: "TEST.2",
				Data:    []byte(secondaryTestMsgData),
			}); err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-done

			if expectMatchLine(t, output, regexp.QuoteMeta(primaryTestMsgData)) {
				t.Errorf("ignored subject should not appear:\n%s", output)
			}
			if !expectMatchLine(t, output, regexp.QuoteMeta(secondaryTestMsgData)) {
				t.Errorf("expected message from allowed subject missing:\n%s", output)
			}

			return nil
		})
	})

	t.Run("--wait", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			// The test here is to put us in a state that will block, and make sure --wait breaks us out.
			// symptoms of a failing --wait flag will be a test that times out
			runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --wait=100ms --count=100", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--report-subjects", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			// This test returns long running output. best we can do here is check if it runs without failure
			runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST.* --report-subjects --report-top=1 --wait=100ms", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--report-subscriptions", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			// This test returns long running output. best we can do here is check if it runs without failure
			runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST.* --report-subscriptions --report-top=1 --wait=100ms", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--timestamp", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			outputCh := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST.* --count=1 --timestamp", srv.ClientURL()))
			nc.Publish("TEST.1", []byte(nonJetstreamTestMsgData))
			output := <-outputCh

			//  [#1] @ Jun  4 11:02:31.562257 Received on "TEST.1"
			if !expectMatchLine(t, output, nonJetstreamTestMsgData) || !expectMatchLine(t, output, `[A-Z][a-z]{2}\s+\d{1,2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--delta-time", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			outputCh := startNatsCli(t, srv, nc, 1, fmt.Sprintf("--server='%s' sub TEST.* --count=1 --delta-time", srv.ClientURL()))
			nc.Publish("TEST.1", []byte(nonJetstreamTestMsgData))
			output := <-outputCh

			if !expectMatchLine(t, output, nonJetstreamTestMsgData) || !expectMatchLine(t, output, `@ \d+(?:\.\d+)?(ns|µs|us|ms|s|m|h)`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--graph", func(t *testing.T) {
		// This command requires a terminal
	})

}

func TestJetStreamSubscribe(t *testing.T) {
	var (
		defaultTestMsg = &nats.Msg{
			Subject: "TEST_STREAM.1",
			Data:    []byte(primaryTestMsgData),
		}
	)

	t.Run("--terminate-at-end", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatal(err)
			}

			start := time.Now()
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --terminate-at-end --wait 2s", srv.ClientURL())))
			if !expectMatchLine(t, output, `\[#\d\] Received JetStream message: stream: TEST_STREAM seq: (\d+) / pending: 0 / subject: TEST_STREAM.1 / time: \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d`) {
				t.Fatalf("missing expected summary line:\n%s", output)
			}

			if time.Since(start) > time.Second {
				t.Fatalf("expected to terminate first message, but took %s", time.Since(start))
			}
			return nil
		})
	})

	t.Run("--dump=file", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			dumpDir := t.TempDir()
			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --dump='%s'", srv.ClientURL(), dumpDir))

			resp, err := os.ReadFile(filepath.Join(dumpDir, "1.json"))
			if err != nil {
				t.Fatal(err)
			}
			responseObj := nats.Msg{}
			err = json.Unmarshal(resp, &responseObj)
			if err != nil {
				t.Fatal(err)
			}

			if string(responseObj.Data) != primaryTestMsgData {
				t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--dump=-", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --dump=-", srv.ClientURL()))
			// We trimspace here because some shells can pre- and append whitespaces to the output
			resp := strings.TrimSpace(strings.Split(string(output), "\n")[1])
			resp = resp[:len(resp)-1]

			responseObj := nats.Msg{}
			err = json.Unmarshal([]byte(resp), &responseObj)
			if err != nil {
				t.Fatal(err)
			}

			if string(responseObj.Data) != primaryTestMsgData {
				t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--translate", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --raw --translate=\"wc -c\"", srv.ClientURL())))
			// We trimspace here because some shells can pre- and append whitespaces to the output
			resp := strings.TrimSpace(strings.Split(output, "\n")[1])
			if resp != "19" {
				t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, "19")
			}
			return nil
		})
	})

	t.Run("--raw and --translate with subject", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --raw --translate=\"sed 's/^/{{Subject}}: /'\"", srv.ClientURL())))
			expected := "TEST_STREAM.1: " + primaryTestMsgData
			resp := strings.TrimSpace(strings.Split(output, "\n")[1])
			if resp != expected {
				t.Errorf("unexpected response. Got %q, expected %q", resp, expected)
			}
			return nil
		})
	})

	t.Run("--translate empty message", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(&nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(""),
			})
			if err != nil {
				t.Errorf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --translate=\"wc -c\"", srv.ClientURL())))
			// We trimspace here because some shells can pre- and append whitespaces to the output
			resp := strings.TrimSpace(strings.Split(output, "\n")[2])
			if resp != "0" {
				t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, "19")
			}
			return nil
		})
	})

	t.Run("--dump and --translate", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			dumpDir := t.TempDir()

			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --dump='%s' --translate=\"wc -c\"", srv.ClientURL(), dumpDir))

			entries, err := os.ReadDir(dumpDir)
			if err != nil {
				t.Fatal(err)
			}

			var dumpFile string
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
					if dumpFile != "" {
						t.Fatalf("unexpected file found in dump directory: %s", entry.Name())
					}
					dumpFile = filepath.Join(dumpDir, entry.Name())
				}
			}

			if dumpFile == "" {
				t.Fatalf("no .json file found in dump directory %q with entries %v", dumpDir, entries)
			}

			resp, err := os.ReadFile(dumpFile)
			if err != nil {
				t.Fatal(err)
			}

			responseObj := nats.Msg{}
			err = json.Unmarshal(resp, &responseObj)
			if err != nil {
				t.Fatal(err)
			}

			// We trimspace here because some shells can pre- and append whitespaces to the output
			if strings.TrimSpace(string(responseObj.Data)) != "19" {
				t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), "19")
			}
			return nil
		})
	})

	t.Run("--raw", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --raw", srv.ClientURL())))
			resp := strings.TrimSpace(strings.Split(output, "\n")[1])
			if resp != primaryTestMsgData {
				t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--pretty", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1", srv.ClientURL())))

			if !expectMatchLine(t, output, `\[#\d\] Received JetStream message: stream: TEST_STREAM seq: (\d+) / pending: (\d+) / subject: TEST_STREAM.1 / time: \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d`) {
				t.Fatalf("missing expected summary line:\n%s", output)
			}

			if !expectMatchLine(t, output, regexp.QuoteMeta(primaryTestMsgData)) {
				t.Fatalf("missing expected message body:\n%s", output)
			}

			return nil
		})
	})

	t.Run("--durable with pull", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)
			js, err := jetstream.New(nc)
			if err != nil {
				return err
			}

			err = nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			ctx := context.Background()
			_, err = js.CreateConsumer(ctx, "TEST_STREAM", jetstream.ConsumerConfig{
				Durable:   "TEST_PULL",
				AckPolicy: jetstream.AckExplicitPolicy,
			})
			if err != nil {
				t.Error(err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --durable=TEST_PULL --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing to JetStream Stream \"TEST_STREAM\" using existing pull consumer \"TEST_PULL\"") ||
				!expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--durable and --direct", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			_, err := runNatsCliWithInput(t, "", fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --durable TEST --direct", srv.ClientURL()))
			if !strings.Contains(err.Error(), "cannot use direct get when a durable name is supplied") {
				t.Fatalf("expected durable+direct error, got %v", err)
			}

			return nil
		})
	})

	t.Run("--durable with push", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)
			js, err := jetstream.New(nc)
			if err != nil {
				return err
			}
			err = nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			ctx := context.Background()
			_, err = js.CreateConsumer(ctx, "TEST_STREAM", jetstream.ConsumerConfig{
				Durable:        "TEST_PUSH",
				AckPolicy:      jetstream.AckExplicitPolicy,
				DeliverSubject: nats.NewInbox(),
			})
			if err != nil {
				t.Error(err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --durable=TEST_PUSH --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing to JetStream Stream \"TEST_STREAM\" using existing push consumer \"TEST_PUSH\"") ||
				!expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--durable with interest streams", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.InterestRetention())

			js, err := jetstream.New(nc)
			checkErr(t, err, "unable to create jetstream context")

			_, err = js.CreateConsumer(context.TODO(), "TEST_STREAM", jetstream.ConsumerConfig{
				Durable:        "TEST_PUSH",
				AckPolicy:      jetstream.AckExplicitPolicy,
				DeliverSubject: nats.NewInbox(),
				DeliverGroup:   "X",
			})
			checkErr(t, err, "unable to create consumer")

			_, err = js.PublishMsg(context.TODO(), defaultTestMsg)
			checkErr(t, err, "unable to publish message")

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --durable=TEST_PUSH --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing to JetStream Stream \"TEST_STREAM\" using existing push consumer \"TEST_PUSH\"") ||
				!expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}

			return nil
		})
	})

	t.Run("--headers-only", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			nc.PublishMsg(&nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(primaryTestMsgData),
				Header: nats.Header{
					"test": []string{"header"},
				},
			})

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --headers-only --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "test: header") || expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--subjects-only", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			defer srv.Shutdown()
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --subjects-only --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "Received JetStream message: stream: TEST_STREAM") || expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--start-sequence", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			newMsg := nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(secondaryTestMsgData),
			}
			err = nc.PublishMsg(&newMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --start-sequence=2 --last --count=1", srv.ClientURL())))
			if expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--all", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.PublishMsg(&nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(secondaryTestMsgData),
			})
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --all --count=2", srv.ClientURL())))
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--new", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			outputCh := startNatsCliConsumer(t, mgr, "TEST_STREAM", fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --new --count=1", srv.ClientURL()))

			newMsg := &nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(secondaryTestMsgData),
			}
			err = nc.PublishMsg(newMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-outputCh
			if expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--since", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --since=10s --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--last-per-subject", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.PublishMsg(&nats.Msg{
				Subject: "TEST_STREAM.1",
				Data:    []byte(secondaryTestMsgData),
			})
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --last-per-subject --count=1", srv.ClientURL())))
			if expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--ignore-subject", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.PublishMsg(&nats.Msg{
				Subject: "TEST_STREAM.2",
				Data:    []byte(secondaryTestMsgData),
			})
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --ignore-subject=TEST_STREAM.1 --count=1", srv.ClientURL())))
			if expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--wait", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)
			// The test here is to put us in a state that will block, and make sure --wait breaks us out.
			// symptoms of a failing --wait flag will be a test that times out
			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --wait=100ms --count=100", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--timestamp", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --count=1 --timestamp", srv.ClientURL())))
			// [#1] @ Aug  6 10:12:56.598999 Received JetStream message:
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `\[#\d+\] @ [A-Z][a-z]{2} {1,2}\d{1,2} \d{2}:\d{2}:\d{2}\.\d{6} Received JetStream message:`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--delta-time", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --count=1 --delta-time", srv.ClientURL())))

			// [#1] @ 226.885ms Received JetStream message:
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `\[#\d+\] @ \d+(?:\.\d+)?(ns|µs|ms|s) Received JetStream message:`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --count=1", srv.ClientURL())))
			// [#18] Received JetStream message (direct): stream: ORDERS seq 2714 / subject: ORDERS.NEW / time: 2025-08-05 11:46:12
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `^\[#\d+\].* Received JetStream message \(direct\): stream: [^ ]+ seq \d+ / subject: [^ ]+ / time: .`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct without stream", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.1 --direct --raw --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with start sequence", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.Publish("TEST_STREAM.new", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --start-sequence=2", srv.ClientURL())))

			// 11:49:38 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* starting with sequence 2
			if !expectMatchLine(t, output, secondaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* starting with sequence 2`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with last", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.Publish("TEST_STREAM.new", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --last", srv.ClientURL())))
			// 11:56:47 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* starting with the last message received
			if !expectMatchLine(t, output, secondaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* starting with the last message received`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with deliver all", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.Publish("TEST_STREAM.new", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --all", srv.ClientURL())))
			// 11:58:16 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* starting with the first message received
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* starting with the first message received`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with new", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			outputCh := startNatsCli(t, srv, nc, 2, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --new", srv.ClientURL()))
			err = nc.Publish("TEST_STREAM.new", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-outputCh
			// 11:59:04 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* delivering any new messages received
			if !expectMatchLine(t, output, secondaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* delivering any new messages received`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with since", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --since=10s", srv.ClientURL())))
			// 11:59:51 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* starting with messages since 10.00s
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* starting with messages since 10.00s`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--direct with last for subject", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --stream=TEST_STREAM --raw --count=1 --last-per-subject --direct", srv.ClientURL())))
			// 12:01:32 Subscribing to JetStream Stream (direct) holding messages with subject TEST_STREAM.* for the last messages for each subject in the Stream
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `^\d{2}:\d{2}:\d{2} Subscribing to JetStream Stream \(direct\) holding messages with subject TEST_STREAM\.\* for the last messages for each subject in the Stream`) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--stream with multiple subjects", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			_, err := mgr.NewStream("MULTI_SUBJECT", jsm.Replicas(1), jsm.Subjects("events.lifecycle.>", "events.machine.>"))
			if err != nil {
				t.Fatalf("unable to create stream: %s", err)
			}

			err = nc.Publish("events.lifecycle.start", []byte(primaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			err = nc.Publish("events.machine.status", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream MULTI_SUBJECT --all --count=2", srv.ClientURL())))
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, secondaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("subjects and --durable", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --stream=TEST_STREAM --raw --count=1 --last-per-subject --direct", srv.ClientURL()))
			if err == nil {
				t.Error("expected error, got none")
			}
			return nil
		})
	})
}

type capturedMsg struct {
	subject string
	header  nats.Header
	data    []byte
}

func readCapture(t *testing.T, dir string) []capturedMsg {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, backup.DataFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var msgs []capturedMsg
	dec := backup.NewDecoder(f)
	for {
		item, err := dec.Next()
		if err != nil {
			t.Fatalf("capture %s does not decode: %v", dir, err)
		}
		switch it := item.(type) {
		case *backup.Message:
			body, err := io.ReadAll(it.Body)
			if err != nil {
				t.Fatal(err)
			}
			m := capturedMsg{subject: it.Subject, data: body[it.HdrSize:]}
			if it.HdrSize > 0 {
				m.header, err = nats.DecodeHeadersMsg(body[:it.HdrSize])
				if err != nil {
					t.Fatal(err)
				}
			}
			msgs = append(msgs, m)
		case backup.End:
			return msgs
		}
	}
}

func TestNatsSubscribeBackup(t *testing.T) {
	t.Run("core capture restores", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub 'TEST.*' --backup '%s' --count 5 --wait 5s", srv.ClientURL(), dir)))
			}()
			time.Sleep(500 * time.Millisecond)

			for i := 1; i <= 5; i++ {
				msg := nats.NewMsg(fmt.Sprintf("TEST.%d", i))
				msg.Data = fmt.Appendf(nil, "message %d", i)
				msg.Header.Set("X-Index", fmt.Sprint(i))
				if err := nc.PublishMsg(msg); err != nil {
					t.Fatal(err)
				}
			}
			output := <-done
			if !expectMatchLine(t, output, `Backup of 5 messages \(.*\) written to .*CAPTURED, 0 dropped`) {
				t.Fatalf("missing summary line:\n%s", output)
			}

			output = string(runNatsCli(t, fmt.Sprintf("backup validate '%s'", dir)))
			if !expectMatchLine(t, output, `^OK: 7 entries, 0 consumers, 5 messages, 5 subjects, sequences 1 to 5$`) {
				t.Fatalf("unexpected validate output: %s", output)
			}

			output = string(runNatsCli(t, fmt.Sprintf("backup info '%s' --no-progress", dir)))
			err := expectMatchJSON(t, output, map[string]any{
				"Configuration": map[string]any{"Name": "^CAPTURED$", "Subjects": `^TEST\.\*$`, "Storage": "^File$"},
				"Messages":      map[string]any{"Messages": "^5$", "Subjects": "^5$"},
				"Source":        map[string]any{"Subjects": `^TEST\.\*$`, "Dropped": "^0$", "Started": `^\d{4}-`, "Ended": `^\d{4}-`},
			})
			if err != nil {
				t.Fatalf("source block not rendered: %v: %s", err, output)
			}
			if strings.Contains(output, "Edit") {
				t.Fatalf("capture rendered as an edit: %s", output)
			}

			runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream '%s' --no-progress", srv.ClientURL(), dir))
			stream, err := mgr.LoadStream("CAPTURED")
			if err != nil {
				t.Fatalf("restored stream missing: %v", err)
			}
			if !reflect.DeepEqual(stream.Subjects(), []string{"TEST.*"}) || stream.Storage() != api.FileStorage {
				t.Fatalf("unexpected restored config %+v", stream.Configuration())
			}
			state, err := stream.State()
			if err != nil {
				t.Fatal(err)
			}
			if state.Msgs != 5 || state.FirstSeq != 1 || state.LastSeq != 5 {
				t.Fatalf("unexpected restored state %+v", state)
			}
			for i := 1; i <= 5; i++ {
				msg, err := stream.ReadMessage(uint64(i))
				if err != nil {
					t.Fatal(err)
				}
				hdr, _ := nats.DecodeHeadersMsg(msg.Header)
				if msg.Subject != fmt.Sprintf("TEST.%d", i) || string(msg.Data) != fmt.Sprintf("message %d", i) || hdr.Get("X-Index") != fmt.Sprint(i) {
					t.Fatalf("message %d restored as %+v", i, msg)
				}
			}
			return nil
		})
	})

	t.Run("invalid directory name refused", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dir := filepath.Join(t.TempDir(), "with.dot")

			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub TEST --backup '%s' --wait 100ms", srv.ClientURL(), dir)); err == nil {
				t.Fatal("invalid default stream name accepted")
			}
			if _, err := os.Stat(dir); err == nil {
				t.Fatal("refused backup created its directory")
			}
			return nil
		})
	})

	t.Run("--match-replies", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			_, err := nc.Subscribe("REQ", func(msg *nats.Msg) {
				nc.Publish(msg.Reply, []byte("pong"))
			})
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub REQ --match-replies --backup '%s' --count 1 --wait 5s", srv.ClientURL(), dir)))
			}()
			time.Sleep(500 * time.Millisecond)

			if _, err := nc.Request("REQ", []byte("ping"), time.Second); err != nil {
				t.Fatal(err)
			}
			<-done

			msgs := readCapture(t, dir)
			if len(msgs) != 2 || msgs[0].subject != "REQ" || string(msgs[0].data) != "ping" || !strings.HasPrefix(msgs[1].subject, "_INBOX.") || string(msgs[1].data) != "pong" {
				t.Fatalf("unexpected capture %+v", msgs)
			}
			return nil
		})
	})

	t.Run("--quiet", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dir := filepath.Join(t.TempDir(), "CAPTURED")
			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub TEST --quiet --wait 100ms", srv.ClientURL())); err == nil {
				t.Fatal("--quiet without --backup accepted")
			}
			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub TEST --quiet --backup '%s' --dump '%s' --wait 100ms", srv.ClientURL(), dir, t.TempDir())); err == nil {
				t.Fatal("--quiet with --dump accepted")
			}

			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --quiet --backup '%s' --count 2 --wait 5s", srv.ClientURL(), dir)))
			}()
			time.Sleep(500 * time.Millisecond)

			for i := 1; i <= 2; i++ {
				msg := nats.NewMsg("TEST")
				msg.Data = fmt.Appendf(nil, "%s %d", primaryTestMsgData, i)
				msg.Header.Set("X-Index", fmt.Sprint(i))
				if err := nc.PublishMsg(msg); err != nil {
					t.Fatal(err)
				}
			}
			output := <-done

			if strings.Contains(output, primaryTestMsgData) || strings.Contains(output, "X-Index") || strings.Contains(output, "Received on") {
				t.Fatalf("messages printed under --quiet:\n%s", output)
			}
			if !expectMatchLine(t, output, `Backup of 2 messages`) {
				t.Fatalf("missing summary line:\n%s", output)
			}
			msgs := readCapture(t, dir)
			if len(msgs) != 2 || string(msgs[1].data) != primaryTestMsgData+" 2" || msgs[1].header.Get("X-Index") != "2" {
				t.Fatalf("unexpected capture %+v", msgs)
			}
			return nil
		})
	})

	t.Run("--report-subjects refused", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dir := filepath.Join(t.TempDir(), "CAPTURED")
			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub 'TEST.*' --report-subjects --backup '%s' --wait 100ms", srv.ClientURL(), dir)); err == nil {
				t.Fatal("--backup with --report-subjects accepted")
			}
			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub 'TEST.*' --graph --backup '%s' --wait 100ms", srv.ClientURL(), dir)); err == nil {
				t.Fatal("--backup with --graph accepted")
			}
			return nil
		})
	})

	t.Run("--dump", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			dir := filepath.Join(t.TempDir(), "CAPTURED")
			dumpDir := t.TempDir()

			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --dump '%s' --backup '%s' --count 1 --wait 5s", srv.ClientURL(), dumpDir, dir)))
			}()
			time.Sleep(500 * time.Millisecond)

			if err := nc.Publish("TEST", []byte(primaryTestMsgData)); err != nil {
				t.Fatal(err)
			}
			<-done

			if _, err := os.Stat(filepath.Join(dumpDir, "1.json")); err != nil {
				t.Fatalf("dump file missing: %v", err)
			}
			msgs := readCapture(t, dir)
			if len(msgs) != 1 || string(msgs[0].data) != primaryTestMsgData {
				t.Fatalf("unexpected capture %+v", msgs)
			}
			return nil
		})
	})
}

func TestJetStreamSubscribeBackup(t *testing.T) {
	publishIndexed := func(t *testing.T, nc *nats.Conn, n int) {
		t.Helper()
		for i := 1; i <= n; i++ {
			msg := nats.NewMsg("TEST_STREAM.1")
			msg.Data = fmt.Appendf(nil, "message %d", i)
			msg.Header.Set("X-Index", fmt.Sprint(i))
			if _, err := nc.RequestMsg(msg, time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}

	originals := func(t *testing.T, mgr *jsm.Manager, n int) []*api.StoredMsg {
		t.Helper()
		stream, err := mgr.LoadStream("TEST_STREAM")
		if err != nil {
			t.Fatal(err)
		}
		var msgs []*api.StoredMsg
		for i := 1; i <= n; i++ {
			msg, err := stream.ReadMessage(uint64(i))
			if err != nil {
				t.Fatal(err)
			}
			msgs = append(msgs, msg)
		}
		return msgs
	}

	t.Run("--all restores as the stream config", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.MaxMessages(1000))
			publishIndexed(t, nc, 3)
			want := originals(t, mgr, 3)
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --all --backup '%s' --terminate-at-end", srv.ClientURL(), dir)))
			if !expectMatchLine(t, output, `Backup of 3 messages`) {
				t.Fatalf("missing summary line:\n%s", output)
			}

			nfo, err := backup.Info(dir)
			if err != nil {
				t.Fatal(err)
			}
			if nfo.Source.Stream != "TEST_STREAM" || !reflect.DeepEqual(nfo.Source.Subjects, []string{"TEST_STREAM.*"}) {
				t.Fatalf("unexpected source block %+v", nfo.Source)
			}

			if nfo.Config.Name != "TEST_STREAM" {
				t.Fatalf("stream name changed to %q", nfo.Config.Name)
			}

			if err := mgr.DeleteStream("TEST_STREAM"); err != nil {
				t.Fatal(err)
			}
			runNatsCli(t, fmt.Sprintf("--server='%s' backup restore stream '%s' --no-progress", srv.ClientURL(), dir))
			stream, err := mgr.LoadStream("TEST_STREAM")
			if err != nil {
				t.Fatalf("restored stream missing: %v", err)
			}
			if stream.MaxMsgs() != 1000 || !reflect.DeepEqual(stream.Subjects(), []string{"TEST_STREAM.*"}) {
				t.Fatalf("stream config not copied: %+v", stream.Configuration())
			}
			state, err := stream.State()
			if err != nil {
				t.Fatal(err)
			}
			if state.Msgs != 3 || state.FirstSeq != 1 || state.LastSeq != 3 {
				t.Fatalf("unexpected restored state %+v", state)
			}
			for i, w := range want {
				got, err := stream.ReadMessage(uint64(i + 1))
				if err != nil {
					t.Fatal(err)
				}
				if string(got.Data) != string(w.Data) || !got.Time.Equal(w.Time) || !reflect.DeepEqual(got.Header, w.Header) {
					t.Fatalf("message %d restored as %+v, want %+v", i+1, got, w)
				}
			}
			return nil
		})
	})

	t.Run("--direct strips the response headers", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect())
			publishIndexed(t, nc, 2)
			want := originals(t, mgr, 2)
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --all --direct --backup '%s' --count 2", srv.ClientURL(), dir))

			msgs := readCapture(t, dir)
			if len(msgs) != 2 {
				t.Fatalf("expected 2 messages, got %d", len(msgs))
			}
			for i, m := range msgs {
				if !reflect.DeepEqual(m.header, nats.Header{"X-Index": {fmt.Sprint(i + 1)}}) || string(m.data) != string(want[i].Data) {
					t.Fatalf("message %d captured as %+v", i+1, m)
				}
			}
			nfo, err := backup.Info(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !nfo.FirstTime.Equal(want[0].Time) || !nfo.LastTime.Equal(want[1].Time) {
				t.Fatalf("stream timestamps not kept: %s %s vs %s %s", nfo.FirstTime, nfo.LastTime, want[0].Time, want[1].Time)
			}
			return nil
		})
	})

	t.Run("--last-per-subject refused on direct", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1, jsm.AllowDirect(), jsm.WorkQueueRetention())
			publishIndexed(t, nc, 1)
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			if err := runNatsCliWithError(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last-per-subject --backup '%s' --terminate-at-end", srv.ClientURL(), dir)); err == nil {
				t.Fatal("--last-per-subject accepted on a direct backup")
			}
			if _, err := os.Stat(dir); err == nil {
				t.Fatal("refused backup created its directory")
			}
			return nil
		})
	})

	t.Run("--headers-only", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)
			publishIndexed(t, nc, 1)
			dir := filepath.Join(t.TempDir(), "CAPTURED")

			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --all --headers-only --backup '%s' --count 1", srv.ClientURL(), dir))

			msgs := readCapture(t, dir)
			if len(msgs) != 1 || len(msgs[0].data) != 0 || msgs[0].header.Get("X-Index") != "1" || msgs[0].header.Get(server.JSMsgSize) != fmt.Sprint(len("message 1")) {
				t.Fatalf("unexpected capture %+v", msgs)
			}
			return nil
		})
	})
}

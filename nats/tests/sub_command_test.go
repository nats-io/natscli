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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
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
			dumpDir := "/tmp/test1"
			t.Cleanup(func() { _ = os.RemoveAll(dumpDir) })

			done := make(chan struct{})
			go func() {
				runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump=%s", srv.ClientURL(), dumpDir))
				close(done)
			}()

			time.Sleep(500 * time.Millisecond)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatal(err)
			}

			<-done

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
			out := make(chan []byte)
			go func() {
				out <- runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump=-", srv.ClientURL()))
			}()

			time.Sleep(500 * time.Millisecond)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out

			resp := strings.TrimSpace(string(output))
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
			out := make(chan []byte)
			go func() {
				out <- runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --raw --translate='wc -c'", srv.ClientURL()))
			}()

			time.Sleep(500 * time.Millisecond)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			resp := strings.TrimSpace(string(output))
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

	t.Run("--translate empty message", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			out := make(chan []byte)
			go func() {
				out <- runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --translate=\"wc -c\"", srv.ClientURL()))
			}()

			time.Sleep(500 * time.Millisecond)

			err := nc.PublishMsg(&nats.Msg{
				Subject: "TEST",
				Data:    []byte(""),
			})
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")
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
			dumpDir := "/tmp/test_dump_and_translate"
			t.Cleanup(func() { _ = os.RemoveAll(dumpDir) })

			done := make(chan struct{})
			go func() {
				runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --dump=%s --translate=\"wc -c\"", srv.ClientURL(), dumpDir))
				close(done)
			}()

			time.Sleep(500 * time.Millisecond)

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
				t.Fatal("no .json file found in dump directory")
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
			out := make(chan []byte)
			go func() {
				out <- runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1 --raw", srv.ClientURL()))
			}()

			time.Sleep(500 * time.Millisecond)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-out
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")

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
			out := make(chan string)
			go func() {
				out <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --count=1", srv.ClientURL())))
			}()

			time.Sleep(500 * time.Millisecond)

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

			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub %s --queue=%s --count=1", srv.ClientURL(), subject, queue)))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(500 * time.Millisecond)

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
			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM --count=1 --ack", srv.ClientURL())))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(1 * time.Second)
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

			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub > --match-replies --count=1 --wait=2s", srv.ClientURL())))
			}()

			_, err = nc.Request("TEST_STREAMJECT", []byte("test request"), 1*time.Second)
			if err != nil {
				t.Error(err)
			}

			output := <-outputCh
			if !expectMatchLine(t, output, `Matched reply on "_INBOX\.`) &&
				!expectMatchLine(t, output, `Matching replies with inbox prefix _INBOX\.>?`) {
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
			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --headers-only --count=1", srv.ClientURL())))
			}()

			time.Sleep(500 * time.Millisecond)

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
			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST --subjects-only --count=1", srv.ClientURL())))
			}()

			time.Sleep(500 * time.Millisecond)

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
			done := make(chan string)
			go func() {
				done <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST.* --ignore-subject=TEST --count=1", srv.ClientURL())))
			}()

			time.Sleep(500 * time.Millisecond)

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
			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST.* --count=1 --timestamp", srv.ClientURL())))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(1 * time.Second)
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
			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST.* --count=1 --delta-time", srv.ClientURL())))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(1 * time.Second)
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

	t.Run("--dump=file", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr, 1)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --dump=/tmp/test1", srv.ClientURL()))
			defer os.RemoveAll("/tmp/test1/")

			resp, err := os.ReadFile("/tmp/test1/1.json")
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

			dumpDir := "/tmp/test_dump_and_translate"

			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1 --dump=%s --translate=\"wc -c\"", srv.ClientURL(), dumpDir))
			defer os.RemoveAll(dumpDir)

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
				t.Fatal("no .json file found in dump directory")
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

			if !expectMatchLine(t, output, `\[#\d\] Received JetStream message: stream: TEST_STREAM seq (\d+) / subject: TEST_STREAM.1 / time: \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d`) {
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

			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --new --count=1", srv.ClientURL())))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(500 * time.Millisecond)

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
			if !expectMatchLine(t, output, primaryTestMsgData) || !expectMatchLine(t, output, `\[#\d+\] @ \d+\.\d{3,6}(ms|s|µs|ns) Received JetStream message:`) {
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
			if !expectMatchLine(t, output, primaryTestMsgData) {
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
			if !expectMatchLine(t, output, secondaryTestMsgData) {
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
			if !expectMatchLine(t, output, secondaryTestMsgData) {
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
			if !expectMatchLine(t, output, primaryTestMsgData) {
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

			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream=TEST_STREAM --direct --raw --count=1 --new", srv.ClientURL())))
			}()

			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(1 * time.Second)
			err = nc.Publish("TEST_STREAM.new", []byte(secondaryTestMsgData))
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := <-outputCh
			if !expectMatchLine(t, output, secondaryTestMsgData) {
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
			if !expectMatchLine(t, output, primaryTestMsgData) {
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

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --stream=TEST_STREAM --raw --count=1 --last-per-subject", srv.ClientURL())))
			if !expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})
}

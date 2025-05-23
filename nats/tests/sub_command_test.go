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
)

func TestSubscribe(t *testing.T) {
	const (
		primaryTestMsgData      = "primary test string"
		secondaryTestMsgData    = "secondary test string"
		nonJetstreamTestMsgData = "not jetstream test string"
	)

	var (
		defaultTestMsg = &nats.Msg{
			Subject: "TEST_STREAM.1",
			Data:    []byte(primaryTestMsgData),
		}
	)

	t.Run("--dump=file", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --last --count=1", srv.ClientURL())))
			pattern := `\[#\d\] Received JetStream message: stream: TEST_STREAM seq (\d+) / subject: TEST_STREAM.1 / time: \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ`
			matcher := regexp.MustCompile(pattern)
			outputSlice := strings.Split(output, "\n")

			if !matcher.Match([]byte(outputSlice[1])) {
				t.Errorf("unexpected response. Got \"%s\" expected string to match\"%s\"", outputSlice[1], pattern)
			}

			if outputSlice[2] != primaryTestMsgData {
				t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", outputSlice[2], primaryTestMsgData)
			}
			return nil
		})
	})

	t.Run("--queue", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			subject := "TEST_STREAM"
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
				nc.Publish("TEST_STREAM", []byte(nonJetstreamTestMsgData))
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

	t.Run("--durable", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			// We only support push consumers currently
			_, err = mgr.NewConsumer("TEST_STREAM", jsm.ConsumerName("TEST_CONSUMER"), jsm.DeliverySubject("NEW_TEST_STREAM.1"))
			if err != nil {
				t.Error(err)
			}

			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --durable=TEST_CONSUMER --last --count=1", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing to JetStream Stream \"TEST_STREAM\" using existing durable \"TEST_CONSUMER\"") ||
				!expectMatchLine(t, output, primaryTestMsgData) {
				t.Errorf("unexpected response: %s", output)
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
			_, err := nc.Subscribe("TEST_SUBJECT", func(msg *nats.Msg) {
				reply := fmt.Sprintf("test reply %s", string(msg.Data))
				nc.Publish(msg.Reply, []byte(reply))
			})
			if err != nil {
				t.Error(err)
			}

			outputCh := make(chan string)
			go func() {
				outputCh <- string(runNatsCli(t, fmt.Sprintf("--server='%s' sub > --match-replies --count=1 --wait=1s", srv.ClientURL())))
			}()
			// Give the process time to spawn in the go routine. it can be slow in a test environment
			time.Sleep(1 * time.Second)

			_, err = nc.Request("TEST_SUBJECT", []byte("test request"), 1*time.Second)
			if err != nil {
				t.Error(err)
			}

			output := <-outputCh
			if !expectMatchLine(t, output, "Matched reply on \"_INBOX.") || !expectMatchLine(t, output, "test reply test request") {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--inbox", func(t *testing.T) {
		withNatsServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn) error {
			output := string(runNatsCli(t, fmt.Sprintf("--server='%s' sub --wait=1ms --inbox", srv.ClientURL())))
			if !expectMatchLine(t, output, "Subscribing on _INBOX.") {
				t.Errorf("unexpected response: %s", output)
			}
			return nil
		})
	})

	t.Run("--headers-only", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

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
			createDefaultTestStream(t, mgr)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			// The test here is to put us in a state that will block, and make sure --wait breaks us out.
			// symptoms of a failing --wait flag will be a test that times out
			runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream TEST_STREAM --wait=1ms --count=100", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--report-subjects", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {
			createDefaultTestStream(t, mgr)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			// This test returns long running output. best we can do here is check if it runs without failure
			runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --report-subjects --report-top=1 --wait=1ms", srv.ClientURL()))
			return nil
		})
	})

	t.Run("--report-subscriptions", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, srv *server.Server, nc *nats.Conn, mgr *jsm.Manager) error {

			createDefaultTestStream(t, mgr)

			err := nc.PublishMsg(defaultTestMsg)
			if err != nil {
				t.Fatalf("unable to publish message: %s", err)
			}
			// This test returns long running output. best we can do here is check if it runs without failure
			runNatsCli(t, fmt.Sprintf("--server='%s' sub TEST_STREAM.* --report-subscriptions --report-top=1 --wait=1ms", srv.ClientURL()))
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

			if !expectMatchLine(t, output, nonJetstreamTestMsgData) || !expectMatchLine(t, output, `[A-Z][a-z]{2} \d{1,2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?`) {
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

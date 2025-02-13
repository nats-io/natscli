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
	"regexp"
	"strings"
	"testing"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
)

func TestSubscribe(t *testing.T) {
	srv, nc, mgr := setupJStreamTest(t)
	testMsgData := "this is a test string"
	defer srv.Shutdown()

	_, err := mgr.NewStream("ORDERS", jsm.Subjects("ORDERS.*"))
	if err != nil {
		t.Fatalf("unable to create stream: %s", err)
	}
	msg := nats.Msg{
		Subject: "ORDERS.1",
		Data:    []byte(testMsgData),
	}

	t.Run("--dump=file", func(t *testing.T) {
		nc.PublishMsg(&msg)
		runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --dump=/tmp/test1", srv.ClientURL()))
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

		if string(responseObj.Data) != testMsgData {
			t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), testMsgData)
		}
	})

	t.Run("--dump=-", func(t *testing.T) {
		nc.PublishMsg(&msg)
		output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --dump=-", srv.ClientURL()))
		// We trimspace here because some shells can pre- and append whitespaces to the output
		resp := strings.TrimSpace(strings.Split(string(output), "\n")[1])
		resp = resp[:len(resp)-1]

		responseObj := nats.Msg{}
		err := json.Unmarshal([]byte(resp), &responseObj)
		if err != nil {
			t.Fatal(err)
		}

		if string(responseObj.Data) != testMsgData {
			t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), testMsgData)
		}
	})

	t.Run("--translate", func(t *testing.T) {
		nc.PublishMsg(&msg)
		output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --raw --translate=\"wc -c\"", srv.ClientURL()))
		// We trimspace here because some shells can pre- and append whitespaces to the output
		resp := strings.TrimSpace(strings.Split(string(output), "\n")[1])
		if resp != "21" {
			t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, "21")
		}
	})

	t.Run("--translate empty message", func(t *testing.T) {
		empty := nats.Msg{
			Subject: "ORDERS.1",
			Data:    []byte(""),
		}
		nc.PublishMsg(&empty)
		output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --translate=\"wc -c\"", srv.ClientURL()))
		// We trimspace here because some shells can pre- and append whitespaces to the output
		resp := strings.TrimSpace(strings.Split(string(output), "\n")[2])
		if resp != "0" {
			t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, "21")
		}
	})

	t.Run("--dump and --translate", func(t *testing.T) {
		nc.PublishMsg(&msg)
		runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --dump=/tmp/test2 --translate=\"wc -c\"", srv.ClientURL()))
		defer os.RemoveAll("/tmp/test2")

		resp, err := os.ReadFile("/tmp/test2/5.json")
		if err != nil {
			t.Fatal(err)
		}
		responseObj := nats.Msg{}
		err = json.Unmarshal(resp, &responseObj)
		if err != nil {
			t.Fatal(err)
		}

		// We trimspace here because some shells can pre- and append whitespaces to the output
		if strings.TrimSpace(string(responseObj.Data)) != "21" {
			t.Errorf("unexpected data section of message. Got \"%s\" expected \"%s\"", string(responseObj.Data), "21")
		}
	})

	t.Run("--raw", func(t *testing.T) {
		nc.PublishMsg(&msg)
		output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1 --raw", srv.ClientURL()))
		resp := strings.TrimSpace(strings.Split(string(output), "\n")[1])
		if resp != testMsgData {
			t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", resp, testMsgData)
		}
	})

	t.Run("--pretty", func(t *testing.T) {
		nc.PublishMsg(&msg)
		output := runNatsCli(t, fmt.Sprintf("--server='%s' sub --stream ORDERS --last --count=1", srv.ClientURL()))
		pattern := `\[#\d\] Received JetStream message: stream: ORDERS seq (\d+) / subject: ORDERS.1 / time: \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ`
		matcher := regexp.MustCompile(pattern)
		outputSlice := strings.Split(string(output), "\n")

		if !matcher.Match([]byte(outputSlice[1])) {
			t.Errorf("unexpected response. Got \"%s\" expected string to match\"%s\"", outputSlice[1], pattern)
		}

		if outputSlice[2] != testMsgData {
			t.Errorf("unexpected response. Got \"%s\" expected \"%s\"", outputSlice[2], testMsgData)
		}
	})
}

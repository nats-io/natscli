// Copyright 2024-2025 The NATS Authors
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

import (
	"errors"
	"github.com/google/go-cmp/cmp"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestIsJsonObjectString(t *testing.T) {
	if !IsJsonObjectString("{}") {
		t.Fatalf("Expected {} to be an object")
	}
	if !IsJsonObjectString(`{"test":test""}`) {
		t.Fatalf("Expected {} to be an object")
	}
	if IsJsonObjectString("[]") {
		t.Fatalf("Expected [] to not be an object")
	}
}

func TestSplitString(t *testing.T) {
	res := SplitString("test test test") // string has a EMSP unicode space
	if !cmp.Equal(res, []string{"test", "test", "test"}) {
		t.Fatalf("invalid split result: %#v", res)
	}
}

func TestCompactStrings(t *testing.T) {
	names := []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-1.broker-broker-ss.choria.svc.cluster.local",
	}

	result := CompactStrings(names)
	if !cmp.Equal(result, []string{"broker-broker-2", "broker-broker-0", "broker-broker-1"}) {
		t.Fatalf("Recevied %#v", result)
	}

	names = []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local1",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local2",
		"broker-broker-1.broker-broker-ss.choria.svc.cluster.local3",
	}
	result = CompactStrings(names)
	if !cmp.Equal(result, names) {
		t.Fatalf("Recevied %#v", result)
	}

	names = []string{
		"broker-broker-2.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-0.broker-broker-ss.choria.svc.cluster.local",
		"broker-broker-1.broker-broker-ss.other.svc.cluster.local",
	}
	result = CompactStrings(names)
	if !cmp.Equal(result, []string{"broker-broker-2.broker-broker-ss.choria", "broker-broker-0.broker-broker-ss.choria", "broker-broker-1.broker-broker-ss.other"}) {
		t.Fatalf("Recevied %#v", result)
	}
}

func TestSplitCLISubjects(t *testing.T) {
	res := SplitCLISubjects([]string{"one two three	four,five", "six,seven"}) // string has a EMSP unicode space
	if !cmp.Equal(res, []string{"one", "two", "three", "four", "five", "six", "seven"}) {
		t.Fatalf("invalid split result: %#v", res)
	}
}

func TestIsPrintable(t *testing.T) {
	if !IsPrintable("test") {
		t.Fatalf("Expected 'test' to be printable")
	}

	if IsPrintable(" ") { // string has a EMSP unicode space
		t.Fatalf("Expected emsp to be unprintable")
	}
}

func TestBase64IfNotPrintable(t *testing.T) {
	if Base64IfNotPrintable([]byte("test")) != "test" {
		t.Fatalf("Expected 'test' returned verbatim")
	}

	if Base64IfNotPrintable([]byte("test test")) != "dGVzdOKAg3Rlc3Q=" { // string has a EMSP unicode space
		t.Fatalf("Expected non printable text to be base64 encoded")
	}
}

func TestMultipleSort(t *testing.T) {
	if SortMultiSort(1, 1, "b", "a") {
		t.Fatalf("expected true")
	}

	if !SortMultiSort(1, 1, "a", "b") {
		t.Fatalf("expected false")
	}

	if SortMultiSort(1, 2, "a", "b") {
		t.Fatalf("expected false")
	}

	if !SortMultiSort(2, 1, "a", "b") {
		t.Fatalf("expected true")
	}
}

func TestSplitCommand(t *testing.T) {
	cmd, args, err := SplitCommand("vim")
	if err != nil {
		t.Fatalf("Expected err to be nil, got %v", err)
	}
	if cmd != "vim" && len(args) != 0 {
		t.Fatalf("Expected vim and [], got %v and %v", cmd, args)
	}

	cmd, args, err = SplitCommand("code --wait")
	if err != nil {
		t.Fatalf("Expected err to be nil, got %v", err)
	}
	if cmd != "code" && !slices.Equal(args, []string{"--wait"}) {
		t.Fatalf("Expected code and [\"--wait\"], got %v and %v", cmd, args)
	}

	cmd, args, err = SplitCommand("code --wait --new-window")
	if err != nil {
		t.Fatalf("Expected err to be nil, got %v", err)
	}
	if cmd != "code" && !slices.Equal(args, []string{"--wait", "--new-window"}) {
		t.Fatalf("Expected code and [\"--wait\", \"--new-window\"], got %v and %v", cmd, args)
	}

	// EOF found when expecting closing quote
	_, _, err = SplitCommand("foo --bar 'hello")
	if err == nil {
		t.Fatal("Expected err to not be nil, got nil")
	}
}

func TestEditFile(t *testing.T) {
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer r.Close()
	defer w.Close()

	f, err := os.CreateTemp("", "test_edit_file")
	if err != nil {
		t.Fatalf("Expected err to be nil, got %v", err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	t.Run("EDITOR unset", func(t *testing.T) {
		os.Unsetenv("EDITOR")
		err := EditFile("")
		if err == nil {
			t.Fatal("Expected err to not be nil, got nil")
		}
	})

	t.Run("EDITOR set", func(t *testing.T) {
		os.Setenv("EDITOR", "echo")
		err := EditFile(f.Name())
		if err != nil {
			t.Fatalf("Expected err to be nil, got %v", err)
		}

		w.Close()
		stdout, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("Expected err to be nil, got %v", err)
		}
		r.Close()

		actual := string(stdout)
		lines := strings.Split(actual, "\n")

		if len(lines) != 2 || lines[1] != "" {
			t.Fatalf("Expected one line of output, got %v", actual)
		}

		if !strings.Contains(lines[0], "test_edit_file") {
			t.Fatalf("Expected echo output, got %v", actual)
		}
	})
}

func TestParseStringAsBytes(t *testing.T) {
	cases := []struct {
		input  string
		expect int64
		error  bool
	}{
		{input: "1", expect: 1},
		{input: "1000", expect: 1000},
		{input: "1K", expect: 1024},
		{input: "1k", expect: 1024},
		{input: "1KB", expect: 1024},
		{input: "1KiB", expect: 1024},
		{input: "1kb", expect: 1024},
		{input: "1M", expect: 1024 * 1024},
		{input: "1MB", expect: 1024 * 1024},
		{input: "1MiB", expect: 1024 * 1024},
		{input: "1m", expect: 1024 * 1024},
		{input: "1G", expect: 1024 * 1024 * 1024},
		{input: "1GB", expect: 1024 * 1024 * 1024},
		{input: "1GiB", expect: 1024 * 1024 * 1024},
		{input: "1g", expect: 1024 * 1024 * 1024},
		{input: "1T", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1TB", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1TiB", expect: 1024 * 1024 * 1024 * 1024},
		{input: "1t", expect: 1024 * 1024 * 1024 * 1024},
		{input: "-1", expect: -1},
		{input: "-10", expect: -1},
		{input: "-10GB", expect: -1},
		{input: "1B", error: true},
		{input: "1FOO", error: true},
		{input: "FOO", error: true},
	}

	for _, c := range cases {
		v, err := ParseStringAsBytes(c.input)
		if c.error {
			if !errors.Is(err, errInvalidByteString) {
				t.Fatalf("expected an invalid bytes error got: %v", err)
			}
		} else {
			if err != nil {
				t.Fatalf("did not expect an error parsing %v: %v", c.input, err)
			}
			if v != c.expect {
				t.Fatalf("expected %v to parse as %d got %d", c.input, c.expect, v)
			}
		}
	}
}

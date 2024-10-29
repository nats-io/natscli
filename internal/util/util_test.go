// Copyright 2024 The NATS Authors
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
	"io"
	"os"
	"slices"
	"strings"
	"testing"
)

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

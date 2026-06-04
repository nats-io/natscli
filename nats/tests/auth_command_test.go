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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	JSON = `
{
	"test.a": [
    	{
        	"subject": "test.b",
        	"weight": 100,
			"cluster": "test_cluster"
      	}
	]
}
`

	YAML = `
test.a:
  - subject: test.b
    weight: 100
    cluster: test_cluster
`
)

var tempDir string

func setup(operator, account string, t *testing.T) {
	t.Helper()
	teardown(t)
	tempDir = t.TempDir()
	err := os.Setenv("XDG_CONFIG_HOME", tempDir)
	if err != nil {
		t.Error(err)
	}
	err = os.Setenv("XDG_DATA_HOME", tempDir)
	if err != nil {
		t.Error(err)
	}

	runNatsCli(t, fmt.Sprintf("auth operator add %s", operator))
	runNatsCli(t, fmt.Sprintf("auth account add --operator=%s --defaults %s", operator, account))
}

func teardown(t *testing.T) {
	t.Helper()
	err := os.Unsetenv("NSC_HOME")
	if err != nil {
		t.Error(err)
	}
}

func TestMapping(t *testing.T) {
	t.Run("--add", func(t *testing.T) {
		accountName, operatorName := "test_account", "test_operator"
		setup(operatorName, accountName, t)
		t.Cleanup(func() {
			teardown(t)
		})

		fields := map[string]any{
			"Configuration": map[string]any{
				"Source":       "test.a",
				"Target":       "test.b",
				"Weight":       "100",
				"Total weight": "100",
			},
		}

		output := string(runNatsCli(t, fmt.Sprintf("auth account mappings add %s test.a test.b 100 --operator=%s", accountName, operatorName)))
		err := expectMatchJSON(t, output, fields)
		if err != nil {
			t.Errorf("failed to add account: %s. %s", err, output)
		}
	})

	t.Run("--add from config", func(t *testing.T) {
		tests := []struct {
			name    string
			fileExt string
			data    string
		}{
			{"--add from config json", "json", JSON},
			{"--add from config yaml", "yaml", YAML},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				accountName, operatorName := "test_account", "test_operator"
				setup(operatorName, accountName, t)
				t.Cleanup(func() { teardown(t) })

				fields := map[string]any{
					"Configuration": map[string]any{
						"Source":       "test.a",
						"Target":       "test.b",
						"Weight":       "100",
						"Total weight": "100",
						"Cluster":      "test_cluster",
					},
				}

				fp := filepath.Join(tempDir, fmt.Sprintf("test.%s", tt.fileExt))
				file, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					t.Fatalf("Error opening file: %s", err)
				}
				defer file.Close()

				_, err = file.WriteString(strings.TrimSpace(tt.data))
				if err != nil {
					t.Fatalf("Error writing to file: %s", err)
				}

				output := string(runNatsCli(t, fmt.Sprintf("auth account mappings add %s --operator=%s --config='%s'", accountName, operatorName, fp)))

				err = expectMatchJSON(t, output, fields)
				if err != nil {
					t.Errorf("failed to add account: %s. %s", err, output)
				}
			})
		}
	})
	t.Run("--ls", func(t *testing.T) {
		accountName, operatorName := "test_account", "test_operator"
		setup(operatorName, accountName, t)
		t.Cleanup(func() {
			teardown(t)
		})

		colums := map[string]*regexp.Regexp{
			"top":    regexp.MustCompile("Subject mappings for account test_account"),
			"middle": regexp.MustCompile("Source Subject │ Target Subject │ Weight │ Cluster"),
			"bottom": regexp.MustCompile("test.a         │ test.b         │    100"),
		}

		runNatsCli(t, fmt.Sprintf("auth account mappings add %s test.a test.b 100 --operator=%s", accountName, operatorName))
		output := runNatsCli(t, fmt.Sprintf("auth account mappings ls %s --operator=%s", accountName, operatorName))

		for name, pattern := range colums {
			if !pattern.Match(output) {
				t.Errorf("%s value does not match expected %s", name, pattern)
			}
		}
	})

	t.Run("--info", func(t *testing.T) {
		accountName, operatorName := "test_account", "test_operator"
		setup(operatorName, accountName, t)
		t.Cleanup(func() {
			teardown(t)
		})

		fields := map[string]any{
			"Configuration": map[string]any{
				"Source":       "test.a",
				"Target":       "test.b",
				"Weight":       "100",
				"Total weight": "100",
			},
		}

		runNatsCli(t, fmt.Sprintf("auth account mappings add %s test.a test.b 100 --operator=%s", accountName, operatorName))
		output := string(runNatsCli(t, fmt.Sprintf("auth account mappings info %s test.a --operator=%s", accountName, operatorName)))

		err := expectMatchJSON(t, output, fields)
		if err != nil {
			t.Errorf("failed to get account info: %s. %s", err, output)
		}
	})

	t.Run("--delete", func(t *testing.T) {
		accountName, operatorName := "test_account", "test_operator"
		setup(operatorName, accountName, t)
		t.Cleanup(func() {
			teardown(t)
		})

		expected := regexp.MustCompile("Deleted mapping {test.a}")

		runNatsCli(t, fmt.Sprintf("auth account mappings add %s test.a test.b 100 --operator=%s", accountName, operatorName))
		output := runNatsCli(t, fmt.Sprintf("auth account mappings rm %s test.a --operator=%s", accountName, operatorName))

		if !expected.Match(output) {
			t.Errorf("failed to delete mapping: %s", output)
		}
	})
}

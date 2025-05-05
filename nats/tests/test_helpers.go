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
	"regexp"
	"strings"
	"testing"
)

func expectMatchRegex(t *testing.T, found, expected string) bool {
	t.Helper()
	return !regexp.MustCompile(expected).MatchString(found)
}

func expectMatchMap(t *testing.T, fields map[string]string, output string) (bool, string, string) {
	t.Helper()
	for field, expected := range fields {
		re := regexp.MustCompile(expected)
		if !re.MatchString(string(output)) {
			return false, field, re.String()
		}
	}
	return true, "", ""
}

func expectMatchLine(t *testing.T, output string, fields ...string) bool {
	t.Helper()
	rowLines := strings.Split(output, "\n")
	found := false

	for _, line := range rowLines {
		line = strings.TrimSpace(line)

		matchesAll := true
		for _, pattern := range fields {
			re, err := regexp.Compile(pattern)
			if err != nil {
				t.Errorf("invalid regex pattern %q: %v", pattern, err)
				matchesAll = false
				break
			}
			if !re.MatchString(line) {
				matchesAll = false
				break
			}
		}

		if matchesAll {
			found = true
			break
		}
	}

	return found
}

func expectMatchJSON(t *testing.T, jsonStr string, expected map[string]any) error {
	t.Helper()

	var actual map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &actual); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return matchRecursive(actual, expected)
}

func matchRecursive(actual map[string]any, expected map[string]any) error {
	for key, expectedValue := range expected {
		actualValue, exists := actual[key]
		if !exists {
			return fmt.Errorf("missing expected key: %q", key)
		}

		switch ev := expectedValue.(type) {
		case map[string]any:
			actualNested, ok := actualValue.(map[string]any)
			if !ok {
				return fmt.Errorf("expected object at key %s, but got %s", key, actualValue)
			}
			if err := matchRecursive(actualNested, ev); err != nil {
				return err
			}

		default:
			actualStr := fmt.Sprint(actualValue)
			patternStr := fmt.Sprint(expectedValue)

			re, err := regexp.Compile(patternStr)
			if err != nil {
				return fmt.Errorf("invalid regex for key %q: %v", key, err)
			}

			if !re.MatchString(actualStr) {
				return fmt.Errorf("regex mismatch at key %s: value %s does not match pattern %s", key, actualStr, patternStr)
			}
		}
	}
	return nil
}

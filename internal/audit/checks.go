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

package audit

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/columns"
	"github.com/nats-io/natscli/internal/archive"
)

// CheckFunc implements a check over gathered audit
type CheckFunc func(check Check, reader *archive.Reader, examples *ExamplesCollection) (Outcome, error)

// CheckConfiguration describes and holds the configuration for a check
type CheckConfiguration struct {
	Key         string   `json:"key"`
	Check       string   `json:"check"`
	Description string   `json:"description"`
	Default     float64  `json:"default"`
	SetValue    *float64 `json:"set_value,omitempty"`
}

// Value retrieves the set value or default value
func (c *CheckConfiguration) Value() float64 {
	if c.SetValue != nil {
		return *c.SetValue
	}

	return c.Default
}

func (c *CheckConfiguration) String() string {
	return columns.F(c.Value())
}

// Set supports fisk
func (c *CheckConfiguration) Set(v string) error {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return err
	}
	c.SetValue = &f

	return nil
}

// SetVal supports fisk
func (c *CheckConfiguration) SetVal(s fisk.Settings) {
	s.SetValue(c)
}

// Check is the basic unit of analysis that is run against a data archive
type Check struct {
	Code          string                         `json:"code"`
	Name          string                         `json:"name"`
	Description   string                         `json:"description"`
	Configuration map[string]*CheckConfiguration `json:"configuration"`
	Handler       CheckFunc                      `json:"-"`
}

var registeredChecks = map[string]Check{}
var checksConfiguration = map[string]*CheckConfiguration{}
var registeredChecksMu sync.Mutex

// MustRegisterCheck allows a new check to be registered as a plugin, panics on error
func MustRegisterCheck(checks ...Check) {
	registeredChecksMu.Lock()
	defer registeredChecksMu.Unlock()

	for _, check := range checks {
		if check.Code == "" {
			panic("check code is required")
		}
		if check.Name == "" {
			panic("check name is required")
		}
		if check.Description == "" {
			panic("check description is required")
		}
		if check.Handler == nil {
			panic("check implementation is required")
		}

		if _, ok := registeredChecks[check.Name]; ok {
			panic(fmt.Sprintf("check %q already registered", check.Name))
		}

		for _, cfg := range check.Configuration {
			if cfg.Key == "" {
				panic("configuration key is required")
			}
			if cfg.Description == "" {
				panic("configuration description is required")
			}

			cfg.Check = check.Code
			checksConfiguration[configItemKey(check.Code, cfg.Key)] = cfg
		}

		registeredChecks[check.Name] = check
	}
}

func configItemKey(code string, key string) string {
	return fmt.Sprintf("%s_%s", strings.ToLower(code), key)
}

// Outcome of running a check against the data gathered into an archive
type Outcome int

const (
	// Pass is for no issues detected
	Pass Outcome = iota
	// PassWithIssues is for non-critical problems
	PassWithIssues Outcome = iota
	// Fail indicates a bad state is detected
	Fail Outcome = iota
	// Skipped is for checks that failed to run (no data, runtime error, ...)
	Skipped Outcome = iota
)

// Outcomes is the list of possible outcomes values
var Outcomes = [...]Outcome{
	Pass,
	PassWithIssues,
	Fail,
	Skipped,
}

// String converts an outcome into a 4-letter string value
func (o Outcome) String() string {
	switch o {
	case Fail:
		return "FAIL"
	case Pass:
		return "PASS"
	case PassWithIssues:
		return "WARN"
	case Skipped:
		return "SKIP"
	default:
		panic(fmt.Sprintf("Uknown outcome code: %d", o))
	}
}

// GetDefaultChecks creates the default list of check using default parameters
func GetDefaultChecks() []Check {
	var res []Check

	registeredChecksMu.Lock()
	defer registeredChecksMu.Unlock()

	for _, check := range registeredChecks {
		res = append(res, check)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Code < res[j].Code
	})

	return res
}

// GetConfigurationItems loads a list of config items sorted by check
//
// Use in fisk applications like:
//
//	 cfg := audit.GetConfigurationItems()
//	 for _, v := range cfg {
//		v.SetVal(analyze.Flag(fmt.Sprintf("%s_%s", strings.ToLower(v.Check), v.Key), v.Description).Default(fmt.Sprintf("%.2f", v.Default)))
//	 }
func GetConfigurationItems() []CheckConfiguration {
	var res []CheckConfiguration

	registeredChecksMu.Lock()
	defer registeredChecksMu.Unlock()

	for _, check := range checksConfiguration {
		res = append(res, *check)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})

	return res
}

// RunCheck is a wrapper to run a check, handling setup and errors
func RunCheck(check Check, ar *archive.Reader, limit uint) (Outcome, *ExamplesCollection) {
	examples := newExamplesCollection(limit)
	outcome, err := check.Handler(check, ar, examples)
	if err != nil {
		// If a check throws an error, mark it as skipped
		fmt.Printf("Check %s failed: %s\n", check.Name, err)
		return Skipped, examples
	}
	return outcome, examples
}

// CheckResult is a outcome of a single check
type CheckResult struct {
	Check         Check               `json:"check"`
	Outcome       Outcome             `json:"outcome"`
	OutcomeString string              `json:"outcome_string"`
	Examples      *ExamplesCollection `json:"examples"`
}

// Analyzes represents the result of an entire analysis
type Analyzes struct {
	Type     string         `json:"type"`
	Time     time.Time      `json:"time"`
	Skipped  []string       `json:"skipped"`
	Results  []CheckResult  `json:"checks"`
	Outcomes map[string]int `json:"outcomes"`
}

// RunChecks runs all the checks
func RunChecks(checks []Check, ar *archive.Reader, limit uint, skip []string, progress func(res CheckResult)) *Analyzes {
	result := &Analyzes{
		Type:     "io.nats.audit.v1.analysis",
		Time:     time.Now().UTC(),
		Skipped:  skip,
		Results:  []CheckResult{},
		Outcomes: make(map[string]int),
	}

	for _, outcome := range Outcomes {
		result.Outcomes[outcome.String()] = 0
	}

	for _, check := range checks {
		should := !slices.ContainsFunc(skip, func(s string) bool {
			return strings.EqualFold(check.Code, s)
		})

		var res CheckResult
		if should {
			outcome, examples := RunCheck(check, ar, limit)
			res = CheckResult{
				Check:   check,
				Outcome: outcome,
			}

			if examples != nil && len(examples.Examples) > 0 {
				res.Examples = examples
			}
		} else {
			res = CheckResult{
				Check:   check,
				Outcome: Skipped,
			}
		}

		res.OutcomeString = res.Outcome.String()

		progress(res)
		result.Results = append(result.Results, res)
		result.Outcomes[res.Outcome.String()]++
	}

	return result
}

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

package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
	"github.com/nats-io/natscli/internal/archive"
	"github.com/nats-io/natscli/internal/audit"
)

type auditAnalyzeCmd struct {
	archivePath   string
	examplesLimit uint
	verbose       bool
	quiet         bool
	checks        []audit.Check
}

func configureAuditAnalyzeCommand(app *fisk.CmdClause) {
	c := &auditAnalyzeCmd{
		checks: audit.GetDefaultChecks(),
	}

	analyze := app.Command("analyze", "perform checks against an archive created by the 'gather' subcommand").Action(c.analyze)
	analyze.Arg("archive", "path to input archive to analyze").Required().ExistingFileVar(&c.archivePath)
	analyze.Flag("max-examples", "How many example issues to display for each failed check (0 for unlimited)").Default("5").UintVar(&c.examplesLimit)
	analyze.Flag("quiet", "Disable info and warning messages during analysis").BoolVar(&c.quiet)

	// Hidden flags
	analyze.Flag("verbose", "Enable debug console messages during analysis").Hidden().BoolVar(&c.verbose)
}

func (cmd *auditAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	// Adjust log levels
	if cmd.quiet {
		audit.LogQuiet()
	} else if cmd.verbose {
		audit.LogVerbose()
	}

	// Open archive
	ar, err := archive.NewReader(cmd.archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer func() {
		err := ar.Close()
		if err != nil {
			fmt.Printf("Failed to close archive reader: %s\n", err)
		}
	}()

	// Table that groups checks based on their outcome
	summaryTable := make(map[audit.Outcome][]audit.Check)
	for _, outcome := range audit.Outcomes {
		summaryTable[outcome] = make([]audit.Check, 0)
	}

	// Run all checks
	fmt.Printf("Running %d checks against archive: %s\n", len(cmd.checks), cmd.archivePath)
	for _, check := range cmd.checks {
		outcome, examples := audit.RunCheck(check, ar, cmd.examplesLimit)
		fmt.Printf("[%s] %s (%s)\n%s\n", outcome, check.Name, check.Description, examples)

		summaryTable[outcome] = append(summaryTable[outcome], check)
	}

	// Print summary of checks
	fmt.Printf("\nSummary of checks:\n")
	for outcome, checks := range summaryTable {
		fmt.Printf("%s: %d\n", outcome, len(checks))
	}

	return nil
}

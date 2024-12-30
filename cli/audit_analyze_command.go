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
	iu "github.com/nats-io/natscli/internal/util"
)

type auditAnalyzeCmd struct {
	archivePath   string
	examplesLimit uint
	verbose       bool
	quiet         bool
	checks        []audit.Check
	block         []string
	json          bool
}

func configureAuditAnalyzeCommand(app *fisk.CmdClause) {
	c := &auditAnalyzeCmd{
		checks: audit.GetDefaultChecks(),
	}

	analyze := app.Command("analyze", "perform checks against an archive created by the 'gather' subcommand").Action(c.analyze)
	analyze.Arg("archive", "path to input archive to analyze").Required().ExistingFileVar(&c.archivePath)
	analyze.Flag("max-examples", "How many example issues to display for each failed check (0 for unlimited)").Default("5").UintVar(&c.examplesLimit)
	analyze.Flag("quiet", "Disable info and warning messages during analysis").BoolVar(&c.quiet)
	analyze.Flag("skip", "Prevents checks from running by check code").PlaceHolder("CODE").StringsVar(&c.block)
	analyze.Flag("json", "Output JSON format").Short('j').BoolVar(&c.json)

	// Hidden flags
	analyze.Flag("verbose", "Enable debug console messages during analysis").Hidden().BoolVar(&c.verbose)
}

func (cmd *auditAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	// Adjust log levels
	if cmd.quiet || cmd.json {
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

	// Run all checks
	if !cmd.json {
		fmt.Printf("Running %d checks against archive: %s\n", len(cmd.checks), cmd.archivePath)
	}

	analyzes := audit.RunChecks(cmd.checks, ar, cmd.examplesLimit, cmd.block, func(res audit.CheckResult) {
		if cmd.json {
			return
		}

		if res.Examples.Count() > 0 {
			fmt.Printf("[%s] [%s] %s\n%s\n", res.Outcome, res.Check.Code, res.Check.Description, res.Examples)
		} else {
			fmt.Printf("[%s] [%s] %s\n", res.Outcome, res.Check.Code, res.Check.Description)
		}
	})

	if cmd.json {
		return iu.PrintJSON(analyzes)
	}

	// Print summary of checks
	fmt.Printf("\nSummary of checks:\n")
	for outcome, checks := range analyzes.Outcomes {
		fmt.Printf("%s: %s\n", outcome, f(checks))
	}

	return nil
}

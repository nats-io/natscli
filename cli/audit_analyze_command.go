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
	"encoding/json"
	"fmt"
	"os"

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
	writePath     string
	loadPath      string
	force         bool
}

func configureAuditAnalyzeCommand(app *fisk.CmdClause) {
	c := &auditAnalyzeCmd{
		checks: audit.GetDefaultChecks(),
	}

	analyze := app.Command("analyze", "perform checks against an archive created by the 'gather' subcommand").Action(c.analyze)
	analyze.Arg("archive", "path to input archive to analyze").ExistingFileVar(&c.archivePath)
	analyze.Flag("max-examples", "How many example issues to display for each failed check (0 for unlimited)").Default("5").UintVar(&c.examplesLimit)
	analyze.Flag("quiet", "Disable info and warning messages during analysis").UnNegatableBoolVar(&c.quiet)
	analyze.Flag("skip", "Prevents checks from running by check code").PlaceHolder("CODE").StringsVar(&c.block)
	analyze.Flag("load", "Loads a saved report").PlaceHolder("FILE").StringVar(&c.loadPath)
	analyze.Flag("save", "Stores the analyze result to a file").PlaceHolder("FILE").StringVar(&c.writePath)
	analyze.Flag("force", "Force overwriting existing report files").Short('f').UnNegatableBoolVar(&c.force)
	analyze.Flag("json", "Output JSON format").Short('j').BoolVar(&c.json)

	// Hidden flags
	analyze.Flag("verbose", "Enable debug console messages during analysis").Hidden().BoolVar(&c.verbose)
}

func (c *auditAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	if c.loadPath != "" {
		return c.loadAndRender()
	}

	// Adjust log levels
	if c.quiet || c.json {
		audit.LogQuiet()
	} else if c.verbose {
		audit.LogVerbose()
	}

	if c.writePath != "" && iu.FileExists(c.writePath) && !c.force {
		return fmt.Errorf("result file %q already exist, use --force to overwrite", c.writePath)
	}

	if c.archivePath == "" {
		return fmt.Errorf("archive path is required")
	}

	// Open archive
	ar, err := archive.NewReader(c.archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer func() {
		err := ar.Close()
		if err != nil {
			fmt.Printf("Failed to close archive reader: %s\n", err)
		}
	}()

	if !c.json {
		fmt.Printf("Running %d checks against archive: %s\n", len(c.checks), c.archivePath)
	}
	analyzes := audit.RunChecks(c.checks, ar, c.examplesLimit, c.block)

	err = c.renderReport(analyzes)
	if err != nil {
		return err
	}

	if c.writePath != "" {
		j, err := json.MarshalIndent(analyzes, "", "  ")
		if err != nil {
			return err
		}

		return os.WriteFile(c.writePath, j, 0644)
	}

	return nil
}

func (c *auditAnalyzeCmd) loadAndRender() error {
	ab, err := os.ReadFile(c.loadPath)
	if err != nil {
		return err
	}

	analyzes := audit.Analyzes{}
	err = json.Unmarshal(ab, &analyzes)
	if err != nil {
		return err
	}

	return c.renderReport(&analyzes)
}

func (c *auditAnalyzeCmd) renderReport(analyzes *audit.Analyzes) error {
	if c.json {
		return iu.PrintJSON(analyzes)
	}

	for _, res := range analyzes.Results {
		fmt.Printf("[%s] [%s] %s\n", res.Outcome, res.Check.Code, res.Check.Description)
		if res.Examples.Count() > 0 {
			res.Examples.Limit = c.examplesLimit
			fmt.Println(res.Examples.String())
		}
	}

	fmt.Printf("\nSummary of checks:\n")
	for outcome, checks := range analyzes.Outcomes {
		fmt.Printf("\t%s: %s\n", outcome, f(checks))
	}

	return nil
}

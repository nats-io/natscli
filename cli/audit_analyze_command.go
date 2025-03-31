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

package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/choria-io/fisk"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/audit"
	"github.com/nats-io/jsm.go/audit/archive"
	iu "github.com/nats-io/natscli/internal/util"
)

type auditAnalyzeCmd struct {
	archivePath   string
	examplesLimit uint
	collection    *audit.CheckCollection
	block         []string
	json          bool
	md            bool
	writePath     string
	loadPath      string
	force         bool
	verbose       bool
	isTerminal    bool
}

func configureAuditAnalyzeCommand(app *fisk.CmdClause) {
	c := &auditAnalyzeCmd{
		isTerminal: iu.IsTerminal(),
	}

	analyze := app.Command("analyze", "perform checks against an archive created by the 'gather' subcommand").Action(c.analyze)
	analyze.Arg("archive", "path to input archive to analyze").ExistingFileVar(&c.archivePath)
	analyze.Flag("max-examples", "How many example issues to display for each failed check (0 for unlimited)").Default("5").UintVar(&c.examplesLimit)
	analyze.Flag("skip", "Prevents checks from running by check code").PlaceHolder("CODE").StringsVar(&c.block)
	analyze.Flag("load", "Loads a saved report").PlaceHolder("FILE").StringVar(&c.loadPath)
	analyze.Flag("save", "Stores the analyze result to a file").PlaceHolder("FILE").StringVar(&c.writePath)
	analyze.Flag("force", "Force overwriting existing report files").Short('f').UnNegatableBoolVar(&c.force)
	analyze.Flag("json", "Output JSON format").Short('j').UnNegatableBoolVar(&c.json)
	analyze.Flag("markdown", "Output Markdown format").UnNegatableBoolVar(&c.md)
	analyze.Flag("verbose", "Log verbosely").UnNegatableBoolVar(&c.verbose)
}

func (c *auditAnalyzeCmd) analyze(_ *fisk.ParseContext) error {
	var log api.Logger
	var err error

	switch {
	case opts().Trace:
		log = api.NewDefaultLogger(api.TraceLevel)
	case !c.verbose:
		log = api.NewDiscardLogger()
	default:
		log = api.NewDefaultLogger(api.InfoLevel)
	}

	c.collection, err = audit.NewDefaultCheckCollection()
	if err != nil {
		return err
	}

	if c.loadPath != "" {
		return c.loadAndRender()
	}

	if c.writePath != "" && iu.FileExists(c.writePath) && !c.force {
		return fmt.Errorf("result file %q already exist, use --force to overwrite", c.writePath)
	}

	if c.archivePath == "" {
		return fmt.Errorf("archive path is required")
	}

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

	c.collection.SkipChecks(c.block...)

	report := c.collection.Run(ar, c.examplesLimit, log)
	err = c.renderReport(report)
	if err != nil {
		return err
	}

	if c.writePath != "" {
		j, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}

		return os.WriteFile(c.writePath, j, 0644)
	}

	return nil
}

func (c *auditAnalyzeCmd) loadAndRender() error {
	report, err := audit.LoadAnalysis(c.loadPath)
	if err != nil {
		return err
	}

	return c.renderReport(report)
}

func (c *auditAnalyzeCmd) outcomeWithColor(o audit.Outcome) string {
	if !c.isTerminal {
		return o.String()
	}

	switch o {
	case audit.Pass:
		return color.GreenString(o.String())
	case audit.PassWithIssues:
		return color.YellowString(o.String())
	case audit.Skipped:
		return o.String()
	case audit.Fail:
		return color.RedString(o.String())
	default:
		return o.String()
	}
}

func (c *auditAnalyzeCmd) renderMarkdown(report *audit.Analysis) error {
	out, err := report.ToMarkdown(audit.MarkdownFormatTemplate, c.examplesLimit)
	if err != nil {
		return err
	}

	fmt.Println(string(out))

	return nil
}

func (c *auditAnalyzeCmd) renderReport(report *audit.Analysis) error {
	switch {
	case c.json:
		return c.renderJSON(report)
	case c.md:
		return c.renderMarkdown(report)
	default:
		return c.renderConsole(report)
	}
}

func (c *auditAnalyzeCmd) renderJSON(report *audit.Analysis) error {
	j, err := report.ToJSON()
	if err != nil {
		return err
	}
	fmt.Println(string(j))

	return nil
}

func (c *auditAnalyzeCmd) renderConsole(report *audit.Analysis) error {
	if c.archivePath != "" {
		fmt.Printf("NATS Audit Report %q captured at %s\n\n", c.archivePath, f(report.Metadata.Timestamp))
	} else {
		fmt.Printf("NATS Audit Report %q captured at %s\n\n", c.loadPath, f(report.Metadata.Timestamp))
	}

	for _, res := range report.Results {
		fmt.Printf("[%s] [%s] %s\n", c.outcomeWithColor(res.Outcome), res.Check.Code, res.Check.Description)
		if res.Examples.Error != "" {
			fmt.Printf("%s: %s\n", color.RedString("Error"), res.Examples.Error)
		}
		if res.Examples.Count() > 0 {
			res.Examples.Limit = c.examplesLimit
			fmt.Println(res.Examples.String())
		}
	}

	fmt.Println()

	cols := newColumns("Report Summary")
	cols.AddSectionTitle("Archive Connection Information")
	cols.AddRow("Connection Server", report.Metadata.ConnectURL)
	cols.AddRow("Server Version", report.Metadata.ConnectedServerVersion)
	cols.AddRow("User", report.Metadata.UserName)
	cols.AddSectionTitle("Summary of Checks")
	cols.AddMapInts(report.Outcomes, true, false)
	cols.Frender(os.Stdout)

	return nil
}

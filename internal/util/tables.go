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
	"fmt"
	"os"
	"sort"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/nats-io/natscli/options"
	terminal "golang.org/x/term"
)

type Table struct {
	writer table.Writer
}

var styles = map[string]table.Style{
	"":        table.StyleRounded,
	"rounded": table.StyleRounded,
	"double":  table.StyleDouble,
	"yellow":  coloredBorderStyle(text.FgYellow),
	"blue":    coloredBorderStyle(text.FgBlue),
	"cyan":    coloredBorderStyle(text.FgCyan),
	"green":   coloredBorderStyle(text.FgGreen),
	"magenta": coloredBorderStyle(text.FgMagenta),
	"red":     coloredBorderStyle(text.FgRed),
}

func coloredBorderStyle(c text.Color) table.Style {
	s := table.StyleRounded
	s.Color.Border = text.Colors{c}
	s.Color.Separator = text.Colors{c}
	s.Format.Footer = text.FormatDefault

	return s
}

// ValidStyles are valid color styles this package supports
func ValidStyles() []string {
	var res []string

	for k := range styles {
		if k == "" {
			continue
		}

		res = append(res, k)
	}

	sort.Strings(res)

	return res
}

func (t *Table) AddHeaders(items ...any) {
	t.writer.AppendHeader(items)
}

func (t *Table) AddFooter(items ...any) {
	t.writer.AppendFooter(items)
}

func (t *Table) AddSeparator() {
	t.writer.AppendSeparator()
}

func (t *Table) AddRow(items ...any) {
	t.writer.AppendRow(items)
}

func (t *Table) Render() string {
	return fmt.Sprintln(t.writer.Render())
}

func (t *Table) RenderCSV() string {
	return fmt.Sprintln(t.writer.RenderCSV())
}

func NewTableWriter(opts *options.Options, format string, a ...any) *Table {
	tbl := &Table{
		writer: table.NewWriter(),
	}

	tbl.writer.SuppressTrailingSpaces()
	tbl.writer.SetStyle(styles["rounded"])

	if terminal.IsTerminal(int(os.Stdout.Fd())) {
		if opts.Config != nil {
			style, ok := styles[opts.Config.ColorScheme()]
			if ok {
				tbl.writer.SetStyle(style)
			}
		}
		w, _, _ := terminal.GetSize(int(os.Stdout.Fd()))
		if w > 0 {
			tbl.writer.Style().Size.WidthMax = w
		}
	}

	tbl.writer.Style().Title.Align = text.AlignCenter
	tbl.writer.Style().Format.Header = text.FormatDefault
	tbl.writer.Style().Format.Footer = text.FormatDefault

	if format != "" {
		tbl.writer.SetTitle(fmt.Sprintf(format, a...))
	}

	return tbl
}

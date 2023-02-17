package cli

import (
	"fmt"
	"sort"

	"github.com/jedib0t/go-pretty/v6/table"
)

type tbl struct {
	writer table.Writer
}

var styles = map[string]table.Style{
	"":              table.StyleRounded,
	"rounded":       table.StyleRounded,
	"double":        table.StyleDouble,
	"dark":          table.StyleColoredDark,
	"bright":        table.StyleColoredBright,
	"bold":          table.StyleBold,
	"light":         table.StyleLight,
	"yellow_light":  table.StyleColoredBlackOnYellowWhite,
	"yellow_dark":   table.StyleColoredYellowWhiteOnBlack,
	"blue_light":    table.StyleColoredBlackOnBlueWhite,
	"blue_dark":     table.StyleColoredBlueWhiteOnBlack,
	"cyan_light":    table.StyleColoredBlackOnCyanWhite,
	"cyan_dark":     table.StyleColoredCyanWhiteOnBlack,
	"green_light":   table.StyleColoredBlackOnGreenWhite,
	"green_dark":    table.StyleColoredGreenWhiteOnBlack,
	"magenta_light": table.StyleColoredBlackOnMagentaWhite,
	"magenta_dark":  table.StyleColoredMagentaWhiteOnBlack,
	"red_light":     table.StyleColoredBlackOnRedWhite,
	"red_dark":      table.StyleColoredRedWhiteOnBlack,
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

func (t *tbl) AddHeaders(items ...any) {
	t.writer.AppendHeader(items)
}

func (t *tbl) AddFooter(items ...any) {
	t.writer.AppendFooter(items)
}

func (t *tbl) AddSeparator() {
	t.writer.AppendSeparator()
}

func (t *tbl) AddRow(items ...any) {
	t.writer.AppendRow(items)
}

func (t *tbl) Render() string {
	return fmt.Sprintln(t.writer.Render())
}

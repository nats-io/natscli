package cli

import (
	"github.com/dustin/go-humanize"
	"github.com/mprimi/natscli/columns"
)

func newColumns(heading string, a ...any) *columns.Writer {
	w := columns.New(heading, a...)
	w.SetColorScheme(opts.Config.ColorScheme())
	w.SetHeading(heading, a...)

	return w
}

func fiBytes(v uint64) string {
	return humanize.IBytes(v)
}

func f(v any) string {
	return columns.F(v)
}

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/mattn/go-isatty"
)

type columnRow struct {
	kind   int
	values []any
}

type columnWriter struct {
	heading string
	rows    []*columnRow
	sep     string
	indent  string
}

var colsStyles = map[string]text.Color{
	"yellow":  text.FgYellow,
	"blue":    text.FgBlue,
	"cyan":    text.FgCyan,
	"green":   text.FgGreen,
	"magenta": text.FgMagenta,
	"red":     text.FgRed,
}

func newColumns(heading string, a ...any) *columnWriter {
	w := &columnWriter{sep: ":"}
	w.SetHeading(heading, a...)

	return w
}

const (
	kindRow    = 1
	kindIndent = 2
	kindLine   = 3
	kindTitle  = 4
)

// Frender renders to the writer 0
func (w *columnWriter) Frender(o io.Writer) error {
	// figure out the right most edge of first column
	longest := 0
	for _, row := range w.rows {
		if row.kind == 1 && len(row.values) > 0 && len(row.values[0].(string)) > longest {
			longest = len(row.values[0].(string))
		}
	}

	if w.isTerminal(o) {
		color, ok := colsStyles[opts.Config.ColorScheme()]
		if ok {
			w.sep = color.Sprint(":")
		}
	}

	if w.heading != "" {
		fmt.Fprintln(o, w.heading)
		fmt.Fprintln(o)
	}

	prev := -1
	for i, row := range w.rows {
		switch row.kind {
		case kindIndent:
			w.indent = row.values[0].(string)

		case kindTitle:
			if i != 0 && prev != kindTitle && prev != kindLine {
				fmt.Fprintln(o)
			}
			fmt.Fprintln(o, w.indent+w.maybeAddColon(o, row.values[0].(string), false))
			fmt.Fprintln(o)
			prev = row.kind

		case kindRow:
			left := row.values[0].(string)
			padding := longest - len(left) + 2
			if padding < 0 {
				padding = 0
			}

			if left == "" {
				// when left is empty we assume it's a multi line continuation so no : in-front
				fmt.Fprintf(o, "%s%s %v\n", w.indent, strings.Repeat(" ", padding+1), row.values[1])
			} else {
				fmt.Fprintf(o, "%s%s%s%s %v\n", w.indent, strings.Repeat(" ", padding), left, w.sep, row.values[1])
			}
			prev = row.kind

		case kindLine:
			// avoid 2 blank lines
			if prev != kindTitle {
				fmt.Fprintln(o, append([]any{w.indent}, row.values...)...)
				prev = row.kind
			}
		}
	}

	return nil
}

// Render produce the result as a string
func (w *columnWriter) Render() (string, error) {
	buf := bytes.NewBuffer([]byte{})
	err := w.Frender(buf)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// AddRow adds a row, v will be formatted if time.Time, time.Duration, []string, floats, ints and uints
func (w *columnWriter) AddRow(t string, v any) {
	w.rows = append(w.rows, &columnRow{kind: kindRow, values: []any{t, f(v)}})
}

// AddRowIf adds a row if the condition is true
func (w *columnWriter) AddRowIf(t string, v any, condition bool) {
	if !condition {
		return
	}
	w.AddRow(t, v)
}

// AddRowIfNotEmpty adds a row if v is not an empty string
func (w *columnWriter) AddRowIfNotEmpty(t string, v string) {
	if v == "" {
		return
	}
	w.AddRow(t, v)
}

// AddRowf adds a row with printf like behavior on the value, no auto formatting of values will be done like in AddRow()
func (w *columnWriter) AddRowf(t string, format string, a ...any) {
	w.AddRow(t, fmt.Sprintf(format, a...))
}

// AddSectionTitle adds a new section
func (w *columnWriter) AddSectionTitle(format string, a ...any) {
	w.rows = append(w.rows, &columnRow{kind: kindTitle, values: []any{fmt.Sprintf(format, a...)}})
}

// SetHeading sets the initial heading
func (w *columnWriter) SetHeading(format string, a ...any) {
	w.heading = fmt.Sprintf(format, a...)
}

// Println Adds a line to the report rendered outside of columnar layout
func (w *columnWriter) Println(msg ...string) {
	var val []any
	for _, v := range msg {
		val = append(val, v)
	}
	w.rows = append(w.rows, &columnRow{kind: kindLine, values: val})
}

// AddMapStringsAsValue adds a row with title t and the data as value, over multiple lines and correctly justified
func (w *columnWriter) AddMapStringsAsValue(t string, data map[string]string) {
	maxLen := progressWidth()

	var list []string
	for k := range data {
		list = append(list, k)
	}
	sort.Strings(list)

	for i, k := range list {
		v := data[k]

		if len(data[k]) > maxLen && maxLen > 20 {
			w := maxLen/2 - 10
			v = fmt.Sprintf("%v ... %v", v[0:w], v[len(v)-w:])
		}

		if i == 0 {
			w.AddRowf(t, "%s: %s", k, v)
		} else {
			w.AddRowf("", "%s: %s", k, v)
		}
	}
}

// AddMapStrings adds data with each key being a column title and value what follows the :
func (w *columnWriter) AddMapStrings(data map[string]string) {
	maxLen := progressWidth()

	var list []string
	for k := range data {
		list = append(list, k)
	}
	sort.Strings(list)

	for _, k := range list {
		v := data[k]

		if len(data[k]) > maxLen && maxLen > 20 {
			w := maxLen/2 - 10
			v = fmt.Sprintf("%v ... %v", v[0:w], v[len(v)-w:])
		}

		w.AddRow(k, v)
	}
}

// Indent results in all following text to be indented this many spaces. When called it sets that value, zero resets to no indent
func (w *columnWriter) Indent(width int) {
	w.rows = append(w.rows, &columnRow{kind: kindIndent, values: []any{strings.Repeat(" ", width)}})
}

func (w *columnWriter) isTerminal(o io.Writer) bool {
	fh, ok := any(o).(*os.File)
	if !ok {
		return false
	}

	return isatty.IsTerminal(fh.Fd())
}

func (w *columnWriter) maybeAddColon(o io.Writer, v string, colorize bool) string {
	if strings.HasSuffix(v, ":") {
		return v
	}

	c := ":"

	if colorize && w.isTerminal(o) {
		color, ok := colsStyles[opts.Config.ColorScheme()]
		if ok {
			c = color.Sprint(":")
		}
	}

	return v + c
}

func f(v any) string {
	switch x := v.(type) {
	case []string:
		return strings.Join(x, ", ")
	case time.Duration:
		return humanizeDuration(x)
	case time.Time:
		return x.Local().Format("2006-01-02 15:04:05")
	case bool:
		return fmt.Sprintf("%t", x)
	case uint:
		return humanize.Comma(int64(x))
	case uint32:
		return humanize.Comma(int64(x))
	case uint16:
		return humanize.Comma(int64(x))
	case uint64:
		return humanize.Comma(int64(x))
	case int:
		return humanize.Comma(int64(x))
	case int32:
		return humanize.Comma(int64(x))
	case int64:
		return humanize.Comma(x)
	case float32:
		return humanize.CommafWithDigits(float64(x), 3)
	case float64:
		return humanize.CommafWithDigits(x, 3)
	default:
		return fmt.Sprintf("%v", x)
	}
}

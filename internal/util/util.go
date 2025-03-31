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

package util

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/google/shlex"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/internal/asciigraph"
	"github.com/nats-io/natscli/options"
	"github.com/nats-io/nkeys"
	terminal "golang.org/x/term"
)

// RemoveReservedMetadata returns a new version of metadata with reserved keys starting with _nats. removed
func RemoveReservedMetadata(metadata map[string]string) map[string]string {
	res := make(map[string]string)

	for k, v := range metadata {
		if strings.HasPrefix(k, "_nats.") {
			continue
		}
		res[k] = v
	}

	if len(res) == 0 {
		return nil
	}

	return res
}

// RequireAPILevel asserts if the meta leader supports api level lvl
func RequireAPILevel(m *jsm.Manager, lvl int, format string, a ...any) error {
	mlvl, err := m.MetaApiLevel(false)
	if err != nil {
		return err
	}

	if mlvl >= lvl {
		return nil
	}

	return fmt.Errorf(format, a...)
}

var semVerRe = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)

func versionComponents(version string) (major, minor, patch int, err error) {
	m := semVerRe.FindStringSubmatch(version)
	if m == nil {
		return 0, 0, 0, errors.New("invalid semver")
	}
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return -1, -1, -1, err
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return -1, -1, -1, err
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return -1, -1, -1, err
	}
	return major, minor, patch, err
}

// ServerMinVersion checks if the connected server meets certain version constraints
func ServerMinVersion(nc *nats.Conn, major, minor, patch int) bool {
	smajor, sminor, spatch, _ := versionComponents(nc.ConnectedServerVersion())
	if smajor < major || (smajor == major && sminor < minor) || (smajor == major && sminor == minor && spatch < patch) {
		return false
	}

	return true
}

// ToJSON converts any to json string
func ToJSON(d any) (string, error) {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return "", err
	}

	return string(j), nil
}

// PrintJSON prints any to stdout as json
func PrintJSON(d any) error {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

// IsTerminal checks if stdin and stdout are both normal terminals
func IsTerminal() bool {
	return terminal.IsTerminal(int(os.Stdin.Fd())) && terminal.IsTerminal(int(os.Stdout.Fd()))
}

// WipeSlice overwrites the contents of slice with 'x'
func WipeSlice(buf []byte) {
	for i := range buf {
		buf[i] = 'x'
	}
}

// ReadKeyFile reads a nkey from a file
func ReadKeyFile(filename string) ([]byte, error) {
	var key []byte
	contents, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	defer WipeSlice(contents)

	lines := bytes.Split(contents, []byte("\n"))
	for _, line := range lines {
		if nkeys.IsValidEncoding(line) {
			key = make([]byte, len(line))
			copy(key, line)
			return key, nil
		}
	}

	return nil, fmt.Errorf("could not find a valid key in %s", filename)
}

// SliceGroups calls fn repeatedly with a subset of input up to size big
func SliceGroups(input []string, size int, fn func(group []string)) {
	// how many to add
	padding := size - (len(input) % size)

	if padding != size {
		p := []string{}

		for i := 0; i <= padding; i++ {
			p = append(p, "")
		}

		input = append(input, p...)
	}

	// how many chunks we're making
	count := len(input) / size

	for i := 0; i < count; i++ {
		chunk := []string{}
		for s := 0; s < size; s++ {
			chunk = append(chunk, input[i+s*count])
		}
		fn(chunk)
	}
}

// StructWithoutOmitEmpty given a non pointer instance of a type with a lot of omitempty json tags will return a new instance without those
//
// does not handle nested values
func StructWithoutOmitEmpty(s any) any {
	st := reflect.TypeOf(s)

	// It's a pointer struct, convert to the value that it points to.
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}

	var fs []reflect.StructField
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		field.Tag = reflect.StructTag(strings.ReplaceAll(string(field.Tag), ",omitempty", ""))
		fs = append(fs, field)
	}

	st2 := reflect.StructOf(fs)
	v := reflect.ValueOf(s)

	j, err := json.Marshal(v.Convert(st2).Interface())
	if err != nil {
		panic(err)
	}

	var res any
	err = json.Unmarshal(j, &res)
	if err != nil {
		panic(err)
	}

	return res
}

// FileExists checks if a file exist regardless of file kind
func FileExists(f string) bool {
	_, err := os.Stat(f)
	return !os.IsNotExist(err)
}

// IsFileAccessible checks if f is a file and accessible, errors for non file arguments
func IsFileAccessible(f string) (bool, error) {
	stat, err := os.Stat(f)
	if err != nil {
		return false, err
	}

	if stat.IsDir() {
		return false, fmt.Errorf("is a directory")
	}

	file, err := os.Open(f)
	if err != nil {
		return false, err
	}
	file.Close()

	return true, nil
}

// IsDirectory returns true when path is a directory
func IsDirectory(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}

	return s.IsDir()
}

// AskOne asks a single question using Prompt
func AskOne(p survey.Prompt, response any, opts ...survey.AskOpt) error {
	if !IsTerminal() {
		return fmt.Errorf("cannot prompt for user input without a terminal")
	}

	return survey.AskOne(p, response, append(SurveyColors(), opts...)...)
}

// SelectPageSize is the size of selection lists influence by screen size
func SelectPageSize(count int) int {
	_, h, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		h = 40
	}

	ps := count
	if ps > h-4 {
		ps = h - 4
	}

	return ps
}

// SurveyColors determines colors for the survey package based on context and options set
func SurveyColors() []survey.AskOpt {
	return []survey.AskOpt{
		survey.WithIcons(func(icons *survey.IconSet) {
			if options.DefaultOptions.Config == nil {
				icons.Question.Format = "white"
				icons.SelectFocus.Format = "white"
				return
			}

			switch options.DefaultOptions.Config.ColorScheme() {
			case "yellow":
				icons.Question.Format = "yellow+hb"
				icons.SelectFocus.Format = "yellow+hb"
			case "blue":
				icons.Question.Format = "blue+hb"
				icons.SelectFocus.Format = "blue+hb"
			case "green":
				icons.Question.Format = "green+hb"
				icons.SelectFocus.Format = "green+hb"
			case "cyan":
				icons.Question.Format = "cyan+hb"
				icons.SelectFocus.Format = "cyan+hb"
			case "magenta":
				icons.Question.Format = "magenta+hb"
				icons.SelectFocus.Format = "magenta+hb"
			case "red":
				icons.Question.Format = "red+hb"
				icons.SelectFocus.Format = "red+hb"
			default:
				icons.Question.Format = "white"
				icons.SelectFocus.Format = "white"
			}

			if options.DefaultOptions.Config != nil && options.DefaultOptions.Config.Name != "" {
				icons.Question.Text = fmt.Sprintf("[%s] ?", options.DefaultOptions.Config.Name)
				icons.Help.Text = ""
			}
		}),
	}
}

func SortMultiSort[V cmp.Ordered, S string | cmp.Ordered](i1 V, j1 V, i2 S, j2 S) bool {
	if i1 == j1 {
		return i2 < j2
	}

	return i1 > j1
}

// MapKeys extracts the keys from a map
func MapKeys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}

	return r
}

// ClearScreen tries to ensure resetting original state of screen
func ClearScreen() {
	asciigraph.Clear()
}

// BarGraph generates a bar group based on data, copied from choria-io/appbuilder
func BarGraph(w io.Writer, data map[string]float64, caption string, width int, bytes bool) error {
	longest := 0
	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	keys := []string{}
	for k, v := range data {
		keys = append(keys, k)
		if len(k) > longest {
			longest = len(k)
		}

		if v < minVal {
			minVal = v
		}

		if v > maxVal {
			maxVal = v
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return data[keys[i]] < data[keys[j]]
	})

	if caption != "" {
		fmt.Fprintln(w, caption)
		fmt.Fprintln(w)
	}

	var steps float64
	if maxVal == minVal {
		steps = maxVal / float64(width)
	} else {
		steps = (maxVal - minVal) / float64(width)
	}

	longestLine := 0
	for _, k := range keys {
		v := data[k]

		var blocks int
		switch {
		case v == 0:
			// value 0 is always 0
			blocks = 0
		case len(keys) == 1:
			// one entry, so we show full width
			blocks = width
		case minVal == maxVal:
			// all entries have same value, so we show full width
			blocks = width
		default:
			blocks = int((v - minVal) / steps)
		}

		var h string
		if bytes {
			h = humanize.IBytes(uint64(v))
		} else {
			h = humanize.Commaf(v)
		}

		bar := strings.Repeat("█", blocks)
		if blocks == 0 {
			bar = "▏"
		}

		line := fmt.Sprintf("%s%s: %s (%s)", strings.Repeat(" ", longest-len(k)+2), k, bar, h)
		if len(line) > longestLine {
			longestLine = len(line)
		}

		fmt.Fprintln(w, line)
	}

	return nil
}

// ProgressWidth calculates progress bar width for uiprogress:
//
// if it cant figure out the width, assume 80
// if the width is too small, set it to minWidth and just live with the overflow
//
// this ensures a reasonable progress size, ideally we should switch over
// to a spinner for < minWidth rather than cause overflows, but thats for later.
func ProgressWidth() int {
	w, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80
	}

	minWidth := 10

	if w-30 <= minWidth {
		return minWidth
	} else {
		return w - 30
	}
}

// JSONString returns a quoted string to be used as a JSON object
func JSONString(s string) string {
	return "\"" + s + "\""
}

// SplitCommand Split the string into a command and its arguments.
func SplitCommand(s string) (string, []string, error) {
	cmdAndArgs, err := shlex.Split(s)
	if err != nil {
		return "", nil, err
	}

	cmd := cmdAndArgs[0]
	args := cmdAndArgs[1:]
	return cmd, args, nil
}

// EditFile edits the file at filepath f using the environment variable EDITOR command.
func EditFile(f string) error {
	rawEditor := os.Getenv("EDITOR")
	if rawEditor == "" {
		return fmt.Errorf("set EDITOR environment variable to your chosen editor")
	}

	editor, args, err := SplitCommand(rawEditor)
	if err != nil {
		return fmt.Errorf("could not parse EDITOR: %v", rawEditor)
	}

	args = append(args, f)
	cmd := exec.Command(editor, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("could not edit file %v: %s", f, err)
	}

	return nil
}

// SplitString splits a string by unicode space or comma
func SplitString(s string) []string {
	return strings.FieldsFunc(s, func(c rune) bool {
		if unicode.IsSpace(c) {
			return true
		}

		if c == ',' {
			return true
		}

		return false
	})
}

// SplitCLISubjects splits a subject list by comma, tab, space, unicode space etc
func SplitCLISubjects(subjects []string) []string {
	res := []string{}

	re := regexp.MustCompile(`,|\t|\s`)
	for _, s := range subjects {
		if re.MatchString(s) {
			res = append(res, SplitString(s)...)
		} else {
			res = append(res, s)
		}
	}

	return res
}

// IsJsonObjectString checks if a string is a JSON document starting and ending with {}
func IsJsonObjectString(s string) bool {
	trimmed := strings.TrimSpace(s)
	return strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")
}

// CompactStrings looks through a list of strings like hostnames and chops off the common parts like domains
func CompactStrings(source []string) []string {
	if len(source) == 0 {
		return source
	}

	hnParts := make([][]string, len(source))
	shortest := math.MaxInt32

	for i, name := range source {
		hnParts[i] = strings.Split(name, ".")
		if len(hnParts[i]) < shortest {
			shortest = len(hnParts[i])
		}
	}

	toRemove := ""

	// we dont chop the 0 item off
	for i := shortest - 1; i > 0; i-- {
		s := hnParts[0][i]

		remove := true
		for _, name := range hnParts {
			if name[i] != s {
				remove = false
				break
			}
		}

		if remove {
			toRemove = "." + s + toRemove
		} else {
			break
		}
	}

	result := make([]string, len(source))
	for i, name := range source {
		result[i] = strings.TrimSuffix(name, toRemove)
	}

	return result
}

// IsPrintable checks if a string is made of only printable characters
func IsPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// Base64IfNotPrintable returns the bytes are all printable else a b64 encoded version
func Base64IfNotPrintable(val []byte) string {
	if IsPrintable(string(val)) {
		return string(val)
	}

	return base64.StdEncoding.EncodeToString(val)
}

var bytesUnitSplitter = regexp.MustCompile(`^(\d+)(\w+)`)
var errInvalidByteString = errors.New("bytes must end in K, KB, M, MB, G, GB, T or TB")

// ParseStringAsBytes nats-server derived string parse, empty string and any negative is -1,others are parsed as 1024 based bytes
func ParseStringAsBytes(s string) (int64, error) {
	if s == "" {
		return -1, nil
	}

	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "-") {
		return -1, nil
	}

	// first we try just parsing it to handle numbers without units
	num, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		if num < 0 {
			return -1, nil
		}
		return num, nil
	}

	matches := bytesUnitSplitter.FindStringSubmatch(s)

	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid bytes specification %v: %w", s, errInvalidByteString)
	}

	num, err = strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, err
	}

	suffix := matches[2]
	suffixMap := map[string]int64{"K": 10, "KB": 10, "KIB": 10, "M": 20, "MB": 20, "MIB": 20, "G": 30, "GB": 30, "GIB": 30, "T": 40, "TB": 40, "TIB": 40}

	mult, ok := suffixMap[strings.ToUpper(suffix)]
	if !ok {
		return 0, fmt.Errorf("invalid bytes specification %v: %w", s, errInvalidByteString)
	}
	num *= 1 << mult

	return num, nil
}

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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
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

// XdgShareHome is where to store data like nsc stored
func XdgShareHome() (string, error) {
	parent := os.Getenv("XDG_DATA_HOME")
	if parent != "" {
		return parent, nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}

	if u.HomeDir == "" {
		return "", fmt.Errorf("cannot determine home directory")
	}

	return filepath.Join(u.HomeDir, ".local", "share"), nil
}

// ConfigDir is the directory holding configuration files
func ConfigDir() (string, error) {
	parent, err := ParentDir()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(parent, "nats", "cli")
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return "", err
	}

	return dir, nil
}

// ParentDir is the parent, controlled by XDG_CONFIG_HOME, for any configuration
func ParentDir() (string, error) {
	parent := os.Getenv("XDG_CONFIG_HOME")
	if parent != "" {
		return parent, nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}

	if u.HomeDir == "" {
		return "", fmt.Errorf("cannot determine home directory")
	}

	return filepath.Join(u.HomeDir, parent, ".config"), nil
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

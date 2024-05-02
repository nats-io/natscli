package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nkeys"
	terminal "golang.org/x/term"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
)

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

	fs := []reflect.StructField{}
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

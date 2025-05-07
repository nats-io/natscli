// Copyright 2023-2025 The NATS Authors
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

package plugins

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/choria-io/fisk"
	iu "github.com/nats-io/natscli/internal/util"
)

var validNames = regexp.MustCompile(`^[a-z]+$`)

type plugin struct {
	Cmd        string          `json:"cmd"`
	Definition json.RawMessage `json:"def"`
}

func AddToApp(app *fisk.Application) error {
	parent, err := pluginDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(parent)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		pb, err := os.ReadFile(filepath.Join(parent, entry.Name()))
		if err != nil {
			log.Printf("Could not read plugin %v: %v", entry.Name(), err)
			continue
		}

		var p plugin
		err = json.Unmarshal(pb, &p)
		if err != nil {
			log.Printf("Could not read plugin %v: %v", entry.Name(), err)
			continue
		}

		_, err = app.ExternalPluginCommand(p.Cmd, p.Definition, strings.TrimSuffix(entry.Name(), ".json"), "")
		if err != nil {
			log.Printf("Invalid plugin %v: %v", entry.Name(), err)
			continue
		}
	}

	return nil
}

func Register(name string, command string, force bool) error {
	if !validNames.MatchString(name) {
		return fmt.Errorf("plugins names must match ^[a-z]$")
	}

	cmd, err := filepath.Abs(command)
	if err != nil {
		return err
	}

	store, err := pluginDir()
	if err != nil {
		return err
	}

	pluginPath := filepath.Join(store, fmt.Sprintf("%s.json", name))

	if !force {
		exist, _ := fileAccessible(pluginPath)
		if exist {
			return fmt.Errorf("plugins %s already registered, use --force to update", name)
		}
	}

	intro := exec.Command(cmd, "--fisk-introspect")
	out, err := intro.CombinedOutput()
	if err != nil {
		return err
	}

	pj, err := json.Marshal(plugin{cmd, out})
	if err != nil {
		return err
	}

	err = os.WriteFile(pluginPath, pj, 0600)
	if err != nil {
		return err
	}

	return nil
}

func fileAccessible(f string) (bool, error) {
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

func pluginDir() (string, error) {
	parent, err := iu.XdgShareHome()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(parent, "nats", "cli", "plugins")
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return "", err
	}

	return dir, nil
}

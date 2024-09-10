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
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
)

type Config struct {
	SelectedOperator string `json:"select_operator"`
}

func LoadConfig() (*Config, error) {
	parent, err := ConfigDir()
	if err != nil {
		return nil, fmt.Errorf("could not determine configuration directory: %w", err)
	}
	cfile := filepath.Join(parent, "config.json")

	cfg := Config{}

	if !FileExists(cfile) {
		return &Config{}, nil
	}

	cj, err := os.ReadFile(cfile)
	if err != nil {
		return nil, fmt.Errorf("could not read configuration file: %w", err)
	}
	err = json.Unmarshal(cj, &cfg)
	if err != nil {
		return nil, fmt.Errorf("could not parse configuration file: %w", err)
	}

	return &cfg, nil
}

func SaveConfig(cfg *Config) error {
	parent, err := ConfigDir()
	if err != nil {
		return err
	}
	cfile := filepath.Join(parent, "config.json")

	j, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	err = os.WriteFile(cfile, j, 0600)
	if err != nil {
		return err
	}

	return nil
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

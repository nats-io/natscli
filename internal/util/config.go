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

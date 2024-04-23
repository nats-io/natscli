package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type config struct {
	SelectedOperator string `json:"select_operator"`
}

func loadConfig() (*config, error) {
	parent, err := configDir()
	if err != nil {
		return nil, fmt.Errorf("could not determine configuration directory: %w", err)
	}
	cfile := filepath.Join(parent, "config.json")

	cfg := config{}

	if !fileExists(cfile) {
		return &config{}, nil
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

func saveConfig(cfg *config) error {
	parent, err := configDir()
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

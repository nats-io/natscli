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

package scaffold

import (
	"archive/zip"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	au "github.com/nats-io/natscli/internal/auth"
	"gopkg.in/yaml.v3"

	"github.com/choria-io/scaffold"
	"github.com/choria-io/scaffold/forms"
	iu "github.com/nats-io/natscli/internal/util"
)

//go:embed all:store

var Store embed.FS

// Requires indicate what data the bundle requires to run successfully
type Requires struct {
	// Operator indicates before running the scaffold an operator should be selected
	Operator bool `json:"operator" yaml:"operator"`
}

type Bundle struct {
	Description  string   `json:"description" yaml:"description"`
	PreScaffold  string   `json:"pre_scaffold" yaml:"pre_scaffold"`
	PostScaffold string   `json:"post_scaffold" yaml:"post_scaffold"`
	Contact      string   `json:"contact" yaml:"contact"`
	Source       string   `json:"source" yaml:"source"`
	Version      string   `json:"version" yaml:"version"`
	Requires     Requires `json:"requires" yaml:"requires"`

	SourceDir string `json:"-" yaml:"-"`
}

// FromFile reads a bundle from a file
func FromFile(file string) (*Bundle, error) {
	td, err := unzip(file)
	if err != nil {
		return nil, err
	}

	return FromDir(td)
}

// FromUrl reads a bundle from a http(s) URL
func FromUrl(url *url.URL) (*Bundle, error) {
	if url.Scheme == "fs" {
		// fs requires unix paths even on windows
		return FromFs(Store, strings.ReplaceAll(filepath.Join("store", url.Path), `\`, `/`))
	}

	res, err := http.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	tf, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(tf, res.Body)
	if err != nil {
		tf.Close()
		return nil, err
	}

	tf.Close()

	return FromFile(tf.Name())
}

// FromFs reads a bundle from an fs directory, typically via embedding
func FromFs(f fs.FS, dir string) (*Bundle, error) {
	tf, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}

	err = os.CopyFS(tf, f)
	if err != nil {
		return nil, err
	}

	return FromDir(filepath.Join(tf, dir))
}

// FromDir reads the bundle from a directory
func FromDir(dir string) (*Bundle, error) {
	f, err := os.ReadFile(filepath.Join(dir, "bundle.yaml"))
	if err != nil {
		return nil, err
	}

	var b Bundle
	err = yaml.Unmarshal(f, &b)
	if err != nil {
		return nil, err
	}

	b.SourceDir = dir

	err = b.Validate()
	if err != nil {
		return nil, err
	}

	return &b, nil
}

// Run runs a bundle writing the result to dest
func (b *Bundle) Run(dest string, env map[string]any, debug bool) error {
	var err error

	if env == nil {
		env = map[string]any{}
	}

	if iu.FileExists(b.formPath()) {
		err = b.executeForm(env)
		if err != nil {
			return err
		}
	}

	if b.Requires.Operator {
		_, operator, err := au.SelectOperator("", true, true)
		if err != nil {
			return err
		}

		env["Requirements"] = map[string]any{
			"Operator": operator,
		}
	}

	fmt.Println()

	return b.scaffold(dest, env, debug)
}

func (b *Bundle) executeForm(env map[string]any) error {
	res, err := forms.ProcessFile(b.formPath(), env)
	if err != nil {
		return err
	}

	for k, v := range res {
		env[k] = v
	}

	return nil
}

func (b *Bundle) scaffold(dest string, env map[string]any, debug bool) error {
	cfg := scaffold.Config{
		SkipEmpty: true,
	}

	sb, err := os.ReadFile(b.scaffoldPath())
	if err != nil {
		return err
	}

	err = json.Unmarshal(sb, &cfg)
	if err != nil {
		return err
	}

	if len(cfg.Source) > 0 {
		return fmt.Errorf("scaffold supports only source directory configuration")
	}

	if cfg.SourceDirectory == "" {
		cfg.SourceDirectory = "scaffold"
	}

	cfg.SourceDirectory = filepath.Join(b.SourceDir, cfg.SourceDirectory)
	cfg.TargetDirectory = dest

	s, err := scaffold.New(cfg, templateFuncs())
	if err != nil {
		return err
	}

	s.Logger(newLogger(debug))

	if b.PreScaffold != "" {
		res, err := s.RenderString(b.PreScaffold, env)
		if err != nil {
			return err
		}
		fmt.Println()
		fmt.Println(res)
		fmt.Println()
	}

	err = s.Render(env)
	if err != nil {
		return err
	}

	if b.PostScaffold != "" {
		res, err := s.RenderString(b.PostScaffold, env)
		if err != nil {
			return err
		}
		fmt.Println()
		fmt.Println(res)
		fmt.Println()
	}

	return nil
}

func (b *Bundle) formPath() string {
	return filepath.Join(b.SourceDir, "form.yaml")
}

func (b *Bundle) scaffoldPath() string {
	return filepath.Join(b.SourceDir, "scaffold.json")
}

// Close removes the temporary files holding the bundle
func (b *Bundle) Close() error {
	if b.SourceDir == "" {
		return nil
	}

	return os.RemoveAll(b.SourceDir)
}

func (b *Bundle) Validate() error {
	if b.Description == "" {
		return fmt.Errorf("bundles require a description")
	}

	if !iu.FileExists(b.scaffoldPath()) {
		return fmt.Errorf("no scaffold found")
	}

	return nil
}

func unzipFile(f *zip.File, dest string) error {
	fname := filepath.Join(dest, f.Name)
	if f.FileInfo().IsDir() {
		return os.MkdirAll(fname, f.FileInfo().Mode())
	}

	w, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer w.Close()

	src, err := f.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	_, err = io.Copy(w, src)
	if err != nil {
		return err
	}

	return nil
}

func unzip(src string) (string, error) {
	td, err := os.MkdirTemp("", "")
	if err != nil {
		return "", err
	}

	zr, err := zip.OpenReader(src)
	if err != nil {
		os.RemoveAll(td)

		return "", err
	}
	defer zr.Close()

	for _, f := range zr.File {
		if !filepath.IsLocal(f.Name) || strings.Contains(f.Name, "\\") {
			os.RemoveAll(td)
			return "", fmt.Errorf("insecure paths found in archive")
		}
	}

	for _, f := range zr.File {
		err = unzipFile(f, td)
		if err != nil {
			os.RemoveAll(td)
			return "", err
		}
	}

	return td, nil
}

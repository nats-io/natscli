// Copyright 2020 The NATS Authors
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

// Package natscontext provides a way for sets of configuration options to be stored
// in named files and later retrieved either by name or if no name is supplied by access
// a chosen default context.
//
// Files are stored in ~/.config/nats or in the directory set by XDG_CONFIG_HOME environment
//
//   .config/nats
//   .config/nats/context
//   .config/nats/context/ngs.js.json
//   .config/nats/context/ngs.stats.json
//   .config/nats/context.txt
//
// Here the context.txt holds simply the string matching a context name like 'ngs.js'
package natscontext

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nats-io/nats.go"
)

type Option func(c *settings)

type settings struct {
	Description string `json:"description"`
	URL         string `json:"url"`
	User        string `json:"user"`
	Password    string `json:"password"`
	Creds       string `json:"creds"`
	NKey        string `json:"nkey"`
	Cert        string `json:"cert"`
	Key         string `json:"key"`
	CA          string `json:"ca"`
}

type Context struct {
	Name   string `json:"-"`
	config *settings
	path   string
}

// New loads a new configuration context. If name is empty the current active
// one will be loaded.  If load is false no loading of existing data is done
// this is mainly useful to create new empty contexts.
//
// When opts is supplied those settings will override what was loaded or supply
// values for an empty context
func New(name string, load bool, opts ...Option) (*Context, error) {
	c := &Context{
		Name:   name,
		config: &settings{URL: nats.DefaultURL},
	}

	if load {
		err := c.loadActiveContext()
		if err != nil {
			return nil, err
		}
	}

	// apply supplied overrides
	for _, opt := range opts {
		opt(c.config)
	}

	return c, nil
}

func parentDir() (string, error) {
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

// DeleteContext deletes a context with a given name, the active context
// can not be deleted
func DeleteContext(name string) error {
	if !validName(name) {
		return fmt.Errorf("invalid context name %q", name)
	}

	if SelectedContext() == name {
		return fmt.Errorf("cannot remove the current active context")
	}

	parent, err := parentDir()
	if err != nil {
		return err
	}

	cfile := filepath.Join(parent, "nats", "context", name+".json")
	_, err = os.Stat(cfile)
	if os.IsNotExist(err) {
		return nil
	}

	return os.Remove(cfile)
}

// IsKnown determines if a context is known
func IsKnown(name string) bool {
	if !validName(name) {
		return false
	}

	parent, err := parentDir()
	if err != nil {
		return false
	}

	return knownContext(parent, name)
}

// ContextPath is the path on disk to store the context
func ContextPath(name string) (string, error) {
	if !validName(name) {
		return "", fmt.Errorf("invalid context name %q", name)
	}

	parent, err := parentDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(ctxDir(parent), name+".json"), nil
}

// KnownContexts is a list of known context
func KnownContexts() []string {
	configs := []string{}

	parent, err := parentDir()
	if err != nil {
		return configs
	}

	files, err := ioutil.ReadDir(filepath.Join(parent, "nats", "context"))
	if err != nil {
		return configs
	}

	for _, f := range files {
		if f.IsDir() || f.Size() == 0 {
			continue
		}

		ext := filepath.Ext(f.Name())
		if ext != ".json" {
			continue
		}

		configs = append(configs, strings.TrimSuffix(f.Name(), ext))
	}

	sort.Strings(configs)

	return configs
}

// SelectedContext returns the name of the current selected context, empty when non is selected
func SelectedContext() string {
	parent, err := parentDir()
	if err != nil {
		return ""
	}

	currentFile := filepath.Join(parent, "nats", "context.txt")

	_, err = os.Stat(currentFile)
	if os.IsNotExist(err) {
		return ""
	}

	fc, err := ioutil.ReadFile(currentFile)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(fc))
}

func knownContext(parent string, name string) bool {
	if !validName(name) {
		return false
	}

	_, err := os.Stat(filepath.Join(ctxDir(parent), name+".json"))
	return !os.IsNotExist(err)
}

func (c *Context) loadActiveContext() error {
	parent, err := parentDir()
	if err != nil {
		return err
	}

	// none given, lets try to find it via the fs
	if c.Name == "" {
		c.Name = SelectedContext()
		if c.Name == "" {
			return nil
		}
	}

	if !validName(c.Name) {
		return fmt.Errorf("invalid context name %s", c.Name)
	}

	if !knownContext(parent, c.Name) {
		return fmt.Errorf("unknown context %q", c.Name)
	}

	c.path = filepath.Join(parent, "nats", "context", c.Name+".json")
	ctxContent, err := ioutil.ReadFile(c.path)
	if err != nil {
		return err
	}

	return json.Unmarshal(ctxContent, c.config)
}

func validName(name string) bool {
	return name != "" && !strings.Contains(name, "..") && !strings.Contains(name, string(os.PathSeparator))
}

func createTree(parent string) error {
	return os.MkdirAll(ctxDir(parent), 0700)
}

func ctxDir(parent string) string {
	return filepath.Join(parent, "nats", "context")
}

// SelectContext sets the given context to be the default, error if it does not exist
func SelectContext(name string) error {
	if !validName(name) {
		return fmt.Errorf("invalid context name %q", name)
	}

	parent, err := parentDir()
	if err != nil {
		return err
	}

	err = createTree(parent)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(parent, "nats", "context.txt"), []byte(name), 0600)
}

func (c *Context) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(c.config, "", "  ")
}

// Save saves the current context to name
func (c *Context) Save(name string) error {
	if name != "" {
		c.Name = name
	}

	if !validName(c.Name) {
		return fmt.Errorf("invalid context name %q", c.Name)
	}

	parent, err := parentDir()
	if err != nil {
		return err
	}

	ctxDir := filepath.Join(parent, "nats", "context")
	err = createTree(parent)
	if err != nil {
		return err
	}

	j, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	c.path = filepath.Join(ctxDir, c.Name+".json")
	return ioutil.WriteFile(c.path, j, 0600)
}

// WithServerURL supplies the url(s) to connect to nats with
func WithServerURL(url string) Option {
	return func(s *settings) {
		if url != "" {
			s.URL = url
		}
	}
}

// ServerURL is the configured server urls, 'nats://localhost:4222' if not set
func (c *Context) ServerURL() string {
	if c.config.URL == "" {
		return nats.DefaultURL
	}

	return c.config.URL
}

// WithUser sets the username
func WithUser(u string) Option {
	return func(s *settings) {
		if u != "" {
			s.User = u
		}
	}
}

// User is the configured username, empty if not set
func (c *Context) User() string { return c.config.User }

// WithPassword sets the password
func WithPassword(p string) Option {
	return func(s *settings) {
		if p != "" {
			s.Password = p
		}
	}
}

// Password retrieves the configured password, empty if not set
func (c *Context) Password() string { return c.config.Password }

// WithCreds sets the credentials file
func WithCreds(c string) Option {
	return func(s *settings) {
		if c != "" {
			s.Creds = c
		}
	}
}

// Creds retrieves the configured credentials file path, empty if not set
func (c *Context) Creds() string { return c.config.Creds }

// WithNKey sets the nkey path
func WithNKey(n string) Option {
	return func(s *settings) {
		if n != "" {
			s.NKey = n
		}
	}
}

// NKey retrieves the configured nkey path, empty if not set
func (c *Context) NKey() string { return c.config.NKey }

// WithCertificate sets the path to the public certificate
func WithCertificate(c string) Option {
	return func(s *settings) {
		if c != "" {
			s.Cert = c
		}
	}
}

// Certificate retrieves the path to the public certificate, empty if not set
func (c *Context) Certificate() string { return c.config.Cert }

// WithKey sets the private key path to use
func WithKey(k string) Option {
	return func(s *settings) {
		if k != "" {
			s.Key = k
		}
	}
}

// Key retrieves the private key path, empty if not set
func (c *Context) Key() string { return c.config.Key }

// WithCA sets the CA certificate path to use
func WithCA(ca string) Option {
	return func(s *settings) {
		if ca != "" {
			s.CA = ca
		}
	}
}

// CA retrieves the CA file path, empty if not set
func (c *Context) CA() string { return c.config.CA }

// WithDescription sets a freiendly description for this context
func WithDescription(d string) Option {
	return func(s *settings) {
		if d != "" {
			s.Description = d
		}
	}
}

// Description retrieves the description, empty if not set
func (c *Context) Description() string { return c.config.Description }

// Path returns the path on disk for a loaded context, empty when not saved or loaded
func (c *Context) Path() string { return c.path }

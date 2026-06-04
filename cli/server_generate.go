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

package cli

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	au "github.com/nats-io/natscli/internal/auth"
	"github.com/nats-io/natscli/internal/scaffold"
	iu "github.com/nats-io/natscli/internal/util"
)

type serverGenerateCmd struct {
	source string
	target string
}

func configureServerGenerateCommand(srv *fisk.CmdClause) {
	c := &serverGenerateCmd{}

	gen := srv.Command("generate", `Generate server configurations`).Alias("gen").Action(c.generateAction)
	gen.Arg("target", "Write the output to a specific location").Required().StringVar(&c.target)
	gen.Flag("source", "Fetch the configuration bundle from a file or URL").StringVar(&c.source)
}

func (c *serverGenerateCmd) generateAction(_ *fisk.ParseContext) error {
	var b *scaffold.Bundle
	var err error

	if iu.FileExists(c.target) {
		return fmt.Errorf("target directory %s already exist", c.target)
	}

	fmt.Println("This tool generates NATS Server configurations based on a question and answer")
	fmt.Println("form-based approach and then renders the result into a directory.")
	fmt.Println()
	fmt.Println("It supports rendering local bundles compiled into the 'nats' command but can also")
	fmt.Println("fetch and render remote ones using a URL.")
	fmt.Println()

	switch {
	case c.source == "":
		err = c.pickEmbedded()
		if err != nil {
			return err
		}

		fallthrough

	case strings.Contains(c.source, "://"):
		var uri *url.URL
		uri, err = url.Parse(c.source)
		if err != nil {
			return err
		}

		if uri.Scheme == "" {
			return fmt.Errorf("invalid URL %q", c.source)
		}

		b, err = scaffold.FromUrl(uri)

	case iu.IsDirectory(c.source):
		b, err = scaffold.FromDir(c.source)

	default:
		b, err = scaffold.FromFile(c.source)
	}
	if err != nil {
		return err
	}

	if b.Requires.Operator {
		auth, err := au.GetAuthBuilder()
		if err != nil {
			return err
		}
		if len(auth.Operators().List()) == 0 {
			return fmt.Errorf("no operator found")
		}
	}

	env := map[string]any{
		"_target": c.target,
		"_source": c.source,
	}

	err = b.Run(c.target, env, opts().Trace)
	if err != nil {
		return err
	}

	return b.Close()
}

func (c *serverGenerateCmd) pickEmbedded() error {
	list := map[string]string{
		"Development Super Cluster using Docker Compose": "fs:///natsbuilder",
		"'nats auth' managed NATS Server configuration":  "fs:///operator",
		"'nats auth' managed NATS Cluster in Kubernetes": "fs:///operatork8s",
		"Synadia Cloud Leafnode Configuration":           "fs:///ngsleafnodeconfig",
	}

	names := []string{}
	for k := range list {
		names = append(names, k)
	}
	sort.Strings(names)

	choice := ""
	err := iu.AskOne(&survey.Select{
		Message:  "Select a template",
		Options:  names,
		PageSize: iu.SelectPageSize(len(names)),
	}, &choice)
	if err != nil {
		return err
	}

	c.source = list[choice]

	return nil
}

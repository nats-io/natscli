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

package cli

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"
)

func constraintsForType(sch *jsonschema.Schema) []string {
	var constraints []string

	if len(sch.Types) == 1 {
		switch sch.Types[0] {
		case "integer":
			if sch.Minimum != nil {
				constraints = append(constraints, fmt.Sprintf("min: %s", sch.Minimum.FloatString(0)))
			}

			if sch.Maximum != nil {
				constraints = append(constraints, fmt.Sprintf("max: %s", sch.Maximum.FloatString(0)))
			}
		case "string":
			if sch.MinLength > -1 {
				constraints = append(constraints, fmt.Sprintf("min length: %d", sch.MinLength))
			}
			if sch.MaxLength > -1 {
				constraints = append(constraints, fmt.Sprintf("max length: %d", sch.MaxLength))
			}
			if sch.Pattern != nil {
				constraints = append(constraints, fmt.Sprintf("pattern: %s", sch.Pattern.String()))
			}
		case "array":
			if sch.MinItems > -1 {
				constraints = append(constraints, fmt.Sprintf("min items: %d", sch.MinItems))
			}

			if items, ok := sch.Items.(*jsonschema.Schema); ok {
				if len(items.Types) > 0 && items.Types[0] != "object" {
					constraints = append(constraints, fmt.Sprintf("items: %v", items.Types[0]))
				}
			}
		}
	}

	return constraints
}

func decorateSequenceNode(n *yaml.Node, sch *jsonschema.Schema) {
	if len(sch.Types) != 1 {
		n.HeadComment = sch.Description
		return
	}

	if len(sch.Types) == 1 {
		switch sch.Types[0] {
		case "array":
			decorateScalarNode(n, sch)

			if ssch, ok := sch.Items.(*jsonschema.Schema); ok {
				for _, c := range n.Content {
					if c.Kind == yaml.MappingNode {
						decorateMappingNode(c, ssch)
					}
				}
			}
		}
	}
}

func decorateMappingNode(node *yaml.Node, sch *jsonschema.Schema) {
	if len(sch.Types) != 1 {
		node.HeadComment = sch.Description
		return
	}

	parent := &jsonschema.Schema{}
	for _, n := range node.Content {
		decorateNode(n, sch, parent)
	}

}

func decorateScalarNode(n *yaml.Node, sch *jsonschema.Schema) {
	if len(sch.Types) != 1 {
		n.HeadComment = sch.Description
		return
	}

	if strings.Contains(sch.Comment, "nanoseconds depicting a duration") {
		n.HeadComment = fmt.Sprintf("\n%s\n#\n  Type: duration like 10s or 2h1m5s", sch.Description)
		return
	}

	constraints := constraintsForType(sch)
	if len(constraints) == 0 {
		n.HeadComment = fmt.Sprintf("\n%s\n#\n  Type: %v", sch.Description, sch.Types[0])
	} else {
		n.HeadComment = fmt.Sprintf("\n%s\n#\n  Type: %v (%s)", sch.Description, sch.Types[0], f(constraints))
	}

	if sch.Comment != "" {
		n.HeadComment = fmt.Sprintf("%s\n  Comment: %s", n.HeadComment, sch.Comment)
	}

	if len(sch.Enum) > 0 {
		var valid []string
		for _, v := range sch.Enum {
			valid = append(valid, fmt.Sprintf("%v", v))
		}
		n.HeadComment = fmt.Sprintf("%s\n  Valid Values: %s", n.HeadComment, f(valid))
	}
}

func decorateNode(n *yaml.Node, sch *jsonschema.Schema, parent *jsonschema.Schema) *jsonschema.Schema {
	switch n.Kind {
	case yaml.ScalarNode:
		schema := sch.Properties[n.Value]
		if schema == nil {
			return nil
		}

		decorateScalarNode(n, schema)
		return schema

	case yaml.MappingNode:
		if parent != nil {
			decorateMappingNode(n, parent)
		}

	case yaml.SequenceNode:
		if parent != nil {
			decorateSequenceNode(n, parent)
		}
	}

	return nil
}

type typedData interface {
	Schema() ([]byte, error)
	SchemaType() string
}

func decoratedYamlMarshal(v typedData) ([]byte, error) {
	var node yaml.Node

	sb, err := v.Schema()
	if err != nil {
		return nil, err
	}

	compiler := jsonschema.NewCompiler()
	compiler.ExtractAnnotations = true
	compiler.AddResource(v.SchemaType(), bytes.NewReader(sb))
	sch := compiler.MustCompile(v.SchemaType())

	node.Encode(v)

	node.HeadComment = fmt.Sprintf("Schema: %v", v.SchemaType())
	if len(sch.Required) > 0 {
		node.HeadComment = fmt.Sprintf("%s\n#\n  Required Items: %v", node.HeadComment, f(sch.Required))
	}

	var parent *jsonschema.Schema
	for _, n := range node.Content {
		parent = decorateNode(n, sch, parent)
	}

	return yaml.Marshal(node)
}

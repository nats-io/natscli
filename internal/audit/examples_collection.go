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

package audit

import (
	"fmt"
	"strings"
)

// ExamplesCollection stores examples of issues found by a check as it scans entities in an archive.
// A limit can be passed to avoid accumulating hundreds of example.
// After the limit is reached, further examples are just counted but not stored.
type ExamplesCollection struct {
	Examples []string `json:"examples,omitempty"`
	Limit    uint     `json:"-"`
}

// newExamplesCollection creates a new empty collection of examples.
// Use 0 as limit to store unlimited examples.
func newExamplesCollection(limit uint) *ExamplesCollection {
	return &ExamplesCollection{
		Limit:    limit,
		Examples: []string{},
	}
}

func (c *ExamplesCollection) add(format string, a ...any) {
	c.Examples = append(c.Examples, fmt.Sprintf(format, a...))
}

func (c *ExamplesCollection) clear() {
	c.Examples = []string{}
}

// Count the number of examples added to this collection (including the omitted ones)
func (c *ExamplesCollection) Count() int {
	if c == nil {
		return 0
	}

	return len(c.Examples)
}

// String produces a multi-line string with one example per line.
// If more examples were added than the limit, an extra line is printed with the number of omitted examples.
func (c *ExamplesCollection) String() string {
	//create a string builder and append each example as string

	examples := c.Examples[:]
	omitted := 0
	if len(c.Examples) > int(c.Limit) {
		examples = examples[:c.Limit]
		omitted = len(c.Examples) - len(examples)
	}

	b := &strings.Builder{}
	for _, example := range examples {
		b.WriteString(fmt.Sprintf(" - %s\n", example))
	}

	if omitted > 0 {
		b.WriteString(fmt.Sprintf(" - ... and %d more ...\n", omitted))
	}

	return b.String()
}

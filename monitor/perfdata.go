// Copyright 2023 The NATS Authors
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

package monitor

import (
	"fmt"
	"strings"
)

type PerfDataItem struct {
	Help  string  `json:"-"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Warn  float64 `json:"warning"`
	Crit  float64 `json:"critical"`
	Unit  string  `json:"unit,omitempty"`
}

type PerfData []*PerfDataItem

func (p PerfData) String() string {
	var res []string
	for _, i := range p {
		res = append(res, i.String())
	}

	return strings.TrimSpace(strings.Join(res, " "))
}

func (i *PerfDataItem) String() string {
	valueFmt := "%0.0f"
	if i.Unit == "s" {
		valueFmt = "%0.4f"
	}

	pd := fmt.Sprintf("%s="+valueFmt, i.Name, i.Value)
	if i.Unit != "" {
		pd = pd + i.Unit
	}

	if i.Warn > 0 || i.Crit > 0 {
		if i.Warn != 0 {
			pd = fmt.Sprintf("%s;"+valueFmt, pd, i.Warn)
		} else if i.Crit > 0 {
			pd = fmt.Sprintf("%s;", pd)
		}

		if i.Crit != 0 {
			pd = fmt.Sprintf("%s;"+valueFmt, pd, i.Crit)
		}
	}

	return pd
}

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

package cli

import (
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/natscli/columns"
)

func newColumns(heading string) *columns.Writer {
	w := columns.New(heading)
	w.SetColorScheme(opts().Config.ColorScheme())
	w.SetHeading(heading)

	return w
}
func newColumnsf(heading string, a ...any) *columns.Writer {
	return newColumns(fmt.Sprintf(heading, a...))
}

func fiBytes(v uint64) string {
	return humanize.IBytes(v)
}

func f(v any) string {
	return columns.F(v)
}

func fFloatFixedDecimal(v any) string {
	return humanize.FormatFloat("#,###.##", v.(float64))
}

func fFloat2Int(v any) string {
	return columns.F(uint64(v.(float64)))
}

func fiBytesFloat2Int(v any) string {
	return fiBytes(uint64(v.(float64)))
}

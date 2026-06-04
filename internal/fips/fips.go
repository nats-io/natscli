// Copyright 2026 The NATS Authors
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

// Package fips reports FIPS 140-3 mode and provides helpers to gate operations
// that rely on cryptography not approved by the Go FIPS 140-3 module.
package fips

import (
	"crypto/fips140"
	"fmt"
)

// Enabled reports whether the Go FIPS 140-3 module is active (GODEBUG=fips140=on
// or fips140=only, or built with //go:debug fips140=...).
func Enabled() bool {
	return fips140.Enabled()
}

// DisabledError returns an error indicating an operation is unavailable in the
// FIPS build. op is the user-facing operation name and algo the algorithm or
// primitive that is not part of the FIPS 140-3 module.
func DisabledError(op, algo string) error {
	return fmt.Errorf("%s is disabled in FIPS mode: %s is not part of the FIPS 140-3 approved algorithm set; use a non-FIPS build of nats to perform this operation", op, algo)
}

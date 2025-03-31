// Copyright 2025 The NATS Authors
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

import "math/rand/v2"

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var passwordRunes = append(letterRunes, []rune("@#_-%^&()")...)

// RandomPassword generates a random string like RandomString() but includes some special characters
func RandomPassword(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = passwordRunes[rand.IntN(len(passwordRunes))]
	}

	return string(b)
}

// RandomString generates a random string that includes only a-zA-Z0-9
func RandomString(shortest uint, longest uint) string {
	if shortest > longest {
		shortest, longest = longest, shortest
	}

	var desired int

	switch {
	case int(longest)-int(shortest) < 0:
		desired = int(shortest) + rand.IntN(int(longest))
	case longest == shortest:
		desired = int(shortest)
	default:
		desired = int(shortest) + rand.IntN(int(longest-shortest))
	}

	b := make([]rune, desired)
	for i := range b {
		b[i] = letterRunes[rand.IntN(len(letterRunes))]
	}

	return string(b)
}

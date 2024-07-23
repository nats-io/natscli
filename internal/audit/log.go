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

import "fmt"

// logLevels declares log levels and sets default values
var logLevels = struct {
	critical bool
	warning  bool
	info     bool
	debug    bool
}{
	true,
	true,
	true,
	false,
}

// LogQuiet turns off all log level, including Critical
func LogQuiet() {
	logLevels.critical = false
	logLevels.warning = false
	logLevels.info = false
	logLevels.debug = false
}

// LogVerbose turns on all log level, including Debug
func LogVerbose() {
	logLevels.critical = true
	logLevels.warning = true
	logLevels.info = true
	logLevels.debug = true
}

// logCritical for serious issues that need attention
func logCritical(format string, a ...any) {
	if logLevels.critical {
		fmt.Printf("(!) "+format+"\n", a...)
	}
}

// logInfo for neutral and positive messages
func logInfo(format string, a ...any) {
	if logLevels.info {
		fmt.Printf(format+"\n", a...)
	}
}

// logWarning for issues running the check itself, but not serious enough to terminate with an error
func logWarning(format string, a ...any) {
	if logLevels.warning {
		fmt.Printf("Warning: "+format+"\n", a...)
	}
}

// logDebug for very fine grained progress, disabled by default
func logDebug(format string, a ...any) {
	if logLevels.debug {
		fmt.Printf("(DEBUG) "+format+"\n", a...)
	}
}

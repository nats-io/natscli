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

package cli

func configureServerCommand(app commandHost) {
	srv := app.Command("server", "Server information").Alias("srv").Alias("sys").Alias("system")
	addCheat("server", srv)

	configureServerAccountCommand(srv)
	configureServerCheckCommand(srv)
	configureServerClusterCommand(srv)
	configureServerInfoCommand(srv)
	configureServerListCommand(srv)
	configureServerMappingCommand(srv)
	configureServerPasswdCommand(srv)
	configureServerPingCommand(srv)
	configureServerReportCommand(srv)
	configureServerRequestCommand(srv)
	configureServerRunCommand(srv)
}

func init() {
	registerCommand("server", 15, configureServerCommand)
}

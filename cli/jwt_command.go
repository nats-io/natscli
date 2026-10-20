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

package cli

func configureJwtCommand(app commandHost) {
	const jwtHelpLong = `The jwt command inspects NATS JWTs without needing a server.

Decode shows a readable summary of the claims based on the JWT type:
operator, account, user, activation, authorization request and
authorization response.

The input can be a raw JWT string, a .jwt file or a .creds file. For
.creds files only the JWT block is read, the NKey seed is never parsed
or printed.

The signature is only checked for internal consistency, the issuer is
not checked against a trusted operator or account, so a successful
decode is not proof that a JWT is valid or trusted. Problems found in
the claims, like an expired JWT, are listed under Validation Issues.

Use --json to get the complete claims for tools like jq.`

	jwtCmd := app.Command("jwt", "Inspect NATS JWTs")
	jwtCmd.HelpLong(jwtHelpLong)
	addCheat("jwt", jwtCmd)

	configureJwtDecodeCommand(jwtCmd)
}

func init() {
	registerCommand("jwt", 20, configureJwtCommand)
}

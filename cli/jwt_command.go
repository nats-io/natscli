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
decode is not proof that a JWT is valid or trusted.

Use --json to get the complete claims for tools like jq.`

	jwtCmd := app.Command("jwt", "Decode JWT from path or raw jwt")
	jwtCmd.HelpLong(jwtHelpLong)
	addCheat("jwt", jwtCmd)

	configureJwtDecodeCommand(jwtCmd)
}

func init() {
	registerCommand("jwt", 20, configureJwtCommand)
}

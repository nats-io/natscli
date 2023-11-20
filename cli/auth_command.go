package cli

func configureAuthCommand(app commandHost) {
	auth := app.Command("auth", "NATS Decentralized Authentication")

	auth.HelpLong("WARNING: This is experimental and subject to massive change, do not use yet")

	configureAuthOperatorCommand(auth)
	configureAuthNkeyCommand(auth)
}

func init() {
	registerCommand("auth", 0, configureAuthCommand)
}

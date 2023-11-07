package cli

func configureAuthCommand(app commandHost) {
	auth := app.Command("auth", "Administers Decentralized Authentication")

	configureAuthNkeyCommand(auth)
}

func init() {
	registerCommand("auth", 0, configureAuthCommand)
}

package cli

func configurePaCommand(app commandHost) {
	srv := app.Command("pa", "NATS PA commands")

	configurePaGatherCommand(srv)
	configurePaAnalyzeCommand(srv)
}

func init() {
	registerCommand("pa", 21, configurePaCommand)
}


package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/alecthomas/kingpin.v2"
)

type devCmd struct {
	proxyPort int
}

func configureDevCommand(app *kingpin.Application) {
	c := devCmd{}
	dev := app.Command("dev", "Developer support commands").Hidden()

	proxy := dev.Command("validating_proxy", "Validating proxy for JetStream client development").Hidden().Action(c.validatingProxy)
	proxy.Arg("port", "Port to listen on").IntVar(&c.proxyPort)
}

func (c *devCmd) validatingProxy(_ *kingpin.ParseContext) error {
	proxy := validatingProxy{
		Port:     c.proxyPort,
		User:     username,
		Password: password,
		Trace:    trace,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.interruptWatcher(ctx, cancel)

	err := proxy.Start(ctx, cancel)
	if err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func (c *devCmd) interruptWatcher(ctx context.Context, cancel func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case sig := <-sigs:
			log.Printf("Shutting down on %s", sig)
			cancel()

		case <-ctx.Done():
			return
		}
	}
}

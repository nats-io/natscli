package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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
	if c.proxyPort <= 0 {
		return fmt.Errorf("proxy port is required")
	}

	if username == "" {
		return fmt.Errorf("username is required")
	}

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	sopts, tf, err := c.serverOpts(dir, username, password)
	if err != nil {
		return err
	}
	defer os.Remove(tf)

	if trace {
		sopts.Trace = true
		sopts.TraceVerbose = true
	}

	srv, err := server.NewServer(sopts)
	if err != nil {
		return err
	}

	srv.ConfigureLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.interruptWatcher(ctx, cancel)
	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		return fmt.Errorf("server did not start")
	}

	return c.startProxy(ctx, srv.ClientURL(), srv, cancel)
}

func (c *devCmd) schemasForSubject(subj string) (string, string) {
	schemaNames := map[string]string{
		"$JS.API.STREAM.CREATE":           "io.nats.jetstream.api.v1.stream_create_request|io.nats.jetstream.api.v1.stream_create_response",
		"$JS.API.STREAM.UPDATE":           "io.nats.jetstream.api.v1.stream_create_request|io.nats.jetstream.api.v1.stream_create_response",
		"$JS.API.STREAM.LIST":             "io.nats.jetstream.api.v1.stream_list_request|io.nats.jetstream.api.v1.stream_list_response",
		"$JS.API.STREAM.NAMES":            "io.nats.jetstream.api.v1.stream_names_request|io.nats.jetstream.api.v1.stream_names_response",
		"$JS.API.STREAM.MSG.GET":          "io.nats.jetstream.api.v1.stream_msg_get_request|io.nats.jetstream.api.v1.stream_msg_get_response",
		"$JS.API.STREAM.SNAPSHOT":         "io.nats.jetstream.api.v1.stream_snapshot_request|io.nats.jetstream.api.v1.stream_snapshot_response",
		"$JS.API.CONSUMER.CREATE":         "io.nats.jetstream.api.v1.consumer_create_request|io.nats.jetstream.api.v1.consumer_create_response",
		"$JS.API.CONSUMER.DURABLE.CREATE": "io.nats.jetstream.api.v1.consumer_create_request|io.nats.jetstream.api.v1.consumer_create_response",
		"$JS.API.CONSUMER.LIST":           "io.nats.jetstream.api.v1.consumer_list_request|io.nats.jetstream.api.v1.consumer_list_response",
		"$JS.API.CONSUMER.MSG.NEXT":       "io.nats.jetstream.api.v1.consumer_getnext_request|io.nats.jetstream.api.v1.consumer_getnext_response",
		"$JS.API.CONSUMER.NAMES":          "io.nats.jetstream.api.v1.consumer_names_request|io.nats.jetstream.api.v1.consumer_names_response",
		"$JS.API.STREAM.TEMPLATE.CREATE":  "io.nats.jetstream.api.v1.stream_template_create_request|io.nats.jetstream.api.v1.stream_template_create_response",
		"$JS.API.STREAM.TEMPLATE.NAMES":   "io.nats.jetstream.api.v1.stream_template_names_request|io.nats.jetstream.api.v1.stream_template_names_response",
	}

	st := ""
	for k, v := range schemaNames {
		if strings.HasPrefix(subj, k) {
			st = v
			break
		}
	}

	if st == "" {
		return "", ""
	}

	parts := strings.Split(st, "|")
	return parts[0], parts[1]
}

func (c *devCmd) validateRequest(subj string, request []byte) (bool, []string, error) {
	st, _ := c.schemasForSubject(subj)
	if st == "" {
		return true, nil, nil
	}

	var data interface{}
	err := json.Unmarshal(request, &data)
	if err != nil {
		return false, nil, err
	}

	ok, errs := new(SchemaValidator).ValidateStruct(data, st)
	return ok, errs, nil
}

func (c *devCmd) errorResponse(rt string, msg string) []byte {
	resp := map[string]interface{}{
		"type": rt,
		"error": map[string]interface{}{
			"code":        400,
			"description": msg,
		},
	}

	rj, _ := json.Marshal(resp)
	return rj
}

func (c *devCmd) startProxy(ctx context.Context, url string, srv *server.Server, cancel func()) error {
	nc, err := nats.Connect(url, nats.UserInfo(username, password), nats.UseOldRequestStyle(), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}

	nch, err := nats.Connect(url, nats.UserInfo("hidden", "secret"), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}

	apiSub, err := nc.Subscribe("$JS.>", func(m *nats.Msg) {
		if strings.HasPrefix(m.Subject, "$JS.EVENT") {
			return
		}

		srv.Noticef("msg: %s: %s", m.Subject, string(m.Data))

		ok, errs, err := c.validateRequest(m.Subject, m.Data)
		if err != nil {
			srv.Noticef("Validation failed: %s", err)
			_, sr := c.schemasForSubject(m.Subject)
			m.Respond(c.errorResponse(sr, fmt.Sprintf("Validation could not be completed: %s", err)))
			return
		}

		if !ok {
			srv.Noticef("Validation failed: %s", strings.Join(errs, "\n"))
			_, sr := c.schemasForSubject(m.Subject)
			m.Respond(c.errorResponse(sr, fmt.Sprintf("Validation failed: %s", strings.Join(errs, ", "))))

			return
		}

		rmsg, err := nch.RequestMsg(m, 5*time.Second)
		if err != nil {
			srv.Noticef("Proxy request to %s failed: %s", m.Subject, err)
			return
		}

		m.RespondMsg(rmsg)
	})
	if err != nil {
		return err
	}
	srv.Noticef("Proxy listening on $JS.>")

	inSub, err := nc.Subscribe("js.in.>", func(m *nats.Msg) {
		srv.Noticef("msg: %s: %s", m.Subject, string(m.Data))

		rmsg, err := nch.RequestMsg(m, 5*time.Second)
		if err != nil {
			srv.Noticef("Proxy request to %s failed: %s", m.Subject, err)
			return
		}

		m.RespondMsg(rmsg)
	})
	if err != nil {
		return err
	}
	srv.Noticef("Proxy listening on js.in.>")

	adminSub, err := nc.Subscribe("ci.admin.>", func(m *nats.Msg) {
		switch m.Subject {
		case "ci.admin.shutdown":
			srv.Noticef("Shutting down on admin request")
			m.Respond([]byte("+OK"))
			nc.Flush()
			cancel()
		case "ci.admin.ready":
			m.Respond([]byte("+OK"))
		case "ci.admin.refresh":
			srv.Noticef("Received CI refresh request, cleaning streams")
			js, err := jsm.New(nch)
			if err != nil {
				srv.Noticef("Refresh failed: %s", err)
			}

			cnt := 0
			err = js.EachStream(func(s *jsm.Stream) {
				err = s.Delete()
				if err != nil {
					srv.Noticef("Failed to delete stream %s: %s", s.Name(), err)
					return
				}
				cnt++
				srv.Noticef("Removed stream %s", s.Name())
			})
			if err != nil {
				srv.Noticef("Could not enumerate streams: %s", err)
			}
			srv.Noticef("CI refresh completed")

			m.Respond([]byte(fmt.Sprintf("+OK removed %d streams", cnt)))
		default:
			m.Respond([]byte(fmt.Sprintf("-ERR unknown command %s", m.Subject)))
		}
	})
	if err != nil {
		return err
	}
	srv.Noticef("Proxy listening on ci.admin.>")
	nc.Publish("ci.ready", []byte("+OK"))
	srv.Noticef("Proxy ready for requests, notified on ci.ready")

	<-ctx.Done()

	srv.Noticef("Shutting down proxy after context interrupt")

	apiSub.Unsubscribe()
	inSub.Unsubscribe()
	adminSub.Unsubscribe()

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

func (c *devCmd) serverOpts(sd string, user string, pass string) (*server.Options, string, error) {
	var cfg = `
server_name: JetStreamCI

jetstream: {
	max_mem: 1G
	max_file: 10G
	store_dir: %s
}

accounts {
	hidden_js: {
		jetstream: enabled

		users = [
			{user: "hidden", password: "secret"}
		]

		exports = [
			{ stream: "js.out.>", accounts: [proxy] },
		]
	}

	proxy {	
		jetstream: false

		users = [
			{user: "%s", password: "%s"}
		]

		imports = [
			{stream: {account: hidden_js, subject: "js.out.>"}, to: "js.out.>"}
		]
	}
}
`

	tf, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, "", err
	}

	tf.WriteString(fmt.Sprintf(cfg, sd, user, pass))
	tf.Close()

	opts, err := server.ProcessConfigFile(tf.Name())
	if err != nil {
		return nil, "", err
	}

	return opts, tf.Name(), nil
}

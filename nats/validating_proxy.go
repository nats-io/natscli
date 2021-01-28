package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type validatingProxy struct {
	Port     int
	User     string
	Password string
	Trace    bool

	cfile  string
	store  string
	ctx    context.Context
	cancel func()
	srv    *server.Server

	sync.Mutex
}

func (p *validatingProxy) ClientURL() string {
	p.Lock()
	defer p.Unlock()

	if p.srv == nil {
		return ""
	}

	return p.srv.ClientURL()
}

func (p *validatingProxy) Start(ctx context.Context, cancel func()) error {
	var err error

	if p.Port <= 0 {
		return fmt.Errorf("proxy port is required")
	}

	if p.User == "" {
		return fmt.Errorf("username is required")
	}

	p.Lock()
	p.ctx = ctx
	p.cancel = cancel

	p.store, err = ioutil.TempDir("", "")
	if err != nil {
		p.Unlock()
		return err
	}

	sopts, err := p.serverOpts()
	if err != nil {
		p.Unlock()
		return err
	}

	if p.Trace {
		sopts.Trace = true
		sopts.TraceVerbose = true
	}

	p.srv, err = server.NewServer(sopts)
	if err != nil {
		p.Unlock()
		return err
	}

	p.srv.ConfigureLogger()

	p.Unlock()

	go p.srv.Start()

	if !p.srv.ReadyForConnections(10 * time.Second) {
		return fmt.Errorf("server did not start")
	}

	return p.startProxy()
}

func (p *validatingProxy) connCloser(nc *nats.Conn) {
	<-p.ctx.Done()
	nc.Close()
}

func (p *validatingProxy) startProxy() error {
	p.Lock()
	url := p.srv.ClientURL()
	p.Unlock()

	if p.srv == nil {
		return fmt.Errorf("server not initiated")
	}

	nc, err := nats.Connect(url, nats.UserInfo(p.User, p.Password), nats.UseOldRequestStyle(), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	go p.connCloser(nc)

	nch, err := nats.Connect(url, nats.UserInfo("hidden", "secret"), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	go p.connCloser(nch)

	_, err = nc.Subscribe("$JS.>", func(m *nats.Msg) {
		if strings.HasPrefix(m.Subject, "$JS.EVENT") {
			return
		}

		p.srv.Noticef("msg: %s: %s", m.Subject, string(m.Data))

		ok, errs, err := p.validateRequest(m.Subject, m.Data)
		if err != nil {
			p.srv.Noticef("Validation failed: %s", err)
			_, sr := p.schemasForSubject(m.Subject)
			m.Respond(p.errorResponse(sr, fmt.Sprintf("Validation could not be completed: %s", err)))
			return
		}

		if !ok {
			p.srv.Noticef("Validation failed: %s", strings.Join(errs, "\n"))
			_, sr := p.schemasForSubject(m.Subject)
			m.Respond(p.errorResponse(sr, fmt.Sprintf("Validation failed: %s", strings.Join(errs, ", "))))

			return
		}

		rmsg, err := nch.RequestMsg(m, 5*time.Second)
		if err != nil {
			p.srv.Noticef("Proxy request to %s failed: %s", m.Subject, err)
			return
		}

		m.RespondMsg(rmsg)
	})
	if err != nil {
		return err
	}
	p.srv.Noticef("Proxy listening on $JS.>")

	_, err = nc.Subscribe("js.in.>", func(m *nats.Msg) {
		p.srv.Noticef("msg: %s: %s", m.Subject, string(m.Data))

		rmsg, err := nch.RequestMsg(m, 5*time.Second)
		if err != nil {
			p.srv.Noticef("Proxy request to %s failed: %s", m.Subject, err)
			return
		}

		m.RespondMsg(rmsg)
	})
	if err != nil {
		return err
	}
	p.srv.Noticef("Proxy listening on js.in.>")

	_, err = nc.Subscribe("ci.admin.>", func(m *nats.Msg) {
		switch m.Subject {
		case "ci.admin.shutdown":
			p.srv.Noticef("Shutting down on admin request")
			m.Respond([]byte("+OK"))
			nc.Flush()
			p.cancel()
		case "ci.admin.ready":
			m.Respond([]byte("+OK"))
		case "ci.admin.refresh":
			p.srv.Noticef("Received CI refresh request, cleaning streams")
			js, err := jsm.New(nch)
			if err != nil {
				p.srv.Noticef("Refresh failed: %s", err)
			}

			cnt := 0
			err = js.EachStream(func(s *jsm.Stream) {
				err = s.Delete()
				if err != nil {
					p.srv.Noticef("Failed to delete stream %s: %s", s.Name(), err)
					return
				}
				cnt++
				p.srv.Noticef("Removed stream %s", s.Name())
			})
			if err != nil {
				p.srv.Noticef("Could not enumerate streams: %s", err)
			}
			p.srv.Noticef("CI refresh completed")

			m.Respond([]byte(fmt.Sprintf("+OK removed %d streams", cnt)))
		default:
			m.Respond([]byte(fmt.Sprintf("-ERR unknown command %s", m.Subject)))
		}
	})
	if err != nil {
		return err
	}
	p.srv.Noticef("Proxy listening on ci.admin.>")
	nc.Publish("ci.ready", []byte("+OK"))
	p.srv.Noticef("Proxy ready for requests, notified on ci.ready")

	<-p.ctx.Done()

	return nil
}

func (p *validatingProxy) errorResponse(rt string, msg string) []byte {
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

func (p *validatingProxy) validateRequest(subj string, request []byte) (bool, []string, error) {
	st, _ := p.schemasForSubject(subj)
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

func (p *validatingProxy) schemasForSubject(subj string) (string, string) {
	// TODO: jsm should be able to do this
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

func (p *validatingProxy) Stop() {
	p.Lock()
	defer p.Unlock()

	if p.cancel != nil {
		p.cancel()
	}

	if p.srv != nil {
		p.srv.Shutdown()
	}

	if p.store != "" {
		os.RemoveAll(p.store)
	}

	if p.cfile != "" {
		os.Remove(p.cfile)
	}
}

func (p *validatingProxy) serverOpts() (*server.Options, error) {
	if p.User == "" || p.Password == "" {
		return nil, fmt.Errorf("username and password is required")
	}

	if p.store == "" {
		return nil, fmt.Errorf("store directory is required")
	}

	var err error
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

	cfile, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	p.cfile = cfile.Name()

	_, err = cfile.WriteString(fmt.Sprintf(cfg, p.store, p.User, p.Password))
	if err != nil {
		cfile.Close()
		return nil, err
	}
	cfile.Close()

	opts, err := server.ProcessConfigFile(cfile.Name())
	if err != nil {
		return nil, err
	}

	return opts, nil
}

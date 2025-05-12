package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/micro"
)

func TestServiceInfo(t *testing.T) {
	srv, _, _ := setupJStreamTest(t)
	defer srv.Shutdown()
	nc, _, _ := prepareHelper(srv.ClientURL())

	svc, err := micro.AddService(nc, micro.Config{
		Name:    "svc",
		Version: "1.0.0",
	})
	if err != nil {
		t.Errorf("failed to add service: %v", err)
	}
	defer svc.Stop()

	err = svc.AddEndpoint("ep1", micro.HandlerFunc(func(req micro.Request) {
		req.Respond(nil)
	}))
	if err != nil {
		t.Errorf("failed to add service endpoint: %v", err)
	}

	err = svc.AddEndpoint("ep2", micro.HandlerFunc(func(req micro.Request) {
		req.Respond(nil)
	}))
	if err != nil {
		t.Errorf("failed to add service endpoint: %v", err)
	}

	t.Run("Info", func(t *testing.T) {
		output := runNatsCli(t, fmt.Sprintf("--server='%s' service info svc --json", srv.ClientURL()))

		var resp struct {
			Info  *micro.Info  `json:"info"`
			Stats *micro.Stats `json:"stats"`
		}
		err = json.Unmarshal(output, &resp)
		if err != nil {
			t.Errorf("failed to unmarshal response: %v", err)
		}

		if resp.Info.Name != "svc" {
			t.Errorf("expected service name to be svc, got %s", resp.Info.Name)
		}

		if resp.Info.Endpoints[0].Name != "ep1" {
			t.Errorf("expected endpoint name to be ep1, got %s", resp.Info.Endpoints[0].Name)
		}

		if resp.Info.Endpoints[1].Name != "ep2" {
			t.Errorf("expected endpoint name to be ep2, got %s", resp.Info.Endpoints[1].Name)
		}
	})

	t.Run("Info with endpoint filter", func(t *testing.T) {
		output := runNatsCli(t, fmt.Sprintf("--server='%s' service info svc --json --endpoint='.*2'", srv.ClientURL()))

		var resp struct {
			Info  *micro.Info  `json:"info"`
			Stats *micro.Stats `json:"stats"`
		}
		err = json.Unmarshal(output, &resp)
		if err != nil {
			t.Errorf("failed to unmarshal response: %v", err)
		}

		if resp.Info.Name != "svc" {
			t.Errorf("expected service name to be svc, got %s", resp.Info.Name)
		}

		if len(resp.Info.Endpoints) != 1 {
			t.Errorf("expected 1 endpoint, got %d", len(resp.Info.Endpoints))
		}

		if resp.Info.Endpoints[0].Name != "ep2" {
			t.Errorf("expected endpoint name to be ep2, got %s", resp.Info.Endpoints[0].Name)
		}
	})

}

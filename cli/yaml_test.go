package cli

import (
	"fmt"
	"testing"

	"github.com/nats-io/jsm.go/api"
)

func TestYaml(t *testing.T) {
	cfg := api.StreamConfig{
		Mirror: &api.StreamSource{
			Name: "x",
		},
	}

	v, err := decoratedYamlMarshal(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	fmt.Println(string(v))
}

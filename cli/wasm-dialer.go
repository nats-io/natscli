package cli

import (
	"context"
	"fmt"
	"net"

	"nhooyr.io/websocket"
)

type wasmDialer struct {
}

func (d *wasmDialer) SkipTLSHandshake() bool {
	return true
}

func (d *wasmDialer) Dial(network, address string) (net.Conn, error) {
	var protocol string
	if network == "tcp" {
		protocol = "ws"
	} else if network == "tls" {
		protocol = "wss"
	} else {
		return nil, fmt.Errorf("unsupported network %s", network)
	}

	conn, _, err := websocket.Dial(context.Background(), protocol+"://"+address, &websocket.DialOptions{})
	if err != nil {
		return nil, fmt.Errorf("websocket.Dial: %w", err)
	}

	return websocket.NetConn(context.Background(), conn, websocket.MessageBinary), nil
}

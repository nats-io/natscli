package serverdata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// DoReqAsync serializes and sends a request to the given subject and handles multiple responses.
// This function uses the value from the timeout parameter as upper limit for responses gathering.
// The value of the `waitFor` may shorten the interval during which responses are gathered:
//
//	waitFor < 0  : listen for responses for the full timeout interval
//	waitFor == 0 : (adaptive timeout), after each response, wait a short amount of time for more, then stop
//	waitFor > 0  : stops listening before the timeout if the given number of responses are received
func DoReqAsync(ctx context.Context, req any, subj string, waitFor int, nc *nats.Conn, timeout time.Duration, trace bool, cb func([]byte)) error {
	jreq := []byte("{}")
	var err error

	if req != nil {
		switch val := req.(type) {
		case string:
			jreq = []byte(val)
		default:
			jreq, err = json.Marshal(req)
			if err != nil {
				return err
			}
		}
	}

	if trace {
		log.Printf(">>> %s: %s\n", subj, string(jreq))
	}

	var (
		mu       sync.Mutex
		ctr      = 0
		finisher *time.Timer
	)

	// Set deadline, max amount of time this function waits for responses
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Activate "adaptive timeout". Finisher may trigger early termination
	if waitFor == 0 {
		// First response can take up to timeout to arrive
		finisher = time.NewTimer(timeout)
		go func() {
			select {
			case <-finisher.C:
				cancel()
			case <-ctx.Done():
				return
			}
		}()
	}

	errs := make(chan error)
	sub, err := nc.Subscribe(nc.NewRespInbox(), func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		data := m.Data
		compressed := false
		if m.Header.Get("Content-Encoding") == "snappy" {
			compressed = true
			ud, err := io.ReadAll(s2.NewReader(bytes.NewBuffer(data)))
			if err != nil {
				errs <- err
				return
			}
			data = ud
		}

		if trace {
			if compressed {
				log.Printf("<<< (%dB -> %dB) %s", len(m.Data), len(data), string(data))
			} else {
				log.Printf("<<< (%dB) %s", len(data), string(data))
			}

			if m.Header != nil {
				log.Printf("<<< Header: %+v", m.Header)
			}
		}

		// If adaptive timeout is active, set deadline for next response
		if finisher != nil {
			// Stop listening and return if no further responses arrive within this interval
			finisher.Reset(300 * time.Millisecond)
		}

		if m.Header.Get("Status") == "503" {
			errs <- nats.ErrNoResponders
			return
		}

		cb(data)
		ctr++

		// Stop listening if the requested number of responses have been received
		if waitFor > 0 && ctr == waitFor {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	if waitFor > 0 {
		sub.AutoUnsubscribe(waitFor)
	}

	msg := nats.NewMsg(subj)
	msg.Data = jreq
	if subj != "$SYS.REQ.SERVER.PING" && !strings.HasPrefix(subj, "$SYS.REQ.ACCOUNT") {
		msg.Header.Set("Accept-Encoding", "snappy")
	}
	msg.Reply = sub.Subject

	err = nc.PublishMsg(msg)
	if err != nil {
		return err
	}

	select {
	case err = <-errs:
		if err == nats.ErrNoResponders && strings.HasPrefix(subj, "$SYS") {
			return fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
		}

		return err
	case <-ctx.Done():
	}

	if trace {
		log.Printf("=== Received %d responses", ctr)
	}

	return nil
}

// DoReq wraps DoReqAsync, collecting all responses into a byte slice array
func DoReq(ctx context.Context, req any, subj string, waitFor int, nc *nats.Conn, timeout time.Duration, trace bool) ([][]byte, error) {
	res := [][]byte{}
	mu := sync.Mutex{}

	err := DoReqAsync(ctx, req, subj, waitFor, nc, timeout, trace, func(r []byte) {
		mu.Lock()
		res = append(res, r)
		mu.Unlock()
	})

	return res, err
}

// CurrentActiveServers determines how many servers the connected server knows about
func CurrentActiveServers(ctx context.Context, nc *nats.Conn, timeout time.Duration, trace bool) (int, error) {
	var expect int

	err := DoReqAsync(ctx, nil, "$SYS.REQ.SERVER.PING", 1, nc, timeout, trace, func(msg []byte) {
		var res server.ServerStatsMsg

		err := json.Unmarshal(msg, &res)
		if err != nil {
			return
		}

		expect = res.Stats.ActiveServers
	})

	return expect, err
}
